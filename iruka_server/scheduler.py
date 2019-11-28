import logging
import queue
import random
import threading

from iruka.protos import iruka_admin_pb2
from iruka.protos import common_pb2


logger = logging.getLogger(__name__)


class Scheduler(object):
    def __init__(self):
        self._job_queue = queue.Queue()
        self._retry_queue = queue.Queue()
        # str(peer) -> ClientChannel
        self._clients = {}

    def client_online(self, client):
        # TODO: peer duplicate?
        self._clients[client.peer] = client
        # retry previous jobs that fails
        self.batch_retry()
        return True

    def client_offline(self, client):
        client.close()
        del self._clients[client.peer]

    def batch_retry(self):
        logger.debug('Transfering all jobs from retry queue to job queue...')
        count = 0
        try:
            while True:
                job = self._retry_queue.get_nowait()
                self._job_queue.put(job)
                count += 1
        except queue.Empty:
            pass
        if count > 0:
            logger.debug('Transferred %d jobs', count)

    def schedule(self):
        job = self._job_queue.get()

        if job is None:
            self.running = False
            return

        logger.info('Trying to dispatch job %s', job)

        clients_cand = list(self._clients.values())
        random.shuffle(clients_cand)

        for client in clients_cand:
            if (client.name not in job.blocked_names and
                not client.current_job):
                logger.info('... to client %r', client)

                # wait for client's ack
                if client.dispatch(job):
                    logger.info('Job %r accepted by %s', job, job.assigned_to)
                    break
        else:
            # FIXME: currently unimeplemented!
            if job.resolved:
                logger.error('All clients reject the request, dropping %r', job)
            else:
                logger.info('No client available, put %r to retry queue', job)
                self._retry_queue.put(job)

    def submit(self, job):
        self._job_queue.put(job)

    def job_accept(self, client, job):
        job.accept(client)
        job.proto.run_by = client.name
        job.status = common_pb2.JOB_RUNNING

    def job_reject(self, client, job, data):
        # undo the decision to assign job to client, and call callback
        job.reject(client)
        client.resolve(data, False)
        # TODO: if rejected by all clients or some condition,
        # set to rejecting state and do not retry
        job.status = common_pb2.JOB_RETRYING

    def job_progress(self, job, data):
        if job.resolved:
            raise ValueError(
                'Job {} is resolved when calling job_progress().'.format(job))
        job.assigned_to.progress(data)

    def job_done(self, job, data):
        if not job.resolved:
            raise ValueError(
                'Job {} is not resolved yet when calling job_done()'.format(job))
        job.assigned_to.resolve(data, True)
        logger.info('Job %r is now resolved', job)
        self.batch_retry()

    def run_forever(self):
        self.running = True

        while self.running:
            self.schedule()

    def shutdown(self):
        for client in self._clients.values():
            client.close()
        self._job_queue.put(None)


class Job(object):
    # for distinguishing from submission id
    NONCE = 10 ** 7

    def __init__(self, submission_id, content):
        # the event
        self.content = content

        self.proto = iruka_admin_pb2.Job(
            # TODO: naive
            id=Job.get_nonce(),
            submission_id=submission_id)

        self.status = common_pb2.JOB_PENDING
        self.callback = None
        self.blocked_names = []

        self.ack_event = threading.Event()
        self.assigned_to = None
        self.dry_run = False

    def __repr__(self):
        return '<Job {} status={}{}>'.format(
            self.id,
            self.status_repr,
            ' acked' if self.ack_event.is_set() else '')

    @classmethod
    def get_nonce(cls):
        ret = cls.NONCE
        cls.NONCE += 1
        return ret

    @property
    def id(self):
        return self.proto.id

    @property
    def status(self):
        return self.proto.status

    @status.setter
    def status(self, new_status):
        self.proto.status = new_status

    @property
    def status_repr(self):
        return common_pb2.JobStatus.Name(self.proto.status)

    @property
    def resolved(self):
        return self.status in [
            common_pb2.JOB_REJECTED,
            common_pb2.JOB_FAILED,
            common_pb2.JOB_COMPLETED]

    def accept(self, client):
        self.assigned_to = client
        self.ack_event.set()

    def reject(self, client):
        if client:
            self.blocked_names.append(client.name)
        self.ack_event.set()

    def cancel(self):
        self.status = common_pb2.JOB_FAILED
        self.ack_event.set()
