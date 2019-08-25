import logging
import queue


logger = logging.getLogger(__name__)


class ClientChannel(object):
    def __init__(self, name, peer):
        self.name = name
        self.peer = peer

        self.current_job = None
        self.event_queue = queue.Queue()

    def __repr__(self):
        return '<ClientChannel "{}" peer="{}">'.format(
            self.name, self.peer)

    def listen(self):
        job = self.event_queue.get()
        self.current_job = job
        return job

    def dispatch(self, job):
        self.event_queue.put(job)
        if job.ack_event.wait():
            if job.assigned_to:
                return True
            return False

    # a bit clumsy here...
    def progress(self, data):
        return self.current_job.callback(data, finalize=False)

    def resolve(self, data, accepted):
        ret = self.current_job.callback(data, finalize=accepted)
        logger.info('Client %s resolves the job %r, accept? %r',
            self,
            self.current_job,
            accepted)
        self.current_job = None
        return ret

    def close(self):
        logging.debug('Closing client channel: %r', self)
        if self.event_queue is None:
            return

        if self.current_job:
            # TODO
            logging.info(
                'Marking job {} as failed due to client channel shutting down.'
                    .format(self.current_job.id))
            self.current_job.cancel()

        self.event_queue.put(None)
        self.event_queue = None
