import atexit
import hmac
import logging
import os
import signal
import threading
from concurrent import futures
from queue import Queue
from pathlib import Path
from functools import partial

import click
import grpc
from google.protobuf import empty_pb2
from iruka.common.utils import pformat, pformat_pb
from iruka.protos import __proto_revision__
from iruka.protos import (
    common_pb2, iruka_rpc_pb2, iruka_rpc_pb2_grpc, iruka_admin_pb2_grpc)
from iruka_server import __version__
from iruka_server.scheduler import Scheduler, Job
from iruka_server.channel import ClientChannel
from iruka_server.admin import IrukaAdminServicer
from iruka_server.config import TMP_PATH


logger = logging.getLogger(__name__)


class IrukaRpcServicer(iruka_rpc_pb2_grpc.IrukaRpcServicer):
    def __init__(self, client_info, scheduler):
        self.client_info = client_info
        self.scheduler = scheduler

        self.server_job_list = {}
        self.is_shutting_down = False

    def Version(self, request, context):
        return iruka_rpc_pb2.VersionInfo(
            version=__version__,
            proto_revision=__proto_revision__)

    def Listen(self, request, context):
        cur_client_info = None

        for cand in self.client_info:
            # NOTE that this effectively restricts tokens to ASCII
            if hmac.compare_digest(request.token, cand['token']):
                cur_client_info = cand
                break
        else:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Invalid token')
            return

        # currently only one client is allowed at this time
        # if len(self.scheduler._clients) >= 1:
        #     context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
        #                   'Multi-clients structure is not implemented :(')
        #     return

        # note that client object is created here, while add_callback can fail
        # and early-return. this arrangement is only for not cycle-referencing
        # when registering that callback.
        cur_client = ClientChannel(cur_client_info['name'], context.peer())

        if not context.add_callback(
            partial(self._on_disconnect, cur_client)):
            return

        self.scheduler.client_online(cur_client)

        logger.info('Client [{}] connected from {}'.format(
            cur_client.name, cur_client.peer))

        yield iruka_rpc_pb2.ServerEvent(type=iruka_rpc_pb2.ServerEvent.ACK)

        # listen from the client's event queue until it drains
        while True:
            job = cur_client.listen()
            if job is None:
                return
            yield job.content

    def ReportSubmission(self, request, context):
        # prioritize operating on scheduler's methods to mutate the state

        logger.debug('Start submission report')

        try:
            event = next(request)
        except StopIteration:
            logger.error('Client call ReportSubmission with an empty payload!')
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Empty report')

        if not event.HasField('ack'):
            return iruka_rpc_pb2.GeneralResponse(
                ok=0, msg='The first reported event should be an ACK!')

        event_ack = event.ack
        logger.debug('Received ACK')
        job_obj = self.server_job_list.get(event_ack.id, None)
        if job_obj is None:
            logger.warn('Client sends an invalid job id %d', event_ack.id)
            return iruka_rpc_pb2.GeneralResponse(ok=0, msg='invalid job id!')

        client = self.scheduler._clients.get(context.peer(), None)
        if client is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Uncognized peer')
            return

        if event_ack.reject_reason:
            reason_str = iruka_rpc_pb2.SubmissionAck.RejectReason.Name(
                event_ack.reject_reason)
            logger.warn('Submission is rejected by client %s: %s.',
                client.peer, reason_str)
            self.scheduler.job_reject(client, job_obj, event)
            return iruka_rpc_pb2.GeneralResponse(ok=1, msg='rejection acked')

        self.scheduler.job_accept(client, job_obj)
        self.scheduler.job_progress(job_obj, event)

        stat_buffer = []
        has_exceptions = False

        for event in request:
            if event.HasField('result'):
                if len(event.result.subtasks):
                    logging.warn('Client reports a non-empty result.subtasks, '
                        'which is unsupported for now.')
                break

            if event.HasField('exception'):
                logger.warn('Exception raised when judging: %s', event.exception.message)
                has_exceptions = True
            elif event.HasField('partial_stat'):
                for stctx in event.partial_stat.values:
                    # FIXME: ignore number & label, assume in order
                    stat = stctx.stat
                    stat_buffer.append(stat)
            else:
                logger.warn('Unexpected event type encountered: %s; do nothing.', event.WhichOneOf('event'))

            self.scheduler.job_progress(job_obj, event)

        # FIXME: if partial stat is sent, concating here is not safe!
        event.result.subtasks.extend(stat_buffer)

        logger.debug('End submission report')
        logger.debug('Stat buffer: %s', pformat([pformat_pb(s, max_level=0) for s in stat_buffer]))

        if has_exceptions:
            job_obj.status = common_pb2.JOB_FAILED
        else:
            job_obj.status = common_pb2.JOB_COMPLETED

        self.scheduler.job_done(job_obj, event)

        return iruka_rpc_pb2.GeneralResponse(ok=1, msg='accepted')

    @property
    def clients(self):
        return self.scheduler._clients.values()

    def _on_disconnect(self, client):
        # a hard-to-reproduce bug tells that print() cannot obtain write lock
        # to stdout on interpreter shutdown. do not execute this callback then
        if self.is_shutting_down:
            return
        logger.info('Client [{}] DISconnected from {}'.format(
            client.name, client.peer))
        self.scheduler.client_offline(client)

    def _submit_job(self, job):
        # workaround
        if job.id in self.server_job_list:
            self.server_job_list[job.id].ack_event.set()

        for j in self.server_job_list.values():
            if (job.proto.submission_id == j.proto.submission_id and
                not j.resolved):

                logger.warn('Not creating job for existing, unresolved submission id')
                return False

        self.server_job_list[job.id] = job
        self.scheduler.submit(job)
        return True

    def _shutdown(self):
        self.is_shutting_down = True
        self.scheduler.shutdown()
        for job in self.server_job_list.values():
            if not job.ack_event.is_set():
                logger.warn('Cancelling unresolved job %s', job)
                job.reject(None)
        # self.scheduler.kill()


def create_pidfile(pidfile_path):
    # fails if the pidfile already exists
    with pidfile_path.open(mode='x') as f:
        f.write(str(os.getpid()))

    atexit.register(unlink_pidfile, pidfile_path)


def unlink_pidfile(pidfile_path):
    try:
        pidfile_path.unlink()
    except OSError:
        pass


def serve(config, polling_fn):
    scheduler = Scheduler()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = IrukaRpcServicer(config.clients, scheduler)
    iruka_rpc_pb2_grpc.add_IrukaRpcServicer_to_server(servicer, server)
    conn_path = '{}:{:d}'.format(config.host, config.port)
    if config.use_https:
        with open(config.ssl_key, 'rb') as f:
            privkey = f.read()
        with open(config.ssl_cert, 'rb') as f:
            fullchain = f.read()
        creds = ((privkey, fullchain),)
        server_credentials = grpc.ssl_server_credentials(creds)
        server.add_secure_port(conn_path, server_credentials)
    else:
        server.add_insecure_port(conn_path)

    server.start()
    print(' \U0001f42c Iruka Server')
    print('>>> Server started at {}:{}'.format(config.host, config.port))

    sockpath = Path(TMP_PATH) / 'iruka.sock'
    ipc_uri = 'unix://{!s}'.format(sockpath.absolute())
    admin_console = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    iruka_admin_pb2_grpc.add_IrukaAdminServicer_to_server(IrukaAdminServicer(servicer), admin_console)
    bind_ret = admin_console.add_insecure_port(ipc_uri)
    # FIXME: can grpc be silent on failed syscalls?
    if bind_ret != 1:
        logger.warn('Failed to start the admin console at {}. No admin console will be available.'.format(path_to_ipc))
    else:
        admin_console.start()
        print('>>> Admin console started at {}'.format(ipc_uri))

    # this looks so arcane
    signal.signal(signal.SIGUSR1, lambda signum, frame: polling_fn(servicer))
    print('>>> Send SIGUSR1 to poll for pending submissions in the database.')

    # FIXME
    threading.Thread(target=scheduler.run_forever).start()

    try:
        while True:
            signal.pause()
    except KeyboardInterrupt:
        print('Interrupted by SIGINT, stopping server...')
        servicer._shutdown()
        server.stop(0)
        admin_console.stop(0)
