import os
import sys
from pathlib import Path
from contextlib import contextmanager

import grpc
import click
import logging

from google.protobuf import empty_pb2
from iruka.protos import common_pb2
from iruka.protos import (iruka_admin_pb2, iruka_admin_pb2_grpc)
from iruka_server import models_hoj as models
from iruka_server.config import TMP_PATH
from iruka_server.operations import _run


logger = logging.getLogger(__name__)


class IrukaAdminServicer(iruka_admin_pb2_grpc.IrukaAdminServicer):
    def __init__(self, rpc_servicer):
        self.rpc_servicer = rpc_servicer

    def ListClients(self, request, context):
        clients = {}
        for ci in self.rpc_servicer.client_info:
            clients[ci['name']] = iruka_admin_pb2.Client(
                name=ci['name'])

        for cobj in self.rpc_servicer.clients:
            job_id = 0
            if cobj.current_job is not None:
                job_id = cobj.current_job.id
            clients[cobj.name].MergeFrom(
                iruka_admin_pb2.Client(
                    address=cobj.peer,
                    job_id=job_id))
        return iruka_admin_pb2.Clients(clients=clients.values())

    def ListJobs(self, request, context):
        jobs = []
        for j in self.rpc_servicer.server_job_list.values():
            jobs.append(j.proto)
        return iruka_admin_pb2.Jobs(jobs=jobs)

    def ExecSubmission(self, request, context):
        try:
            logger.info('Executing judgement of submission id {}'.format(request.id))
            result = _run(self.rpc_servicer, request.id)
        except Exception as err:
            logger.exception('--- Error requesting to judge submission ---')
            result = iruka_admin_pb2.ExecResult.UNKNOWN

        return iruka_admin_pb2.ExecResult(result=result)


sock_path = (Path(TMP_PATH) / 'iruka.sock').absolute()


def grpc_socket(request, die_if_connect_fail=True):
    def _grpc_socket_call(sockpath, channelClass=grpc.insecure_channel):
        ipc_uri = 'unix://{!s}'.format(sockpath)
        logger.debug('Using IPC %s', ipc_uri)
        try:
            with channelClass(ipc_uri) as channel:
                stub = iruka_admin_pb2_grpc.IrukaAdminStub(channel)
                return request(channel, stub)
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.UNAVAILABLE:
                if die_if_connect_fail:
                    logger.error('*** Cannot connect to the socket %s. ***',
                        ipc_uri)
                    sys.exit(1)
                return None
            else:
                raise

    return _grpc_socket_call


@click.command('client')
def cli_client():
    ''' Manage clients. '''

    @grpc_socket
    def request(channel, stub):
        payload = empty_pb2.Empty()
        return stub.ListClients(payload)

    resp = request(sock_path)

    print('Clients: {:d}'.format(len(resp.clients)))
    for client in resp.clients:
        if client.address:
            print('{:12s} {:24s} {}'.format(
                client.name,
                client.address,
                client.job_id if client.job_id else '(Idle)'))
        else:
            print('{:12s} {:24s}'.format(
                client.name,
                '(Unconnected)'))



@click.command('job')
def cli_job():
    ''' Manage jobs. '''

    @grpc_socket
    def request(channel, stub):
        payload = iruka_admin_pb2.JobFilter()
        return stub.ListJobs(payload)

    resp = request(sock_path)

    print('Jobs: {:d}'.format(len(resp.jobs)))
    for job in resp.jobs:
        print(job.id, job.submission_id, common_pb2.JobStatus.Name(job.status))


@click.command('exec')
@click.argument('id', required=True, type=click.INT)
def cli_exec(id):
    ''' Run judge of the specified submission ID. '''

    @grpc_socket
    def request(channel, stub):
        payload = iruka_admin_pb2.SubmissionId(id=id)
        return stub.ExecSubmission(payload)

    resp = request(sock_path)

    if resp.result != 0:
        errstr = iruka_admin_pb2.ExecResult.ExecResultCode.Name(resp.result)
        logger.error('Failed to submit job: %s.', errstr)


def add_admin_commands(cli):
    # the order here will be adopted in command usage
    commands = [
        cli_client,
        cli_job,
        cli_exec,
    ]

    for cmd in commands:
        cli.add_command(cmd)
