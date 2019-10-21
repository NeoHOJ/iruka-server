import io
import logging
import threading
from functools import partial

import peewee
from colors import strip_color

from iruka.common.utils import (pformat, pformat_pb)
from iruka.protos import common_pb2
from iruka.protos import iruka_rpc_pb2
from iruka.protos import iruka_admin_pb2
from iruka_server import models_hoj as models
from iruka_server.scheduler import Job


logger = logging.getLogger(__name__)


def _update_subm(submission, pb_event):
    ''' Update submission object according to a **successful** event.
    Usually `job, submission` are curried for a callback function
    '''

    time_used = 0
    mem_used = 0

    stat_buffer = []

    result = pb_event.result

    for stat in result.subtasks:
        stat_buffer.append([stat.verdict, stat.time_used, stat.mem_used])
        time_used += stat.time_used
        mem_used = max(mem_used, stat.mem_used)

    submission.submission_result = stat_buffer
    submission.submission_time = time_used
    submission.submission_mem = mem_used

    return submission


def _finalize_subm(submission, result):
    final_stat = result.final_stat

    # guard against infinite loop
    if final_stat.verdict == 0:
        final_stat.verdict = common_pb2.SERR

    submission.submission_len = result.code_length
    submission.submission_score = final_stat.score
    submission.submission_status = final_stat.verdict

    if result.log['COMPILE_STDERR']:
        log = result.log['COMPILE_STDERR']
    else:
        log = result.log['COMPILE_STDOUT']

    if log.truncated:
        log += '[[[TRUNCATED]]]'

    submission.submission_error = strip_color(log.content)
    return submission


# TODO: ensure callback is called for EVERY event
def _writeback_subm_result(job, submission, pb_event, finalize=False):
    orig_status = submission.submission_status
    is_resolving = any((pb_event.HasField(x) for x in ['result', 'exception']))

    # NOTE that partial_stat is convert in server.py as a workaround!
    # when the workaround is removed the logic here should be modified as well.

    if pb_event.HasField('ack') and pb_event.ack.reject_reason:
        # somehow, client fails
        submission.submission_status = common_pb2.OTHER
        submission.submission_error = 'Client rejected this job: {} :('.format(
            iruka_rpc_pb2.SubmissionAck.RejectReason.Name(pb_event.ack.reject_reason))

    elif pb_event.HasField('result'):
        result = pb_event.result
        logger.debug('Submission result: %s', pformat_pb(result, max_level=1))

        if result.pipeline_success:
            _update_subm(submission, pb_event)
        else:
            final_verdict = result.final_stat.verdict

            arr = []
            for stat in job.content.submission_req.hoj_spec:
                # Fill in all CEs, for example
                # but how many rows should I fill in?
                arr.append([final_verdict, 0, 0])

            # actually, N/A
            submission.submission_time = 0
            submission.submission_mem = 0
            submission.submission_score = 0
            submission.submission_result = arr

        _finalize_subm(submission, pb_event.result)

        logger.debug('submission now: %s', pformat(submission.debug_view))

    elif pb_event.HasField('exception'):
        submission.submission_status = common_pb2.SERR
        # hmm should backtrace be shown to the user?

        # err_buf = io.StringIO()
        # err_buf.write(pb_event.exception.message + '\n')
        # err_buf.write(pb_event.exception.backtrace)

        # err_buf.seek(0)
        # submission.submission_error = ''.join(['> ' + ln for ln in err_buf.readlines()])

        submission.submission_error = '*** Please contact the staff ***';
        if hasattr(submission, 'submission_verbose'):
            submission.submission_verbose = '{}\n{}\n'.format(
                pb_event.exception.message,
                pb_event.exception.backtrace)
        else:
            logger.info(
                'Submission has no `submission_verbose` field. '
                'The verbose info. is lost.')

        # FIXME: the values should be computed for partial stats before
        # the exception

        arr = []
        for stat in job.content.submission_req.hoj_spec:
            arr.append([common_pb2.SERR, 0, 0])

        submission.submission_time = 0
        submission.submission_mem = 0
        submission.submission_score = 0
        submission.submission_result = arr

    if is_resolving and not job.dry_run:
        with models.connection_context():
            submission.save()

        logger.info('Updated submission %d in database.', submission.submission_id)


def unmark(subm, clear_all=False):
    with models.connection_context():
        subm.submission_status = 0

        if clear_all:
            subm.submission_result = ''
            subm.submission_error = ''
            subm.submission_time = 0
            subm.submission_mem = 0
            subm.submission_score = 0

        subm.save()


def submit_job(servicer, submission_id, callback):
    resultEnum = iruka_admin_pb2.ExecResult

    with models.connection_context():
        try:
            subm = models.Submission.get_by_id(submission_id)
        except peewee.DoesNotExist:
            # logger.error('Submission of id {} does not exist'.format(submission_id))
            return resultEnum.RECORD_NOT_FOUND

        prob = subm.problem
        pid = prob.problem_id

    logging.debug('submission: %s', pformat(subm.debug_view))
    logging.debug('problem: %d - %s', pid, prob.problem_title)

    # transform 2D list to protobuf
    prob_spec = [ common_pb2.Int64Array(value=l) for l in prob.problem_testdata ]

    prob_type_enum = iruka_rpc_pb2.SubmissionRequest.HojProblemType
    prob_type = prob_type_enum.REGULAR
    map_extra_files = {}

    if prob.problem_special:
        prob_type = prob_type_enum.SPECIAL_JUDGE
        map_extra_files['checker.cpp'] = prob.problem_check.encode()
    elif prob.problem_id in [23, 118, 119, 120, 121, 122, 257, 290, 363]:
        # TODO
        prob_type = prob_type_enum.INTERACTIVE

    job = Job(submission_id,
        iruka_rpc_pb2.ServerEvent(
            type=iruka_rpc_pb2.ServerEvent.REQUEST_JUDGE))

    if subm.submission_status != 0:
        logger.warn(
            'Submission %d has a resolved status. The result will not be updated.',
            subm.submission_id)
        job.dry_run = True

    # backward compatibility
    preset = subm.submission_preset
    if preset is None:
        preset = 'cpp'

    submObj = iruka_rpc_pb2.SubmissionRequest(
        id=job.id,
        submission_id=submission_id,
        submission=iruka_rpc_pb2.Submission(
            problem_id=pid,
            code=subm.submission_code,
            build_preset=subm.submission_preset,
            files=map_extra_files),
        hoj_spec=prob_spec,
        hoj_type=prob_type)

    job.content.submission_req.CopyFrom(submObj)

    job.callback = partial(callback, job, subm)

    if not servicer._submit_job(job):
        return resultEnum.ALREADY_IN_QUEUE

    logger.debug('Job %s submitted', job)
    return resultEnum.SUCCESS


_run = partial(submit_job, callback=_writeback_subm_result)


def poll_for_submissions(servicer):
    screwed_ids = set()

    while True:
        excluding_submissions = set([
            job.proto.submission_id for job
            in servicer.server_job_list.values()
            if not job.resolved]) | screwed_ids

        logger.info('Excluding unresolved submissions in queue: %r',
            excluding_submissions)

        with models.connection_context():
            Submission = models.Submission
            try:
                subm_active = (Submission
                    .select()
                    .where((Submission.submission_status == 0) &
                           Submission.submission_id.not_in(excluding_submissions))
                    .order_by(Submission.submission_date)
                    .get())
            except peewee.DoesNotExist:
                logger.info('No active submission, halt')
                return None

        submission_id = subm_active.submission_id

        try:
            submit_job(servicer, submission_id, _writeback_subm_result)
        except Exception:
            logger.exception('Error submitting submission of id %d', submission_id)
            screwed_ids.add(submission_id)
