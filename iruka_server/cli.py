import logging
import logging.config
import os
import signal
import sys
import yaml
from pathlib import Path

import click
import peewee

import iruka_server.server
import iruka_server.models_hoj as models
from iruka_server import admin
from iruka_server import operations
from iruka_server.config import Config
from iruka_server.utils import MyClickGroup
from iruka_server.config import TMP_PATH


BASE_PATH = Path(__file__).parent.absolute()
logging.Formatter.default_msec_format = '%s.%03d'
logger = logging.getLogger(__name__)


def load_config(path=None):
    if path is None:
        try:
            path_dfl = BASE_PATH / '../server.yml'
            path = path_dfl.resolve(strict=True)
        except FileNotFoundError:
            raise FileNotFoundError(
                'The default config file "{}" does not exist. Specify one by --config.'.format(path_dfl.resolve()))

    with open(path, 'r') as f:
        d = yaml.safe_load(f)
    config = Config()
    config.load_from_dict(d)
    return config


def init_database(config):
    logger.debug('Initializing database')
    models.init(peewee.MySQLDatabase(**config.database_config))


def read_pidfile(pidfile_path):
    try:
        return int(pidfile_path.read_text())
    except FileNotFoundError:
        logger.critical('Cannot find pidfile')
        return None
    except ValueError:
        logger.critical('Malformed pidfile')
        return None


def naive_logger_setup():
    # naive logger setup
    from iruka.common.logging import ColoredFormatter

    formatter = ColoredFormatter(
        fmt='{color_apply}{levelname}{color_reset} {message}',
        style='{')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)


@click.group(cls=MyClickGroup)
@click.option('-c', '--config',
              help='The config file the server refers to.',
              default=lambda: None,
              show_default='Fallback to `../server-config.yml`')
@click.pass_context
def cli(ctx, config):
    ''' The server of Iruka, the backend of NeoHOJ. '''

    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['pidfile_path'] = Path(TMP_PATH) / 'iruka.pid'

    naive_logger_setup()


@cli.command('runserver')
@click.pass_context
def cli_runserver(ctx):
    ''' Start judge server as a daemon. '''

    log_config_path = Path(
        os.getenv('IRUKA_LOG_CONFIG', BASE_PATH / '../logging.yml'))
    with open(log_config_path, 'r') as f:
        logging.config.dictConfig(yaml.safe_load(f))

    config = load_config(ctx.obj['config'])
    init_database(config)

    try:
        iruka_server.server.create_pidfile(ctx.obj['pidfile_path'])
    except Exception:
        logger.exception(
            'Error writing pidfile. If the server did not shutdown '
            'properly, you must delete the pidfile first.')
        return

    return iruka_server.server.serve(
        config,
        operations.poll_for_submissions)


@cli.command('poke')
@click.pass_context
def cli_poke(ctx):
    ''' Send SIGUSR1 to server, initiating database polling. '''
    pid = read_pidfile(ctx.obj['pidfile_path'])
    if not pid: return

    try:
        os.kill(pid, signal.SIGUSR1)
    except ProcessLookupError:
        logger.exception('Cannot send SIGUSR1 to server')
        return


@cli.command('kill')
@click.pass_context
def cli_kill(ctx):
    ''' Send SIGKILL to server, killing server, then delete pidfile. '''
    pid = read_pidfile(ctx.obj['pidfile_path'])
    if not pid: return

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        logger.exception('Cannot send SIGKILL to server')
        return

    ctx.obj['pidfile_path'].unlink()


@cli.command('unmark')
@click.pass_context
@click.argument('id', required=True, type=click.INT)
@click.option('-a', '--all', 'clear_all', is_flag=True)
def cli_unmark(ctx, id, clear_all):
    ''' Clear the status of a submission. '''

    config = load_config(ctx.obj['config'])
    init_database(config)

    with models.connection_context():
        try:
            subm = models.Submission.get_by_id(id)
        except peewee.DoesNotExist:
            logger.error(
                'The submission of id %d does not exist.', id)
            return

    if clear_all:
        click.confirm(
            'This will erase all additional data with this submission. Proceed?',
            abort=True)

    return operations.unmark(subm, clear_all=clear_all)


def main(as_module=False):
    admin.add_admin_commands(cli)

    cli(obj={})


if __name__ == '__main__':
    main(as_module=True)
