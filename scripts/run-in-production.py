#!/usr/bin/env python

import os
import signal
import argparse

import psutil

from omnipath_server.server import _legacy as _server

__all__ = [
    'PORT',
    'POSTGRES_ADDRESS',
    'PW_PATH',
    'SERVER_PARAM',
    'kill_old',
    'load_db',
]

#  All settings default to the omnipathdb.org production values but can be
#  overridden from the environment, so the same script drives any deployment
#  (e.g. a containerised Postgres on a host port; see docker-compose.yml).
POSTGRES_ADDRESS = {
    'user': os.environ.get('OMNIPATH_PSQL_USER', 'omnipath'),
    'password': os.environ.get('OMNIPATH_PSQL_PASSWD') or None,
    'host': os.environ.get('OMNIPATH_PSQL_HOST', '/var/run/postgresql'),
    'database': os.environ.get('OMNIPATH_PSQL_DB', 'omnipath'),
}

if _psql_port := os.environ.get('OMNIPATH_PSQL_PORT'):

    POSTGRES_ADDRESS['port'] = int(_psql_port)

PW_PATH = os.path.expanduser('~/OMNIPATH_PSQL_PASSWD')

PORT = int(os.environ.get('OMNIPATH_SERVER_PORT', 44444))

SERVER_PARAM = {
    'host': os.environ.get('OMNIPATH_SERVER_HOST', '127.0.0.1'),
    'port': PORT,
    'dev': False,
    'single_process': True,
}


def kill_old(port: int) -> bool:
    """
    If an old instance is running, kill it to free the port.
    """

    old_proc = None

    for proc in psutil.process_iter(['pid', 'name', 'net_connections']):

        try:

            for conn in proc.net_connections():

                if conn.laddr.port == port:

                    old_proc = proc
                    break

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):

            continue

    if old_proc is not None:

        try:

            os.killpg(os.getpgid(old_proc.pid), signal.SIGTERM)

        except OSError:

            return False

        else:

            return True

    return True


def load_db() -> bool:

    parser = argparse.ArgumentParser(
        description = 'Run the OmniPath server in production mode.',
    )

    parser.add_argument(
        '--load-db',
        action = 'store_true',
        default = False,
        help = 'Populate the database (default: False)',
    )

    args = parser.parse_args()

    return args.load_db


if POSTGRES_ADDRESS['password'] is None:

    if not os.path.exists(PW_PATH):

        raise RuntimeError(
            'No Postgres password: set `OMNIPATH_PSQL_PASSWD` or provide the '
            f'file `{PW_PATH}`.',
        )

    with open(PW_PATH) as fp:

        POSTGRES_ADDRESS['password'] = fp.read().strip()

loader_args = False

if load_db():

    loader_args = {
        'path': os.environ.get(
            'OMNIPATH_BUILD_DIR',
            os.path.expanduser('~'),
        ),
        'wipe': True,
    }

app = _server.create_server(con = POSTGRES_ADDRESS, load_db = loader_args)
app.config.WORKER_START_TIMEOUT = 300


if __name__ == '__main__':

    if not kill_old(port = PORT):

        raise RuntimeError(
            f'Port {PORT} is already in use. And failed to free it.',
        )

    app.run(**SERVER_PARAM)
