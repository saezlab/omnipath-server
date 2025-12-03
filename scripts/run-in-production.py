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

POSTGRES_ADDRESS = {
    'user': 'omnipath',
    'password': None,
    'host': '/var/run/postgresql',
    'database': 'omnipath',
}

PW_PATH = os.path.expanduser('~/OMNIPATH_PSQL_PASSWD')

PORT = 44444

SERVER_PARAM = {
    'host': '127.0.0.1',
    'port': PORT,
    'dev': False,
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
            print('Old process found. Killing PID:', old_proc.pid)
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


if not os.path.exists(PW_PATH):

    raise RuntimeError(
        'File with Postgres password not found. '
        f'Please make sure it is available at `{PW_PATH}`.',
    )

with open(PW_PATH) as fp:

    POSTGRES_ADDRESS['password'] = fp.read().strip()

loader_args = False

if load_db():

    loader_args = {
        'path': os.path.expanduser('~'),
        'wipe': True,
    }


if not kill_old(port = PORT):

    raise RuntimeError(
        f'Port {PORT} is already in use. And failed to free it.',
    )

app = _server.create_server(con = POSTGRES_ADDRESS, load_db = loader_args)

if __name__ == '__main__':

    app.run(**SERVER_PARAM)
