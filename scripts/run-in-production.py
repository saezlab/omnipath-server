#!/usr/bin/env python

import os
import argparse

from omnipath_server.server import _legacy as _server

__all__ = [
    'POSTGRES_ADDRESS',
    'PW_PATH',
    'SERVER_PARAM',
    'load_db',
]

POSTGRES_ADDRESS = {
    'user': 'omnipath',
    'password': None,
    'host': '/var/run/postgresql',
    'database': 'omnipath',
}

PW_PATH = os.path.expanduser('~/OMNIPATH_PSQL_PASSWD')

SERVER_PARAM = {
    'host': '127.0.0.1',
    'port': 44444,
    'dev': False,
}

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

app = _server.create_server(con = POSTGRES_ADDRESS, load_db = loader_args)

if __name__ == '__main__':

    app.run(**SERVER_PARAM)
