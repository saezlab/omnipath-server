#!/usr/bin/env python

import os
import argparse

from omnipath_server.loader import _legacy as _loader
from omnipath_server.server import _legacy as _server

__all__ = [
    'POSTGRES_ADDRESS',
    'PW_PATH',
    'load_db',
]

POSTGRES_ADDRESS = {
    'user': 'omnipath',
    'password': None,
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

PW_PATH = os.path.expanduser('~/OMNIPATH_PSQL_PASSWD')

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

if load_db():

    _loader.Loader(
        path = os.path.expanduser('~'),
        con = POSTGRES_ADDRESS,
        wipe = True,
    ).load()

app = _server.create_server(con = POSTGRES_ADDRESS)

if __name__ == '__main__':

    app.run(host = '127.0.0.1', port = 44444, dev = False)
