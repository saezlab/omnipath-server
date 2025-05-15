#!/usr/bin/env python

import os

from omnipath_server.server import _legacy

__all__ = [
    'POSTGRES_ADDRESS',
    'PW_PATH',
]

POSTGRES_ADDRESS = {
    'user': 'omnipath',
    'password': None,
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

PW_PATH = '~/OMNIPATH_PSQL_PASSWD'

if not os.path.exists(PW_PATH):

    raise RuntimeError(
        'File with Postgres password not found. '
        f'Please make sure it is available at `{PW_PATH}`.',
    )

with open(PW_PATH) as fp:

    POSTGRES_ADDRESS['password'] = fp.read().strip()

app = _legacy.create_server(con = POSTGRES_ADDRESS)

if __name__ == '__main__':

    app.run(host = '127.0.0.1', port = 44444, dev = False)
