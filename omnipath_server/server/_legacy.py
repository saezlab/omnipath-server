#!/usr/bin/env python

#
# This file is part of the `omnipath_server` Python module
#
# Copyright 2024
# Heidelberg University Hospital
#
# File author(s): OmniPath Team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

from sanic import Sanic, Request, response

from omnipath_server import _log
from omnipath_server.service import LegacyService

__all__ = [
#    'create_server',
]

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

#def create_server(**kwargs):

_log('Creating new legacy server...')
legacy_server = Sanic('LegacyServer')
legacy_server.ctx.service = LegacyService(con_param)

@legacy_server.route('/<path:path>')
async def legacy_handler(request: Request, path: str):

    if (
        not path.startswith('_') and
        # TODO: maintain a registry of endpoints,
        # don't rely on this getattr
        (endpoint := getattr(request.ctx.service, path, None))
    ):

        return response.text(endpoint(**request.args))

    return response.text(f'No such path: {path}', status = 404)

if __name__ == '__main__':
    
    legacy_server.run()

_log('Legacy server ready.')

    # return legacy_server
