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

from collections.abc import Generator

from sanic import Sanic, Request, response

from omnipath_server import _log
from omnipath_server.service import LegacyService

__all__ = [
    'create_server',
]


def create_server(**kwargs):

    _log('Creating new legacy server...')
    legacy_server = Sanic('LegacyServer')
    legacy_server.ctx.service = LegacyService(kwargs)

    async def serve_tsv(request: Request, lines: Generator):

        response = await request.respond(
            content_type = 'text/tab-separated-values',
        )

        for line in lines:

            await response.send(line)

        await response.eof()

        return response


    @legacy_server.route('/<path:path>')
    async def legacy_handler(request: Request, path: str):

        if (
            not path.startswith('_') and
            # TODO: maintain a registry of endpoints,
            # don't rely on this getattr
            (endpoint := getattr(legacy_server.ctx.service, path, None))
        ):

            return await serve_tsv(request, endpoint(**request.args))

        return response.text(f'No such path: {path}', status = 404)

    _log('Legacy server ready.')

    return legacy_server
