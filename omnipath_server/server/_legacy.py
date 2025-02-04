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


def create_server(**kwargs) -> Sanic:
    '''
    Creates and sets up the legacy database server (implemented in Sanic).

    Args:
        **kwargs: Arguments passed to the `LegacyService`.

    Returns:
        Instance of the server.
    '''

    _log('Creating new legacy server...')
    legacy_server = Sanic('LegacyServer')
    legacy_server.ctx.service = LegacyService(kwargs)

    async def stream(
            request: Request,
            lines: Generator,
            json_format: bool,
    ) -> None:
        '''
        Streams the response from the server from a given request.

        Args:
            request:
                Instance of `Sanic.Request` containing the user request.
            lines:
                Response as a generator of tuples.
            json_format:
                Whether to respond in JSON format or not (if not JSON, defaults
                to TSV).
        '''

        content_type = (
            'application/json'
                if json_format else
            'text/tab-separated-values'
        )

        _response = await request.respond(content_type = content_type)

        for line in lines:

            await _response.send(line)

        await _response.eof()


    @legacy_server.route('/<path:path>')
    async def legacy_handler(request: Request, path: str):
        '''
        Request handler

        Args:
            request:
                Instance of `Sanic.Request` containing the user request.
            path:
                Path for the database that has to process the request (e.g.
                interactions, annotations, etc.).

        Returns:
            Server response as text.
        '''

        if (
            not path.startswith('_') and
            # TODO: maintain a registry of endpoints,
            # don't rely on this getattr
            (endpoint := getattr(legacy_server.ctx.service, path, None))
        ):

            json_format = request.args.get('format', 'tsv') == 'json'

            precontent = ('[\n',) if json_format else ()
            postcontent = (']',) if json_format else ()
            postformat = (
                (lambda x, last: f'{x}\n' if last else f'{x},\n')
                    if json_format else
                (lambda x, last: x[:-1] if last else x)
            )

            lines = endpoint(
                postformat = postformat,
                precontent = precontent,
                postcontent = postcontent,
                **request.args,
            )

            await stream(request, lines, json_format)

        else:

            return response.text(f'No such path: {path}', status = 404)

    _log('Legacy server ready.')

    return legacy_server
