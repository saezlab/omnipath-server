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
from pypath_common import _misc
from sanic.worker.manager import WorkerManager

from omnipath_server import _log
from omnipath_server.service import LegacyService

__all__ = [
    'create_server',
]

WorkerManager.THRESHOLD = 1200

def create_server(con: dict, load_db: bool | dict = False, **kwargs) -> Sanic:
    """
    Creates and sets up the legacy database server (implemented in Sanic).

    Args:
        con:
            Database connection parameters.
        load_db:
            Populate the database if True, if a dict, it will be passed to the
            loader as arguments.
        **kwargs:
            Arguments passed to the `LegacyService`.

    Returns:
        Instance of the server.
    """

    _log('Creating new legacy server...')
    legacy_server = Sanic('LegacyServer')
    legacy_server.state.args = {
        'con': con,
        'load_db': load_db,
        'service_args': kwargs,
    }


    @legacy_server.main_process_start
    async def maybe_load(app, _):

        load_db = app.state.args['load_db']

        if (dct := isinstance(load_db, dict)) or load_db:

            _log('Loading legacy database...')
            from omnipath_server.loader import _legacy as _loader
            load_db = load_db if dct else {}
            loader = _loader.LegacyLoader(con = con, **load_db).load()


    @legacy_server.before_server_start
    async def worker_startup(app, _):

        con = app.state.args['con']
        kwargs = app.state.args['service_args']
        app.ctx.service = LegacyService(con = con, **kwargs)

        async def stream(
                request: Request,
                lines: Generator,
                json_format: bool,
        ) -> None:
            """
            Streams the response from the server from a given request.

            Args:
                request:
                    Instance of `Sanic.Request` containing the user request.
                lines:
                    Response as a generator of tuples.
                json_format:
                    Whether to respond in JSON format or not (if not JSON, defaults
                    to TSV).
            """

            content_type = 'application/json' if json_format else 'text/plain'

            _response = await request.respond(content_type = content_type)

            for line in lines:

                await _response.send(line)

            await _response.eof()


        @legacy_server.route('/<path:path>')
        async def legacy_handler(request: Request, path: str):
            """
            Request handler

            Args:
                request:
                    Instance of `Sanic.Request` containing the user request.
                path:
                    Path for the database that has to process the request (e.g.
                    interactions, annotations, etc.).

            Returns:
                Server response as text.
            """

            path = path.split('/')

            if (
                not path[0].startswith('_') and
                # TODO: maintain a registry of endpoints,
                # don't rely on this getattr
                (endpoint := getattr(legacy_server.ctx.service, path[0], None))
            ):

                resources = endpoint == 'resources'
                format = _misc.first(request.args.pop('format', ('tsv',)))
                json_format = not resources and format == 'json'

                precontent = ('[\n',) if json_format else ()
                postcontent = (']',) if json_format else ()
                postformat = (
                    (lambda x, last: f'{x}\n' if last else f'{x},\n')
                        if json_format else
                    (lambda x, last: x.rstrip('\n') if last else x)
                )

                lines = endpoint(
                    postformat = postformat,
                    precontent = precontent,
                    postcontent = postcontent,
                    path = path,
                    format = format,
                    **request.args,
                )

                await stream(request, lines, json_format or resources)

            else:

                return response.text(
                    f'No such path: {"/".join(path)}',
                    status = 404,
                )

        _log('Legacy server ready.')

    return legacy_server
