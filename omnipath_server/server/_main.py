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

__all__ = [
    'DOMAINS',
    'SERVERS',
    'handle_request',
    'route_requests',
]

DOMAINS = {
    'legacy': ('next.omnipathdb.org', 'omnipathdb.org'),
    'metabo': ('metabo.omnipathdb.org'),
}

SERVERS = {}

main_server = Sanic('RootServer')


@main_server.middleware('request')
async def route_requests(request: Request):
    '''
    Sets the request context to the correct service.

    Args:
        request:
            Instance of `Sanic.Request` containing the user request.
    '''

    request.ctx.server = None

    for service, domains in DOMAINS.items():

        if (
            any(request.host.startswith(d) for d in domains) and
            service in SERVERS
        ):

            request.ctx.server = SERVERS[service]


@main_server.route(
    '/<path:path>',
    methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
)
async def handle_request(request: Request, path: str):
    '''
    Passes the request to the assigned service.

    Args:
        request:
            Instance of `Sanic.Request` containing the user request.
        path:
            Path for the database that has to process the request (e.g. for the
            legacy service: interactions, annotations, etc.).
    '''

    if (server := request.ctx.server) is not None:

        return await server.handle_request(request)

    return response.text(f'No such service: {request.host}', status = 500)
