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

from sanic import Sanic, response

from ..service import LegacyService

__all__ = [
    'create_server',
]


def create_server(**kwargs):

    legacy_server = Sanic('LegacyServer')
    legacy_server.ctx.service = LegacyService(**kwargs)

    @legacy_server.route('/<path:path>')
    async def legacy_handler(request, path):

        return response.text(f'Legacy: {path}')


    return legacy_server
