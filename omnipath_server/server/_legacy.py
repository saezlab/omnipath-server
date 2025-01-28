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

__all__ = [
    'legacy_handler',
]

legacy_server = Sanic('LegacyServer')

@legacy_server.route('/<path:path>')
async def legacy_handler(request, path):

    return response.text(f'Legacy: {path}')
