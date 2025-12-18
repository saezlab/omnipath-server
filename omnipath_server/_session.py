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

import functools as _ft

from pypath_common import log as _read_log
from pypath_common import session as _session

from ._query_context import get_query_id

_get_session = _ft.partial(_session, 'omnipath_server')
log = _ft.partial(_read_log, 'omnipath_server')

session = _get_session()
_log_original = session._logger.msg


def _log(msg: str):
    """
    Log a message with query ID prefix if available.

    Args:
        msg: The message to log.
    """

    query_id = get_query_id()
    prefix = f'[ID:{query_id}] ' if query_id is not None else ''
    _log_original(f'{prefix}{msg}')
