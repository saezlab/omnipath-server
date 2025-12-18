#!/usr/bin/env python

#
# This file is part of the `omnipath_server` Python module
#
# Copyright 2024-2025
# Heidelberg University Hospital
#
# File author(s): OmniPath Team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

"""
Query context management for tracking concurrent queries.

This module provides context variables to track query IDs across
async/concurrent request handling, enabling better log traceability.
"""

import itertools
import contextvars

__all__ = [
    'get_query_id',
    'set_query_id',
    'reset_query_id',
]


# Global counter for query IDs
_query_counter = itertools.count(1)

# Context variable to store current query ID
_query_id_ctx: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    'query_id',
    default=None,
)


def set_query_id() -> int:
    """
    Set a new query ID in the current context.

    Generates a unique sequential query ID and stores it in the context.
    This should be called at the entry point of each query.

    Returns:
        The generated query ID.
    """

    query_id = next(_query_counter)
    _query_id_ctx.set(query_id)

    return query_id


def get_query_id() -> int | None:
    """
    Get the current query ID from context.

    Returns:
        The current query ID, or None if not set.
    """

    return _query_id_ctx.get()


def reset_query_id():
    """
    Reset the query ID in the current context.

    This is mainly useful for testing or cleanup.
    """

    _query_id_ctx.set(None)
