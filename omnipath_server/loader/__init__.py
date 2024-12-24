"""
Each service optionally has a loader, a class that populates the database.
"""

from ._legacy import Loader as LegacyLoader

__all__ = [
    'LegacyLoader',
]
