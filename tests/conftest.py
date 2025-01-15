import sys
import pathlib as pl
import pytest

from omnipath_server._connection import Connection

__all__ = ['test_connection']

sys.path.append(str(pl.Path(__file__).parent.parent))


@pytest.fixture(scope='session')
def test_connection():

    return Connection('./test_config.yaml')
