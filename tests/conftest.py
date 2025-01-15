import sys
import pathlib as pl
import pytest

from omnipath_server._connection import Connection

__all__ = ['test_connection', 'test_path_legacy']

sys.path.append(str(pl.Path(__file__).parent.parent))


@pytest.fixture(scope='session')
def test_connection():

    return Connection('./tests/test_config.yaml')

@pytest.fixture
def test_path_legacy():

    return './tests/data/legacy/'