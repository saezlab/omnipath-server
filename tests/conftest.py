import sys
import pathlib as pl
import pytest

from omnipath_server._connection import Connection
from omnipath_server.service._legacy import LegacyService 

__all__ = ['test_connection', 'test_path_legacy']

sys.path.append(str(pl.Path(__file__).parent.parent))


@pytest.fixture
def legacy_data_path() -> str:

    return './tests/data/legacy/'


def pytest_addoption(parser):

    parser.addoption(
        '--db-config',
        default = './tests/test_config.yaml',
        action = 'store',
        help = 'Path to database config YAML',
    )


@pytest.fixture(scope = 'session')
def postgres_con(request) -> Connection:

    config_path = request.config.getoption('--db-config')

    return Connection(config_path)

@pytest.fixture(scope = 'session')
def legacy_service(request) -> LegacyService:

    con = postgres_con(request)

    return LegacyService(con)
