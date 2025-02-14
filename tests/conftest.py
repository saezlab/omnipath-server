import sys
import pathlib as pl

import pytest

from omnipath_server._connection import Connection
from omnipath_server.loader._legacy import Loader
from omnipath_server.service._legacy import LegacyService

__all__ = [
    'legacy_data_path',
    'legacy_db_loaded',
    'legacy_loader',
    'legacy_service',
    'postgres_con',
    'pytest_addoption',
]

sys.path.append(str(pl.Path(__file__).parent.parent))


def pytest_addoption(parser):

    parser.addoption(
        '--db-config',
        default = './tests/test_config.yaml',
        action = 'store',
        help = 'Path to database config YAML',
    )


@pytest.fixture(scope = 'session')
def legacy_data_path() -> str:

    return './tests/data/legacy/'


@pytest.fixture(scope = 'session')
def postgres_con(request) -> Connection:

    config_path = request.config.getoption('--db-config')

    return Connection(config_path)


@pytest.fixture(scope = 'session')
def legacy_loader(legacy_data_path, postgres_con):

    loader = Loader(path = legacy_data_path, con = postgres_con, wipe = True)
    loader.create()

    return loader


@pytest.fixture(scope = 'session')
def legacy_db_loaded(legacy_loader):

    legacy_loader.load()

    return legacy_loader


@pytest.fixture(scope = 'session')
def legacy_service(postgres_con, legacy_db_loaded) -> LegacyService:

    return LegacyService(postgres_con)
