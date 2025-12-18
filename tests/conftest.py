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
    'pytest_configure',
    'pytest_collection_modifyitems',
]

sys.path.append(str(pl.Path(__file__).parent.parent))


def pytest_configure(config):
    """Register custom markers."""

    config.addinivalue_line(
        'markers',
        'requires_loader: mark test as requiring database loading '
        '(skipped with --use-existing-db)',
    )


def pytest_collection_modifyitems(config, items):
    """Skip tests marked with requires_loader when --use-existing-db is set."""

    if config.getoption('--use-existing-db'):
        skip_loader = pytest.mark.skip(
            reason = 'Skipping loader tests (--use-existing-db enabled)',
        )
        for item in items:
            if 'requires_loader' in item.keywords:
                item.add_marker(skip_loader)


def pytest_addoption(parser):

    parser.addoption(
        '--db-config',
        default = './tests/test_config.yaml',
        action = 'store',
        help = 'Path to database config YAML',
    )
    parser.addoption(
        '--use-existing-db',
        action = 'store_true',
        default = False,
        help = 'Use existing database (skip loading, skip loader tests)',
    )


@pytest.fixture(scope = 'session')
def legacy_data_path() -> str:

    return './tests/data/legacy/'


@pytest.fixture(scope = 'session')
def postgres_con(request) -> Connection:

    config_path = request.config.getoption('--db-config')

    return Connection(config_path)


@pytest.fixture(scope = 'session')
def legacy_loader(legacy_data_path, postgres_con, request):

    use_existing_db = request.config.getoption('--use-existing-db')

    loader = Loader(
        path = legacy_data_path,
        con = postgres_con,
        wipe = not use_existing_db,
    )

    if not use_existing_db:
        loader.create()

    return loader


@pytest.fixture(scope = 'session')
def legacy_db_loaded(legacy_loader, request):

    use_existing_db = request.config.getoption('--use-existing-db')

    if not use_existing_db:
        legacy_loader.load()

    return legacy_loader


@pytest.fixture(scope = 'session')
def legacy_service(postgres_con, legacy_db_loaded) -> LegacyService:

    return LegacyService(postgres_con)
