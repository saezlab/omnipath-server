import pytest
from sqlalchemy import inspect
from sqlalchemy import text

from omnipath_server.loader._legacy import Loader


def test_create_table(postgres_con, legacy_data_path):

    loader = Loader(path = legacy_data_path, con = postgres_con)
    loader.create()

    tables = inspect(loader.con.engine).get_table_names()

    tables_expected = {
        'annotations',
        'complexes',
        'enzsub',
        'interactions',
        'intercell',
    }

    assert set(tables) == tables_expected


def test_load_tables(postgres_con, legacy_data_path):

    loader = Loader(path = legacy_data_path, con = postgres_con, wipe = True)
    loader.create()
    loader.load()

    query = 'SELECT COUNT(*) FROM %s;'

    for table in loader.tables:

        result = loader.con.session.execute(text(query % table))

        assert next(result)[0] == 99
