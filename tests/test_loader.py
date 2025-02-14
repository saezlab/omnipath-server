from sqlalchemy import text, inspect
import pytest

__all__ = [
    'test_create_table',
    'test_load_tables',
]


def test_create_table(legacy_loader):

    tables = inspect(legacy_loader.con.engine).get_table_names()

    tables_expected = {
        'annotations',
        'complexes',
        'enzsub',
        'interactions',
        'intercell',
    }

    assert set(tables) == tables_expected


def test_load_tables(legacy_db_loaded):

    loader = legacy_db_loaded

    query = 'SELECT COUNT(*) FROM %s;'

    for table in loader.tables:

        result = loader.con.session.execute(text(query % table))

        assert next(result)[0] == 99
