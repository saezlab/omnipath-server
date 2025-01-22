import pytest
from sqlalchemy import inspect
from sqlalchemy import text

from omnipath_server.loader._legacy import Loader


def test_create_table(test_connection, test_path_legacy):

    loader = Loader(path=test_path_legacy, con=test_connection)
    loader.create()

    tables = inspect(loader.con.engine).get_table_names()

    tables_expected = {
        'annotations',
        'complexes',
        'enz_sub',
        'interactions',
        'intercell',
    }

    assert set(tables) == tables_expected

def test_load_tables(test_connection, test_path_legacy):

    loader = Loader(path=test_path_legacy, con=test_connection, wipe=True)
    loader.create()
    loader.load()

    query = 'SELECT COUNT(*) FROM %s;'

    for table in loader.tables:
        
        result = loader.con.session.execute(text(query % table))
        print(result)
        assert next(result)[0] == 99
