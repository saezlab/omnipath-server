import pytest
from sqlalchemy import inspect

from omnipath_server.loader._legacy import Loader


def create_table(test_connection, test_path_legacy):

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
