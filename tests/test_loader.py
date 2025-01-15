import pytest

from omnipath_server.loader._legacy import Loader


def create_table(test_connection, test_path_legacy):

    loader = Loader(path=test_path_legacy, con=test_connection)
    loader.create()

    # TODO: assert


#def test_table():