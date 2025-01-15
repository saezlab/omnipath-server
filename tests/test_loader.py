import pytest

from omnipath_server.loader._legacy import Loader


def create_table():

    loader = Loader('./data/legacy/')
    loader.create() # WIP


#def test_table():