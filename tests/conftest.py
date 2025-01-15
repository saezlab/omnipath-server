import sys
import pathlib as pl
import pytest

from omnipath_server.loader._legacy import Loader

__all__ = ['loader']

sys.path.append(str(pl.Path(__file__).parent.parent))


@pytest.fixture
def loader():

    return Loader(path = './data/legacy/', con = './test_config.yaml')
