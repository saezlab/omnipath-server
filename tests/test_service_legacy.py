import pytest
from omnipath_server.service import _legacy


def test_enzsub_statement(legacy_service):
    pass
    # TODO: Get only query to check it's correct
    #req = legacy_service.enzsub(enzymes = 'P06239', substrates = 'O14543', limit = 10, format = 'raw')