import pytest
from omnipath_server.service import _legacy


def test_enzsub_statement(legacy_service):
    pass
    # TODO: Get only query to check it's correct
    query_str = str(legacy_service.query(
        'enzsub',
        enzymes = 'P06239',
        substrates = 'O14543',
        limit = 10)
    )

    where = query_str.split("WHERE")[1]
    expectation = (" (enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
                   "((enzsub.enzyme = ANY (ARRAY[%(param_2)s])) OR "
                   "(enzsub.enzyme_genesymbol = ANY (ARRAY[%(param_3)s]))) AND "
                   "((enzsub.substrate = ANY (ARRAY[%(param_4)s])) OR "
                   "(enzsub.substrate_genesymbol = ANY (ARRAY[%(param_5)s]))) \n "
                   "LIMIT %(param_6)s")

    assert where == expectation
