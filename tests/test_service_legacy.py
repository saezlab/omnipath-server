import pytest

from omnipath_server.service import _legacy

__all__ = [
    'WHERE_CASES',
    'test_where_statement',
]


WHERE_CASES = {
    'enzsub': [
        (
            {'enzymes': 'P06239', 'substrates': 'O14543', 'limit': 10},
            "((enzsub.ncbi_tax_id = ANY (ARRAY[9606])) AND "
            "((enzsub.enzyme = ANY (ARRAY['P06239'])) OR "
            "(enzsub.enzyme_genesymbol = ANY (ARRAY['P06239']))) AND "
            "((enzsub.substrate = ANY (ARRAY['O14543'])) OR "
            "(enzsub.substrate_genesymbol = ANY (ARRAY['O14543']))) "
            "LIMIT 10",
        ),
    ],
}


def test_where_statement(legacy_service):

    for query_type, args in WHERE_CASES.items():

        stm = legacy_service.query(query_type, **args[0])

        assert str(stm).split('WHERE')[1] == args[1]
