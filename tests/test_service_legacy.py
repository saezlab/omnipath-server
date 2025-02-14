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
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
            "((enzsub.enzyme = ANY (ARRAY[%(param_2)s])) OR "
            "(enzsub.enzyme_genesymbol = ANY (ARRAY[%(param_3)s]))) AND "
            "((enzsub.substrate = ANY (ARRAY[%(param_4)s])) OR "
            "(enzsub.substrate_genesymbol = ANY (ARRAY[%(param_5)s]))) "
            "LIMIT %(param_6)s",
        ),
    ],
}


def test_where_statement(legacy_service):

    for query_type, param in WHERE_CASES.items():

        for args, expected in param:

            stm = legacy_service.query_str(query_type, **args)

            assert stm.split('WHERE')[1].strip() == expected
