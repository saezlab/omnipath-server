import pytest

__all__ = [
    'WHERE_CASES',
    'test_where_statement',
]


WHERE_CASES = {
    'enzsub': [
        (
            # basic
            {'enzymes': 'P06239', 'substrates': 'O14543', 'limit': 10},
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
            "((enzsub.enzyme = ANY (ARRAY[%(param_2)s])) OR "
            "(enzsub.enzyme_genesymbol = ANY (ARRAY[%(param_3)s])) OR "
            "(enzsub.substrate = ANY (ARRAY[%(param_4)s])) OR "
            "(enzsub.substrate_genesymbol = ANY (ARRAY[%(param_5)s]))) "
            "LIMIT %(param_6)s",
        ),
        (
            # multiple enzymes
            {'enzymes': ['P06241', 'P12931'], 'limit': 10},
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
            "((enzsub.enzyme = ANY (ARRAY[%(param_2)s, %(param_3)s])) OR "
            "(enzsub.enzyme_genesymbol = "
            "ANY (ARRAY[%(param_4)s, %(param_5)s]))) LIMIT %(param_6)s",
        ),
        (
            # enzyme AND substrate (instead of default OR)
            {
                'enzymes': 'P06241',
                'substrates': 'O14543',
                'enzyme_substrate': 'AND',
                'limit': 10,
            },
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
            "((enzsub.enzyme = ANY (ARRAY[%(param_2)s])) OR "
            "(enzsub.enzyme_genesymbol = ANY (ARRAY[%(param_3)s]))) "
            "AND ((enzsub.substrate = ANY (ARRAY[%(param_4)s])) OR "
            "(enzsub.substrate_genesymbol = ANY (ARRAY[%(param_5)s]))) "
            "LIMIT %(param_6)s",
        ),
        (
            {'organisms': 10090},
            "enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])",
        ),
        (
            # modification types
            {'types': 'phosphorylation'},
            "(enzsub.modification = ANY (ARRAY[%(param_1)s])) AND "
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_2)s]))",
        ),
        (
            # multiple modification types
            {'types': ['phosphorylation', 'acetylation']},
            "(enzsub.modification = ANY (ARRAY[%(param_1)s, %(param_2)s])) AND "
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_3)s]))",
        ),
    ],
}


@pytest.mark.parametrize(
    'query_type, args, expected',
    ((q, a, e) for q, p in WHERE_CASES.items() for a, e in p),
)
def test_statements_where(legacy_service, query_type, args, expected):

    stm = legacy_service.query_str(query_type, **args)

    assert stm.split('WHERE')[1].strip() == expected
