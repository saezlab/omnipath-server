import pytest

__all__ = [
    'SELECT_CASES',
    'WHERE_CASES',
    'test_statements_select',
    'test_statements_where',
]


WHERE_CASES = {
    'annotations': [
        (
            {'proteins': 'FST', 'limit': 10},
            "(annotations.uniprot = ANY (ARRAY[%(param_1)s])) OR "
            "(annotations.genesymbol = ANY (ARRAY[%(param_1)s])) "
            "LIMIT %(param_2)s",
        ),
        (
            {'proteins': ['FST', 'TGFB1']},
            "(annotations.uniprot = ANY (ARRAY[%(param_1)s, %(param_2)s])) OR "
            "(annotations.genesymbol = ANY (ARRAY[%(param_1)s, %(param_2)s]))",
        ),
        (
            {'resources': ['UniProt_tissue', 'KEGG']},
            "annotations.source = ANY (ARRAY[%(param_1)s, %(param_2)s])",
        ),
        (
            {'entity_types': 'complex', 'limit': 10},
            "annotations.entity_type = ANY (ARRAY[%(param_1)s]) "
            "LIMIT %(param_2)s",
        ),
        (
            {'entity_types': 'potato', 'limit': 10},
            "None",
        ),
    ],
    'enzsub': [
        (
            # basic
            {'enzymes': 'P06239', 'substrates': 'O14543', 'limit': 10},
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
            "((enzsub.enzyme = ANY (ARRAY[%(param_2)s])) OR "
            "(enzsub.enzyme_genesymbol = ANY (ARRAY[%(param_3)s])) OR "
            "(enzsub.substrate = ANY (ARRAY[%(param_4)s])) OR "
            "(enzsub.substrate_genesymbol = ANY (ARRAY[%(param_5)s]))) "
            "AND (enzsub.enzyme != enzsub.substrate) LIMIT %(param_6)s",
        ),
        (
            # multiple enzymes
            {'enzymes': ['P06241', 'P12931'], 'limit': 10},
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) AND "
            "((enzsub.enzyme = ANY (ARRAY[%(param_2)s, %(param_3)s])) OR "
            "(enzsub.enzyme_genesymbol = "
            "ANY (ARRAY[%(param_4)s, %(param_5)s]))) AND "
            "(enzsub.enzyme != enzsub.substrate) LIMIT %(param_6)s",
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
            "AND (enzsub.enzyme != enzsub.substrate) LIMIT %(param_6)s",
        ),
        (
            {'organisms': 10090},
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])) "
            "AND (enzsub.enzyme != enzsub.substrate)",
        ),
        (
            # modification types
            {'types': 'phosphorylation'},
            "(enzsub.modification = ANY (ARRAY[%(param_1)s])) AND "
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_2)s])) AND "
            "(enzsub.enzyme != enzsub.substrate)",
        ),
        (
            # multiple modification types
            {'types': ['phosphorylation', 'acetylation']},
            "(enzsub.modification = ANY (ARRAY[%(param_1)s, %(param_2)s])) AND "
            "(enzsub.ncbi_tax_id = ANY (ARRAY[%(param_3)s])) AND "
            "(enzsub.enzyme != enzsub.substrate)",
        ),
        (
            # multiple residue types
            {'residues': ['Y', 'T']},
            "(enzsub.residue_type = ANY (ARRAY[%(param_1)s, %(param_2)s])) "
            "AND (enzsub.ncbi_tax_id = ANY (ARRAY[%(param_3)s])) AND "
            "(enzsub.enzyme != enzsub.substrate)",
        ),
        (
            {'loops': True},
            'enzsub.ncbi_tax_id = ANY (ARRAY[%(param_1)s])',
        ),
    ],
    'intercell': [
            (
                {'resources': 'CellPhoneDB', 'limit': 10},
                "intercell.database = ANY (ARRAY[%(param_1)s]) LIMIT "
                "%(param_2)s",
            ),
            (
                {'resources': ['CellPhoneDB', 'UniProt_location']},
                "intercell.database = ANY (ARRAY[%(param_1)s, %(param_2)s])",
            ),
            (
                {'proteins': 'EGFR'},
                "(intercell.uniprot = ANY (ARRAY[%(param_1)s])) OR "
                "(intercell.genesymbol = ANY (ARRAY[%(param_1)s]))",
            ),
            (
                {'proteins': ['EGFR', 'TGFB1']},
                "(intercell.uniprot = ANY (ARRAY[%(param_1)s, %(param_2)s])) "
                "OR (intercell.genesymbol = ANY (ARRAY[%(param_1)s, "
                "%(param_2)s]))",
            ),
            (
                {'entity_types': 'protein'},
                "intercell.entity_type = ANY (ARRAY[%(param_1)s])",
            ),
            (
                {'aspect': 'functional'},
                "intercell.aspect = %(aspect_1)s",
            ),
            (
                {'scope': 'generic'},
                "intercell.scope = %(scope_1)s",
            ),
            (
                {'source': 'composite'},
                "intercell.source = %(source_1)s",
            ),
            (
                {'categories': 'receptor'},
                "intercell.category = %(category_1)s",
            ),
            (
                {'categories': ['receptor', 'ligand']},
                "intercell.category = ANY (ARRAY[%(param_1)s, %(param_2)s])",
            ),
            (
                {'parent': 'receptor'},
                "intercell.parent = %(parent_1)s",
            ),
            (
                {'transmitter': True},
                "intercell.transmitter IS %(transmitter_1)s",
            ),
            (
                {'transmitter': 0},
                "NOT intercell.transmitter",
            ),
            (
                {'receiver': 'false'},
                "NOT intercell.receiver",
            ),
            (
                {'receiver': 'false', 'transmitter': 'true'},
                "(intercell.transmitter IS %(transmitter_1)s) AND "
                "NOT intercell.receiver",
            ),
            (
                {'secreted': 'false'},
                "NOT intercell.secreted",
            ),
            (
                {'plasma_membrane_transmembrane': 1},
                "intercell.plasma_membrane_transmembrane IS "
                "%(plasma_membrane_transmembrane_1)s",
            ),
            (
                {'pmtm': 1},
                "intercell.plasma_membrane_transmembrane IS "
                "%(plasma_membrane_transmembrane_1)s",
            ),
            (
                {'pmp': 1},
                "intercell.plasma_membrane_peripheral IS "
                "%(plasma_membrane_peripheral_1)s",
            ),
    ],
}


SELECT_CASES = {
    'enzsub': [
        (
            {},
            "SELECT enzsub.enzyme AS enzsub_enzyme, enzsub.substrate AS "
            "enzsub_substrate, enzsub.residue_type AS enzsub_residue_type, "
            "enzsub.residue_offset AS enzsub_residue_offset, "
            "enzsub.modification AS enzsub_modification FROM enzsub",
        ),
    ],
}


@pytest.mark.parametrize(
    'query_type, args, expected',
    ((q, a, e) for q, p in WHERE_CASES.items() for a, e in p),
    ids = lambda p: '#' if isinstance(p, str) and len(p) > 19 else None,
)
def test_statements_where(legacy_service, query_type, args, expected):

    stm = legacy_service._query_str(query_type, **args)

    assert stm.split('WHERE')[-1].strip() == expected


@pytest.mark.parametrize(
    'query_type, args, expected',
    ((q, a, e) for q, p in SELECT_CASES.items() for a, e in p),
    ids = lambda p: '#' if isinstance(p, str) and len(p) > 19 else None,
)
def test_statements_select(legacy_service, query_type, args, expected):

    stm = legacy_service._query_str(query_type, **args)

    assert stm.split('WHERE')[0].strip() == expected
