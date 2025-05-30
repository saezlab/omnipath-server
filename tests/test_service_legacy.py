import pytest

__all__ = [
    'SELECT_CASES',
    'WHERE_CASES',
    'WHERE_CASES2',
    'test_statements_select',
    'test_statements_where',
    'test_statements_where2',
]

WHERE_CASES2 = { # XXX: Attempting systematic testing of all arguments
    'interactions': [
        (
            # XXX: Adds "AND true" clause in the query for some reason
            {'resources': ['SIGNOR']},
            'interactions.sources && %(sources_1)s::VARCHAR[]',
        ),
        (
            {'partners': ['EGFR']},
            'interactions.source = ANY (ARRAY[%(param_2)s])) OR '
            '(interactions.source_genesymbol = ANY (ARRAY[%(param_3)s])) OR '
            '(interactions.target = ANY (ARRAY[%(param_4)s])) OR '
            '(interactions.target_genesymbol = ANY (ARRAY[%(param_5)s]',
        ),
        (
            {'sources': ['EGFR']},
            'interactions.source = ANY (ARRAY[%(param_2)s])) OR '
            '(interactions.source_genesymbol = ANY (ARRAY[%(param_3)s]',
        ),
        (
            {'targets': ['EGFR']},
            'interactions.target = ANY (ARRAY[%(param_2)s])) OR '
            '(interactions.target_genesymbol = ANY (ARRAY[%(param_3)s]',
        ),
        (
            {'datasets': ['collectri', 'omnipath']},
            'interactions.collectri OR interactions.omnipath',
        ),
        (
            {'dorothea_levels': ['A', 'B', 'C']},
            'interactions.dorothea_level && %(dorothea_level_1)s::VARCHAR[]',
        ),
        (
            {
                'dorothea_methods': ['dorothea_curated', 'dorothea_tfbs'],
           },
           'interactions.dorothea_curated OR interactions.dorothea_tfbs',
        ),
        (
            {'types': 'post_translational'},
            'interactions.type = ANY (ARRAY[%(param_2)s]',
        ),
        (
            {'signed': True},
            'interactions.is_inhibition OR interactions.is_stimulation',
        ),
        (# XXX: Test the reverse, when it something shouldn't be in the query
            {'loops': False},
            'interactions.source != interactions.target',
        ),
        (
            {'entity_types': ['protein']},
            'interactions.entity_type_source = ANY (ARRAY[%(param_2)s])) OR '
            '(interactions.entity_type_target = ANY (ARRAY[%(param_2)s]'
        ),
    ],
}



WHERE_CASES = {
    'interactions': [
        (
            {'datasets': ["collectri", "omnipath"], 'limit': 10},
            "((interactions.ncbi_tax_id_source = ANY (ARRAY[%(param_1)s])) "
            "OR (interactions.ncbi_tax_id_target = ANY (ARRAY[%(param_1)s]))) "
            "AND (interactions.is_directed IS %(is_directed_1)s) AND "
            "(interactions.collectri OR interactions.omnipath) AND "
            "(interactions.source != interactions.target) LIMIT %(param_2)s",
        ),
        (
            {'directed': True},
            "((interactions.ncbi_tax_id_source = ANY (ARRAY[%(param_1)s])) OR "
            "(interactions.ncbi_tax_id_target = ANY (ARRAY[%(param_1)s]))) AND "
            "(interactions.is_directed IS %(is_directed_1)s) AND "
            "interactions.omnipath AND "
            "(interactions.source != interactions.target)",
        ),
#        (
#            {
#                'datasets': ['collectri', 'dorothea'],
#                'dorothea_methods': ['dorothea_curated', 'dorothea_tfbs'],
#                'limit': 10
#            },
#            "((interactions.ncbi_tax_id_source = ANY (ARRAY[%(param_1)s])) "
#            "OR (interactions.ncbi_tax_id_target = ANY (ARRAY[%(param_1)s]))) "
#            "AND (interactions.is_directed IS %(is_directed_1)s) AND "
#            "((interactions.dorothea_level && %(dorothea_level_1)s::VARCHAR[]) "
#            "AND interactions.collectri "
#            "OR (interactions.dorothea_curated OR interactions.dorothea_tfbs)) AND "
#            "(interactions.source != interactions.target) LIMIT %(param_2)s",
#        ),
    ],
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
            "enzsub.modification AS enzsub_modification, enzsub.sources AS "
            "enzsub_sources, enzsub.\"references\" AS enzsub_references "
            # XXX: Not sure where the double quotes come from
            "FROM enzsub",
        ),
    ],
    'interactions': [
        (
            {},
            "SELECT interactions.source AS interactions_source, "
            "interactions.target AS interactions_target, "
            "interactions.is_directed AS interactions_is_directed, "
            "interactions.is_stimulation AS interactions_is_stimulation, "
            "interactions.is_inhibition AS interactions_is_inhibition, "
            "interactions.consensus_direction AS "
            "interactions_consensus_direction, "
            "interactions.consensus_stimulation AS "
            "interactions_consensus_stimulation, "
            "interactions.consensus_inhibition AS "
            "interactions_consensus_inhibition, interactions.sources AS "
            "interactions_sources, interactions.\"references\" AS "
            "interactions_references, interactions.type AS interactions_type "
            "FROM interactions",
        ),
    ],
    'intercell': [
        (
            {},
            "SELECT intercell.category AS intercell_category, "
            "intercell.parent AS intercell_parent, intercell.database "
            "AS intercell_database, intercell.scope AS intercell_scope, "
            "intercell.aspect AS intercell_aspect, intercell.source "
            "AS intercell_source, intercell.uniprot AS intercell_uniprot, "
            "intercell.genesymbol AS intercell_genesymbol, "
            "intercell.entity_type AS intercell_entity_type, "
            "intercell.consensus_score AS intercell_consensus_score, "
            "intercell.transmitter AS intercell_transmitter, "
            "intercell.receiver AS intercell_receiver, intercell.secreted AS "
            "intercell_secreted, intercell.plasma_membrane_transmembrane AS "
            "intercell_plasma_membrane_transmembrane, "
            "intercell.plasma_membrane_peripheral AS "
            "intercell_plasma_membrane_peripheral FROM intercell",
        ),
    ],
    'complexes': [
        (
            {},
            "SELECT complexes.name AS complexes_name, complexes.components AS "
            "complexes_components, complexes.components_genesymbols AS "
            "complexes_components_genesymbols, complexes.stoichiometry AS "
            "complexes_stoichiometry, complexes.sources AS complexes_sources, "
            "complexes.\"references\" AS complexes_references, "
            "complexes.identifiers AS complexes_identifiers FROM complexes",
        ),
    ],
    'annotations': [
        (
            {},
            "SELECT annotations.uniprot AS annotations_uniprot, "
            "annotations.genesymbol AS annotations_genesymbol, "
            "annotations.entity_type AS annotations_entity_type, "
            "annotations.source AS annotations_source, annotations.label "
            "AS annotations_label, annotations.value AS annotations_value, "
            "annotations.record_id AS annotations_record_id FROM annotations",
        ),
    ],
}


@pytest.mark.parametrize(
    'query_type, args, expected, test_neg',
    (((q, ) + args + (False, ))[:4] for q, p in WHERE_CASES2.items() for args in p),
    ids = lambda p: '#' if isinstance(p, str) and len(p) > 19 else None,
)
def test_statements_where2(
    legacy_service,
    query_type,
    args,
    expected,
    test_neg
):

    stm = legacy_service._query_str(query_type, **args)
    where = stm.split('WHERE')[-1].strip()

    where_args = [s.strip('()') for s in where.split(' AND ')]

    assert expected in where_args


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
