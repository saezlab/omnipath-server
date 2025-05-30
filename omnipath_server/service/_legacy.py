#!/usr/bin/env python

#
# This file is part of the `omnipath_server` Python module
#
# Copyright 2024-2025
# Heidelberg University Hospital
#
# File author(s): OmniPath Team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

from typing import Any, Literal
from collections.abc import Callable, Iterable, Generator, Collection
import re
import json
import functools
import importlib as imp
import itertools
import collections

from sqlalchemy import or_, and_, any_, not_, text
from pypath_common import _misc
from pypath_common import _constants as _const
from sqlalchemy.orm import Query
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.dialects.postgresql import array

from omnipath_server import session
from .. import _log, _connection
from .._misc import SetEncoder
from ..schema import _legacy as _schema

__all__ = [
    'DEFAULT_LICENSE',
    'DOROTHEA_LEVELS',
    'DOROTHEA_METHODS',
    'ENTITY_TYPES',
    'FORMATS',
    'GEN_OF_STR',
    'GEN_OF_TUPLES',
    'INTERACTION_DATASETS',
    'INTERACTION_TYPES',
    'LICENSE_IGNORE',
    'LICENSE_INVALID',
    'LICENSE_LEVELS',
    'LICENSE_RANKS',
    'LegacyService',
    'NO_LICENSE',
    'ORGANISMS',
    'QUERY_TYPES',
    'with_last',
]


LICENSE_IGNORE = 'ignore'
LICENSE_INVALID = {'composite', 'ignore'}
DEFAULT_LICENSE = 'academic'
NO_LICENSE = {
    'name': 'No license',
    'full_name': 'No license',
    'purpose': 'ignore',
}
FORMATS = Literal[
    'raw',
    'json',
    'tab',
    'text',
    'tsv',
    'table',
    'query',
]
ORGANISMS = Literal[
    9606,
    10090,
    10116,
]
QUERY_TYPES = Literal[
    'complexes',
    'enzsub',
    'interactions',
    'intercell',
    'annotations',
]
ENTITY_TYPES = Literal[
    'complex',
    'mirna',
    'protein',
    'small_molecule',
    'lncrna',
    'drug',
    'metabolite',
    'lipid',
]
INTERACTION_TYPES = Literal[
    'post_translational',
    'transcriptional',
    'post_transcriptional',
    'mirna_transcriptional',
    'lncrna_post_transcriptional',
    'small_molecule_protein',
]
INTERACTION_DATASETS = Literal[
    'omnipath',
    'kinaseextra',
    'ligrecextra',
    'pathwayextra',
    'mirnatarget',
    'dorothea',
    'collectri',
    'tf_target',
    'lncrna_mrna',
    'tf_mirna',
    'small_molecule',
]
DOROTHEA_LEVELS = Literal['A', 'B', 'C', 'D']
DOROTHEA_METHODS = Literal[
    'curated',
    'coexp',
    'tfbs',
    'chipseq',
]
LICENSE_LEVELS = Literal[
    'ignore',
    'academic',
    'non_profit',
    'nonprofit',
    'for_profit',
    'forprofit',
    'commercial',
]
LICENSE_RANKS = {
    'ignore': 30,
    'composite': 30,
    'academic': 20,
    'non_profit': 20,
    'nonprofit': 20,
    'for_profit': 10,
    'forprofit': 10,
    'commercial': 10,
}
GEN_OF_TUPLES = Generator[tuple, None, None]
GEN_OF_STR = Generator[str, None, None]

# TODO: replace with `resources` SQL table
# to avoid having pypath-omnipath as dependency
resources_mod = None


class LegacyService:

    query_param = {
        'interactions': {
            'array_args': {
                'sources',
                'targets',
                'partners',
                'resources',
                'types',
                'organisms',
                'datasets',
                'dorothea_levels',
                'dorothea_methods',
                'entity_types',
                'fields',
            },
            'select': {
                'genesymbols': {'source_genesymbol', 'target_genesymbol'},
                'organism': {'ncbi_tax_id_source', 'ncbi_tax_id_target'},
                'entity_type': {'entity_type_source', 'entity_type_target'},
                'extra_attrs': 'extra_attrs',
                'evidences': 'evidences',
                'datasets': INTERACTION_DATASETS.__args__,
                'dorothea_methods': {
                    f'dorothea_{m}' for m in DOROTHEA_METHODS.__args__
                },
            },
            'select_args': {
                'genesymbols',
                'extra_attrs',
                'evidences',
            },
            'select_default': {
                'source',
                'target',
                'is_directed',
                'is_stimulation',
                'is_inhibition',
                'consensus_direction',
                'consensus_stimulation',
                'consensus_inhibition',
                'type',
            },
            'where': {
                'resources': 'sources',
                'types': 'type',
                'directed': 'is_directed',
                'organisms': 'ncbi_tax_id_source:ncbi_tax_id_target',
                'entity_types': 'entity_type_source:entity_type_target',
            },
            'where_partners': {
                'sides': {
                    'sources': 'source',
                    'targets': 'target',
                },
                'operator': 'source_target',
            },
            'where_bool': {
                'dorothea_methods': {
                    'curated': 'dorothea_curated',
                    'chipseq': 'dorothea_chipseq',
                    'tfbs': 'dorothea_tfbs',
                    'coexp': 'dorothea_coexp',
                },
                'datasets': {
                    'omnipath',
                    'kinaseextra',
                    'ligrecextra',
                    'pathwayextra',
                    'mirnatarget',
                    'dorothea',
                    'collectri',
                    'tf_target',
                    'lncrna_mrna',
                    'tf_mirna',
                    'small_molecule',
                },
                'signed': {
                    'is_stimulation',
                    'is_inhibition',
                },
            },
            'where_bool_override': {
                'dorothea': {
                    'dorothea_levels': 'dorothea_level',
                    'dorothea_methods': DOROTHEA_METHODS.__args__,
                },
            },
        },
        'complexes': {
            'array_args': {
                'resources',
                'proteins',
            },
            'where': {
                'resources': 'sources',
                'proteins': 'components',
            },
        },
        'enzsub': {
            'array_args': {
                'enzymes',
                'substrates',
                'partners',
                'resources',
                'organisms',
                'types',
                'residues',
            },
            'select': {
                'genesymbols': {'enzyme_genesymbol', 'substrate_genesymbol'},
            },
            'select_args': {
                'genesymbols',
            },
            'select_default': {
                'enzyme',
                'substrate',
                'residue_type',
                'residue_offset',
                'modification',
            },
            'where': {
                'organisms': 'ncbi_tax_id',
                'types': 'modification',
                'resources': 'sources',
                'residues': 'residue_type',
            },
            'where_partners': {
                'sides': {
                    'enzymes': 'enzyme',
                    'substrates': 'substrate',
                },
                'operator': 'enzyme_substrate',
            },
        },
        'intercell': {
            'array_args': {
                'proteins',
                'resources',
                'entity_types',
            },
            'where': {
                'resources': 'database',
                'entity_types': 'entity_type',
                'proteins': 'uniprot:genesymbol',
                'aspect': 'aspect',
                'scope': 'scope',
                'source': 'source',
                'categories': 'category',
                'parent': 'parent',
                'transmitter': 'transmitter',
                'receiver': 'receiver',
                'secreted': 'secreted',
                'plasma_membrane_transmembrane': 'plasma_membrane_transmembrane',
                'plasma_membrane_peripheral': 'plasma_membrane_peripheral',
            },
            'where_synonyms': {
                'trans': 'transmitter',
                'rec': 'receiver',
                'sec': 'secreted',
                'pmtm': 'plasma_membrane_transmembrane',
                'pmp': 'plasma_membrane_peripheral',
            },
        },
        'annotations': {
            'array_args': {
                'proteins',
                'resources',
                'entity_types',
            },
            'where': {
                'resources': 'source',
                'entity_types': 'entity_type',
                'proteins': 'uniprot:genesymbol',
            },
        },
    }
    query_types = {
        'annotations',
        'intercell',
        'interactions',
        'enz_sub',
        'enzsub',
        'ptms',
        'complexes',
        'about',
        'info',
        'queries',
        'annotations_summary',
        'intercell_summary',
    }
    data_query_types = {
        'annotations',
        'intercell',
        'interactions',
        'enzsub',
        'complexes',
    }
    list_fields = {
        'sources',
        'references',
        'isoforms',
    }

    int_list_fields = {
        'references',
        'isoforms',
    }

    field_synonyms = {
        'organism': 'ncbi_tax_id',
        'sources': 'resources',
        'databases': 'resources',
    }

    args_reference = {
        'interactions': {
            'header': None,
            'format': FORMATS.__args__,
            'license': {
                'ignore',
                'academic',
                'non_profit',
                'nonprofit',
                'for_profit',
                'forprofit',
                'commercial',
            },
            'password': None,
            'limit': None,
            'datasets': {
                'omnipath',
                'dorothea',
                'collectri',
                'tf_target',
                'tf_mirna',
                'lncrna_mrna',
                'kinaseextra',
                'ligrecextra',
                'pathwayextra',
                'mirnatarget',
                'small_molecule',
            },
            'types': {
                'post_translational',
                'transcriptional',
                'post_transcriptional',
                'mirna_transcriptional',
                'lncrna_post_transcriptional',
                'small_molecule_protein',
            },
            'sources':  None,
            'resources': None,
            'databases': None,
            'targets':  None,
            'partners': None,
            'genesymbols': _const.BOOLEAN_VALUES,
            'evidences': None,
            'extra_attrs': None,
            'fields': {
                'entity_type',
                'references',
                'sources',
                'dorothea_level',
                'dorothea_methods',
                'type',
                'ncbi_tax_id',
                'databases',
                'resources',
                'organism',
                'curation_effort',
                'datasets',
                'extra_attrs',
                'evidences',
            },
            'dorothea_levels':  {'A', 'B', 'C', 'D', 'E'},
            'dorothea_methods': {
                'curated',
                'chipseq',
                'coexp',
                'tfbs',
                'dorothea_curated',
                'dorothea_chipseq',
                'dorothea_coexp',
                'dorothea_tfbs',
            },
            'organisms': ORGANISMS.__args__,
            'source_target': {
                'AND',
                'OR',
                'and',
                'or',
            },
            'directed': _const.BOOLEAN_VALUES,
            'signed': _const.BOOLEAN_VALUES,
            'loops': _const.BOOLEAN_VALUES,
            'entity_types': ENTITY_TYPES.__args__,
        },
        'enzsub': {
            'header': None,
            'format': FORMATS.__args__,
            'license': {
                'ignore',
                'academic',
                'non_profit',
                'nonprofit',
                'for_profit',
                'forprofit',
                'commercial',
            },
            'password': None,
            'limit': None,
            'enzymes':     None,
            'substrates':  None,
            'partners':    None,
            'genesymbols': _const.BOOLEAN_VALUES,
            'organisms': ORGANISMS.__args__,
            'databases': None,
            'resources': None,
            'residues':  None,
            'modification': None,
            'types': None,
            'loops': _const.BOOLEAN_VALUES,
            'fields': {
                'sources',
                'references',
                'ncbi_tax_id',
                'organism',
                'databases',
                'resources',
                'isoforms',
                'curation_effort',
            },
            'enzyme_substrate': {
                'AND',
                'OR',
                'and',
                'or',
            },
        },
        'annotations': {
            'header': None,
            'format': FORMATS.__args__,
            'license': {
                'ignore',
                'academic',
                'non_profit',
                'nonprofit',
                'for_profit',
                'forprofit',
                'commercial',
            },
            'password': None,
            'limit': None,
            'databases': None,
            'resources': None,
            'proteins': None,
            'fields': None,
            'genesymbols': _const.BOOLEAN_VALUES,
            'entity_types': ENTITY_TYPES.__args__,
        },
        'annotations_summary': {
            'header': None,
            'format': FORMATS.__args__,
            'databases': None,
            'resources': None,
            'fields': None,
            'cytoscape': _const.BOOLEAN_VALUES,
        },
        'intercell': {
            'header': None,
            'format': FORMATS.__args__,
            'license': {
                'ignore',
                'academic',
                'non_profit',
                'nonprofit',
                'for_profit',
                'forprofit',
                'commercial',
            },
            'password': None,
            'limit': None,
            'scope': {
                'specific',
                'generic',
            },
            'aspect': {
                'functional',
                'locational',
            },
            'source': {
                'resource_specific',
                'composite',
            },
            'categories': None,
            'databases': None,
            'resources': None,
            'parent': None,
            'proteins': None,
            'fields': None,
            'entity_types': ENTITY_TYPES.__args__,
            'transmitter': _const.BOOLEAN_VALUES,
            'receiver': _const.BOOLEAN_VALUES,
            'trans': _const.BOOLEAN_VALUES,
            'rec': _const.BOOLEAN_VALUES,
            'secreted': _const.BOOLEAN_VALUES,
            'plasma_membrane_peripheral': _const.BOOLEAN_VALUES,
            'plasma_membrane_transmembrane': _const.BOOLEAN_VALUES,
            'sec': _const.BOOLEAN_VALUES,
            'pmp': _const.BOOLEAN_VALUES,
            'pmtm': _const.BOOLEAN_VALUES,
            'causality': {
                'transmitter',
                'trans',
                'receiver',
                'rec',
                'both',
            },
            'topology': {
                'secreted',
                'sec',
                'plasma_membrane_peripheral',
                'pmp',
                'plasma_membrane_transmembrane',
                'pmtm',
            },
        },
        'intercell_summary': {
            'header': None,
            'format': FORMATS.__args__,
            'scope': {
                'specific',
                'generic',
            },
            'aspect': {
                'functional',
                'locational',
            },
            'source': {
                'resource_specific',
                'generic',
            },
            'categories': None,
            'resources': None,
            'databases': None,
            'parent': None,
            'fields': None,
            'transmitter': _const.BOOLEAN_VALUES,
            'receiver': _const.BOOLEAN_VALUES,
            'trans': _const.BOOLEAN_VALUES,
            'rec': _const.BOOLEAN_VALUES,
            'secreted': _const.BOOLEAN_VALUES,
            'plasma_membrane_peripheral': _const.BOOLEAN_VALUES,
            'plasma_membrane_transmembrane': _const.BOOLEAN_VALUES,
            'sec': _const.BOOLEAN_VALUES,
            'pmp': _const.BOOLEAN_VALUES,
            'pmtm': _const.BOOLEAN_VALUES,
        },
        'complexes': {
            'header': None,
            'format': FORMATS.__args__,
            'license': {
                'ignore',
                'academic',
                'non_profit',
                'nonprofit',
                'for_profit',
                'forprofit',
                'commercial',
            },
            'password': None,
            'limit': None,
            'databases': None,
            'resources': None,
            'proteins': None,
            'fields': None,
        },
        'resources': {
            'license': {
                'ignore',
                'academic',
                'non_profit',
                'nonprofit',
                'for_profit',
                'forprofit',
                'commercial',
            },
            'format': {
                'json',
            },
            'datasets': {
                'interactions',
                'interaction',
                'network',
                'enzsub',
                'enz_sub',
                'enzyme-substrate',
                'annotations',
                'annotation',
                'annot',
                'intercell',
                'complex',
                'complexes',
            },
            'subtypes': None,
        },
        'queries': {
            'format': FORMATS.__args__,
        },
    }
    query_type_synonyms = {
        'interactions': 'interactions',
        'interaction': 'interactions',
        'network': 'interactions',
        'enz_sub': 'enzsub',
        'enz-sub': 'enzsub',
        'ptms': 'enzsub',
        'ptm': 'enzsub',
        'enzyme-substrate': 'enzsub',
        'enzyme_substrate': 'enzsub',
        'annotations': 'annotations',
        'annotation': 'annotations',
        'annot': 'annotations',
        'intercell': 'intercell',
        'intercellular': 'intercell',
        'inter_cell': 'intercell',
        'inter-cell': 'intercell',
        'complex': 'complexes',
        'complexes': 'complexes',
    }
    datasets_ = {
        'omnipath',
        'dorothea',
        'collectri',
        'tf_target',
        'kinaseextra',
        'ligrecextra',
        'pathwayextra',
        'mirnatarget',
        'tf_mirna',
        'lncrna_mrna',
        'small_molecule',
    }
    dorothea_methods = {'curated', 'coexp', 'chipseq', 'tfbs'}
    dataset2type = {
        'omnipath': 'post_translational',
        'dorothea': 'transcriptional',
        'collectri': 'transcriptional',
        'tf_target': 'transcriptional',
        'kinaseextra': 'post_translational',
        'ligrecextra': 'post_translational',
        'pathwayextra': 'post_translational',
        'mirnatarget': 'post_transcriptional',
        'tf_mirna': 'mirna_transcriptional',
        'lncrna_mrna': 'lncrna_post_transcriptional',
        'small_molecule': 'small_molecule_protein',
    }
    interaction_fields = {
        'references', 'sources', 'dorothea_level',
        'dorothea_curated', 'dorothea_chipseq',
        'dorothea_tfbs', 'dorothea_coexp',
        'type', 'ncbi_tax_id', 'databases', 'organism',
        'curation_effort', 'resources', 'entity_type',
        'datasets', 'extra_attrs', 'evidences',
    }
    enzsub_fields = {
        'references', 'sources', 'databases',
        'isoforms', 'organism', 'ncbi_tax_id',
        'curation_effort', 'resources',
    }
    default_input_files = {
        'interactions': 'omnipath_webservice_interactions.tsv',
        'enzsub': 'omnipath_webservice_enz_sub.tsv',
        'annotations': 'omnipath_webservice_annotations.tsv',
        'complexes': 'omnipath_webservice_complexes.tsv',
        'intercell': 'omnipath_webservice_intercell.tsv',
    }
    default_dtypes = collections.defaultdict(
        dict,
        interactions = {
            'source': 'category',
            'target': 'category',
            'source_genesymbol': 'category',
            'target_genesymbol': 'category',
            'is_directed': 'int8',
            'is_stimulation': 'int8',
            'is_inhibition': 'int8',
            'consensus_direction': 'int8',
            'consensus_stimulation': 'int8',
            'consensus_inhibition': 'int8',
            'sources': 'category',
            'references': 'category',
            'dorothea_curated': 'category',
            'dorothea_chipseq': 'category',
            'dorothea_tfbs': 'category',
            'dorothea_coexp': 'category',
            'dorothea_level': 'category',
            'type': 'category',
            'ncbi_tax_id_source': 'int16',
            'ncbi_tax_id_target': 'int16',
            'entity_type_source': 'category',
            'entity_type_target': 'category',
            'curation_effort': 'int16',
            'extra_attrs': 'category',
            'evidences': 'category',
        },
        annotations = {
            'uniprot': 'category',
            'genesymbol': 'category',
            'entity_type': 'category',
            'source': 'category',
            'label': 'category',
            'value': 'category',
            'record_id': 'uint32',
        },
        enzsub = {
            'enzyme': 'category',
            'substrate': 'category',
            'enzyme_genesymbol': 'category',
            'substrate_genesymbol': 'category',
            'isoforms': 'category',
            'residue_type': 'category',
            'residue_offset': 'uint16',
            'modification': 'category',
            'sources': 'category',
            'references': 'category',
            'ncbi_tax_id': 'int16',
            'curation_effort': 'int32',
        },
        complexes = {
            'name': 'category',
            'stoichiometry': 'category',
            'sources': 'category',
            'references': 'category',
            'identifiers': 'category',
        },
        intercell = {
            'category': 'category',
            'database': 'category',
            'uniprot': 'category',
            'genesymbol': 'category',
            'parent': 'category',
            'aspect': 'category',
            'scope': 'category',
            'source': 'category',
            'entity_type': 'category',
            'consensus_score': 'uint16',
            'transmitter': 'bool',
            'receiver': 'bool',
            'secreted': 'bool',
            'plasma_membrane_transmembrane': 'bool',
            'plasma_membrane_peripheral': 'bool',
        },
    )
    # the annotation attributes served for the cytoscape app
    cytoscape_attributes = {
        'Zhong2015': 'type',
        'MatrixDB': 'mainclass',
        'Matrisome': ('mainclass', 'subclass', 'subsubclass'),
        # 'TFcensus': 'in TFcensus',
        'Locate': ('location', 'cls'),
        'Phosphatome': (
            'family',
            'subfamily',
            #'has_protein_substrates',
        ),
        'CancerSEA': 'state',
        'GO_Intercell': 'mainclass',
        'Adhesome': 'mainclass',
        'SignaLink3': 'pathway',
        'HPA_secretome': (
            'mainclass',
            #'secreted',
        ),
        'OPM': (
            'membrane',
            'family',
            #'transmembrane',
        ),
        'KEGG': 'pathway',
        #'CellPhoneDB': (
            # 'receptor',
            # 'peripheral',
            # 'secreted',
            # 'transmembrane',
            # 'receptor_class',
            # 'secreted_class',
        #),
        'kinase.com': ('group', 'family', 'subfamily'),
        'Membranome': ('membrane',),
        #'CSPA': 'in CSPA',
        #'MSigDB': 'geneset',
        #'Integrins': 'in Integrins',
        'HGNC': 'mainclass',
        'CPAD': ('pathway', 'effect_on_cancer', 'cancer'),
        'Signor': 'pathway',
        'Ramilowski2015': 'mainclass',
        'HPA_subcellular': 'location',
        #'DisGeNet': 'disease',
        'Surfaceome': ('mainclass', 'subclasses'),
        'IntOGen': 'role',
        'HPMR': ('role', 'mainclass', 'subclass', 'subsubclass'),
        #'CancerGeneCensus': (
            ##'hallmark',
            ##'somatic',
            ##'germline',
            #'tumour_types_somatic',
            #'tumour_types_germline',
        #)
        #'DGIdb', 'category',
        'ComPPI': 'location',
        'Exocarta': 'vesicle',
        'Vesiclepedia': 'vesicle',
        'Ramilowski_location': 'location',
        'LRdb': ('role', 'cell_type'),
    }

    def __init__(self, con: _connection.Connection | dict | None = None):
        """
        Service for the old OmniPath web API.

        Args:
            con:
                Instance of `Connection` to the SQL database or a dictionary
                with the connection configuration parameters.
        """

        _log('Creating LegacyService.')

        self._connect(con)

        self._cached_data = {}

        self._preprocess()


    def _preprocess(self):

        self._update_resources()
        self._preprocess_annotations()
        self._preprocess_intercell()


    def _reload(self):
        """
        Reloads the object from the module level.
        """

        modname = self.__class__.__module__
        mod = __import__(modname, fromlist = [modname.split('.')[0]])
        imp.reload(mod)
        new = getattr(mod, self.__class__.__name__)
        setattr(self, '__class__', new)


    def _connect(self, con: _connection.Connection | dict | None = None):
        """
        Establishes the connection to the SQL database.

        Args:
            con:
                Instance of `Connection` to the SQL database or a dictionary
                with the connection configuration parameters.
        """

        con = con or {}

        if isinstance(con, dict):

            con = {
                    param:
                    session.config.get(
                        f'legacy_db_{param}',
                        override = con.get(param, None),
                        default = default,
                    )
                for param, default in _connection.DEFAULTS.items()
            }

        self.con = _connection.ensure_con(con)


    def _preprocess_annotations(self):
        """
        """

        _log('Preprocessing annotations.')

        query = "SELECT source, label, ARRAY_AGG(DISTINCT value) FROM " \
        "annotations GROUP BY source, label;"

        self._cached_data["annotations_summary"] = list(
            self.con.execute(text(query)),
        )


    def _preprocess_intercell(self):
        """
        """

        _log('Preprocessing intercell.')

        record = collections.namedtuple(
            'intercell_records', [
                'category',
                'parent',
                'database',
                'aspect',
                'source',
                'scope',
                'transmitter',
                'receiver',
            ],
        )

        query = (
            "SELECT DISTINCT ON (category, parent, database) "
            f"{', '.join(record._fields)} "
            "FROM intercell;"
        )

        self._cached_data["intercell_summary"] = [
            record(*x) for x in self.con.execute(text(query))
        ]


    def _resource_col(self, query_type: QUERY_TYPES) -> str:
        """
        Name of the column with resource names for each query_type.
        """

        cols = {c.name: c for c in self._columns(query_type)}

        # finding out what is the name of the column with the resources
        # as this is different across the tables
        for colname, argname in (
            ('database', 'databases'),
            ('sources', 'databases'),
            ('source', 'databases'),
            ('category', 'categories'),
        ):

            if colname in cols:

                return colname


    def _resource_prefix_cols(self, query_type: QUERY_TYPES) -> list[str]:
        """
        Columns with resource name prefixes.
        """

        _prefix_cols = {
            'interactions': 'references',
            'enzsub': 'references',
            'complexes': 'identifiers',
        }

        return _misc.to_list(_prefix_cols.get(query_type))


    def _update_resources(self):
        """
        Compiles list of all the different resources across all databases.
        """

        _log('Updating resource information.')

        self._resources_meta = collections.defaultdict(dict)

        _log('Loading license information.')

        license_query = "SELECT * FROM licenses;"
        license_cols = [c.name for c in self._columns('licenses')]
        licenses = {
            l[1]: dict(zip(license_cols[2:], l[2:]))
            for l in self.con.execute(text(license_query))
        }

        for query_type in self.data_query_types:

            datasets = {}
            categories = collections.defaultdict(set)
            cols = {c.name: c for c in self._columns(query_type)}
            colname = self._resource_col(query_type)

            unnest = (
                ('unnest(', ')')
                    if self._isarray(cols[colname]) else
                ('', '')
            )

            query = f'SELECT DISTINCT {colname.join(unnest)} FROM {query_type};'
            resources = {x[0] for x in self.con.execute(text(query))}

            if query_type == 'interactions':

                for dataset in self.datasets_:

                    if dataset not in cols.keys():

                        continue

                    query = (
                        f'SELECT DISTINCT {colname.join(unnest)} '
                        f'FROM {query_type} WHERE {dataset};'
                    )
                    datasets[dataset] = {
                        x[0]
                        for x in self.con.execute(text(query))
                    }

            elif query_type == 'intercell':

                query = (
                    f'SELECT category, database '
                    f"FROM {query_type} WHERE scope = 'generic' "
                    f'GROUP BY category, database;'
                )

                for category, database in self.con.execute(text(query)):

                    categories[database].add(category)

            for db in resources:

                if db not in licenses:

                    licenses[db] = NO_LICENSE.copy()

                if (
                    licenses[db]['purpose'] in LICENSE_INVALID and
                    '_' in db and
                    (component_db := db.split('_')[0]) in licenses
                ):

                    licenses[db] = licenses[component_db].copy()


                if licenses[db]['purpose'] == LICENSE_IGNORE:

                    msg = (
                        f'No license for resource `{db}`. '
                        'Data from this resource will be '
                        'served only with permission to ignore licensing.'
                    )
                    _log(msg)


                self._resources_meta[db]['license'] = licenses[db]

                qt_data = {}

                if datasets:

                    qt_data['datasets'] = {
                        k
                        for k, v in datasets.items()
                        if db in v
                    }

                if categories:

                    qt_data['categories'] = categories[db]

                if 'queries' not in self._resources_meta[db]:

                    self._resources_meta[db]['queries'] = {}
                    self._resources_meta[db]['datasets'] = set()

                self._resources_meta[db]['queries'][query_type] = qt_data
                self._resources_meta[db]['datasets'] |= (
                    qt_data.get('datasets', set())
                )

        composite_resources = {
            res
            for res, info in self._resources_meta.items()
            if info['license']['purpose'] == 'composite'
        }
        components = {
            cres: {
                res for res in self._resources_meta.keys()
                if res.endswith(cres)
            }
            for cres in composite_resources
        }

        for res, comp in components.items():

            self._resources_meta[res]['components'] = comp

        self._resources_meta = dict(self._resources_meta)

        _log('Finished updating resource information.')


    def _clean_args(self, args: dict) -> dict:
        """
        Removes empty arguments, `kwargs` and `self` to prepare them for
        generating the SQL query.

        Args:
            args:
                Dictionary of arguments of a query.

        Returns:
            The clean dictionary of arguments ready to generate a query.
        """

        args.pop('self', None)
        kwargs = args.pop('kwargs', {})
        args = {
            k: self._maybe_bool(v)
            for k, v in args.items()
            if v is not None
        }
        args['format'] = self._ensure_str(args.get('format'))

        return args


    def _maybe_bool(self, val: Any) -> Any:
        """
        Checks whether a variable is any alternative representation of a boolean
        value (e.g. `1`/`0`, `'true'`/`'false'`, etc...).

        Args:
            val:
                Variable to check.

        Returns:
            The original value if not boolean-encoded, otherwise
            `'true'`/`'false'` according to the value.
        """

        if (bval := str(val).lower()) in _const.BOOLEAN_VALUES:

            val = self._parse_bool_arg(bval)

        return val


    @staticmethod
    def _ensure_str(val: str | Iterable[str] | None = None) -> str | None:
        """
        Ensures a given value is a string.

        Args:
            val:
                A string or iterable of these.

        Returns:
            The value in `str` format.
        """

        return _misc.first(_misc.to_list(val))


    @staticmethod
    def _ensure_array(val: Any | Iterable[Any]) -> list[Any]:
        """
        Ensures a given value is (or is within) a list.

        Args:
            val:
                A value to be ensured as list.

        Returns:
            The value as (within) a list.
        """

        if isinstance(val, _const.LIST_LIKE) and len(val) == 1:

            val = _misc.first(val)

        elif isinstance(val, str):

            val = val.split(',')

        return _misc.to_list(val)


    def _array_args(self, args: dict, query_type: str) -> dict:
        """
        Ensures array arguments of a query are correctly set in the right
        format. These are defined for each of the different databases in the
        class variable `query_param[query_type]['array_args']`.

        Args:
            args:
                Collection of query arguments for which to ensure the arrays.
            query_type:
                Name of the target database of the query.

        Returns:
            The provided `args` dictionary where array variables are ensured to
            be in the right format.
        """

        array_args = self.query_param[query_type].get('array_args', set())

        args = {
            k: self._ensure_array(v) if k in array_args else v
            for k, v in args.items()
        }

        return args


    def _check_args(self, args: dict, query_type: str):
        """
        Checks the arguments of a given query and ensures consistency and data
        types as well as raise a warning if a wrong argument and/or value is
        passed.

        Args:
            args:
                Collection of query arguments to check.
            query_type:
                Name of the target database of the query.
        """

        result = []

        ref = (
            self.args_reference['resources']
                if query_type == 'databases' else
            self.args_reference[query_type]
        )

        for arg, val in args.items():

            if arg in ref:

                if not ref[arg] or not val:

                    continue

                val = val[0] if isinstance(val, list) else val
                val = str(val).lower() if isinstance(val, bool) else val
                val = _misc.to_set(val)

                unknowns = val - set(ref[arg])

                if unknowns:

                    result.append(
                        ' ==> Unknown values for argument `{}`: `{}`'.format(
                            arg,
                            ', '.join(str(u) for u in unknowns),
                        ),
                    )

            else:

                result.append(' ==> Unknown argument: `%s`' % arg)

        args['header'] = self._parse_bool_arg(args.get('header', True))

        if result:

            return (
                'Something is not entirely good:\n%s\n\n'
                'Please check the examples at\n'
                'https://github.com/saezlab/pypath\n'
                'and\n'
                'https://github.com/saezlab/DoRothEA\n'
                'If you still experiencing issues contact us at\n'
                'https://github.com/saezlab/pypath/issues'
                '' % '\n'.join(result)
            )


    def _query_type(self, query_type):

        return (
            self.query_type_synonyms[query_type]
                if (
                    hasattr(self, 'query_type_synonyms') and
                    query_type in self.query_type_synonyms
                ) else
            query_type
        )


    def queries(
            self,
            query_type: str | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ):
        """
        Gives back the argument values of the query for the given database.

        Args:
            TODO.
        """

        format = self._ensure_str(format)
        query = query_type or kwargs.pop('path', [])[1:]

        query = _misc.to_list(query)

        if len(query) < 1:

            raise ValueError('Query type not specified.')

        query_type = self._query_type(query[0])
        query_param = query[1] if len(query) > 1 else None

        if query_type in self.args_reference:

            result = {
                    k:
                    sorted(v) if isinstance(v, _const.LIST_LIKE) else v
                for k, v in self.args_reference[query_type].items()
            }

            resource_col = self._resource_col(query_type)
            resources = {
                res
                for res, res_info in self._resources_meta.items()
                if query_type in res_info['queries']
            }
            result[resource_col] = resources

            if query_param is not None and query_param in result:

                result = {query_param: result[query_param]}

        else:

            result = {}
            result[query_type] = (
                f'No possible arguments defined for'
                f'query {query_type} or no such query available.'
            )

        result = self._dict_set_to_list(result)

        fmt_value = lambda v: (
            ';'.join(str(x) for x in v)
                if isinstance(v, (list, set, tuple)) else
            str(v)
        )

        yield from self._output(
            (
                (k, fmt_value(v))
                for k, v in result.items()
            ),
            names = ['argument', 'values'],
            format = format,
            **kwargs,
        )


    @classmethod
    def _dict_set_to_list(cls, dct):

        return {
                key:
                (
                    sorted(val)
                        if isinstance(val, _const.LIST_LIKE) else
                    cls._dict_set_to_list(val)
                        if isinstance(val, dict) else
                    val
                )
            for key, val in dct.items()
        }


    def databases(self, req):

        query_type = (
            req.postpath[1]
                if len(req.postpath) > 1 else
            'interactions'
        )

        query_type = self._query_type(query_type)

        datasets = (
            set(req.postpath[2].split(','))
                if len(req.postpath) > 2 else
            None
        )

        tbl = (
            self.data[query_type]
                if query_type in self.data else
            self.data['interactions']
        )

        # filter for datasets
        if query_type == 'interactions':

            if datasets is not None:

                tbl = tbl.loc[tbl.type.isin(datasets)]

            else:

                datasets = self._get_datasets()

            result = {}

            for dataset in datasets:

                result[dataset] = sorted(
                    set.union(
                        *tbl[tbl.type == dataset].set_sources,
                    ),
                )

        else:

            result = {}
            result['*'] = sorted(set.union(*tbl.set_sources))

        if b'format' in req.args and req.args['format'][0] == b'json':

            return json.dumps(result)

        else:

            return 'dataset\tresources\n%s' % '\n'.join(
                '{}\t{}'.format(k, ';'.join(v)) for k, v in result.items()
            )

    @functools.cache
    def _get_datasets(self):

        query = 'SELECT DISTINCT type FROM interactions'

        return [x[0] for x in self.con.execute(text(query))]


    def datasets(
            self,
            path: list[str] | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ) -> Generator[tuple | str | dict, None, None]:

        path = _misc.to_list(path)
        query_type = path[1] if len(path) > 1 else 'interactions'

        result = self._get_datasets() if query_type == 'interactions' else []

        if format not in ('json', 'raw'):

            result = ((';'.join(f'{dataset}' for dataset in result),),)

        yield from self._output(result, names = None, format = format, **kwargs)


    def _schema(self, query_type: str) -> ReadOnlyColumnCollection:
        """
        Retrieves the schema class of the corresponding query type (e.g.
        `Interactions`, `Enzsub`, `Annotations`, etc.).

        Args:
            query_type:
                The desired database to retrieve its schema class.

        Returns:
            The `_schema` class of the requested database.
        """

        return getattr(_schema, query_type.capitalize())


    def _columns(self, query_type: str) -> list[str]:
        """
        Retrieve the list of columns of a given database as defined in its
        schema.

        Args:
            query_type:
                The desired database to retrieve its schema class.

        Returns:
            The list of columns of the requested database.
        """

        return self._schema(query_type).__table__.columns


    def _where_op(
            self,
            col: InstrumentedAttribute | Column,
            val: Any,
            op: str | None = None,
    ) -> str:
        """
        Infers the SQL operator for the `WHERE` clause based on column and value
        types.

        Args:
            col:
                Column in the database where the search is performed.
            val:
                The value to be searched.
            op:
                Pre-defined operator (if any).

        Returns:
            Pair of operator/value according to the column and data type. If any
            operator has been already provided in the arguments, returns that
            operator and value without performing any checks.
        """

        # we can simplify this later, once we are sure
        # it's fully correct

        if op is None: # XXX: If not `None`, basically does nothing?

            if self._isarray(col):

                if isinstance(val, _const.SIMPLE_TYPES):

                    # col in set[val]
                    op = 'in'
                    val = any_(array(val))

                else:

                    # col.any_(val)
                    op = '&&'

            elif val is True:
                op = 'IS'

            elif val is False:
                op = 'NOT'

            elif isinstance(val, _const.SIMPLE_TYPES):

                # col.val == val.val
                # Note: this covers BOOL columns, despite
                # there the operator is redundant
                op = '='

            else:

                # col.val in set[val]
                op = '='
                val = any_(array(val))

        return op, val


    def _isarray(self, col: InstrumentedAttribute) -> bool:
        """
        Checks whether a given column is array type.

        Args:
            col:
                The column to perform the check.

        Returns:
            Whether the column contains array data or not.
        """

        return col.type.python_type is list


    def _where(self, query: Query, args: dict, query_type: str) -> Query:
        """
        Adds `WHERE` clauses to the query instance.

        Args:
             query:
                The instance of the query to add the `WHERE` clauses
             args:
                A dictionary containing the different arguments for the query
                search (argument name/value pairs).
             query_type:
                The target database name for the query (e.g. `'intercell`).

        Returns:
            The updated query instance with the `WHERE` clauses added according
            to the arguments.
        """

        param = self.query_param[query_type].get('where', {})
        synonyms = self.query_param[query_type].get('where_synonyms', {})
        columns = self._columns(query_type)

        # Adding WHERE clauses
        for key, value in args.items():

            # If key has synonym, get long version, otherwise, keep as it is
            key = synonyms.get(key, key)

            if col_op := param.get(key, None):

                value = self._parse_arg(value)
                cols, *op = _misc.to_tuple(col_op) + (None,)

                where_expr = []

                for col in cols.split(':'):

                    col = columns[col]
                    op, value = self._where_op(col, value, op[0])

                    if op == 'NOT':

                        expr = not_(col)

                    else:

                        expr = col.op(op)(value)

                    where_expr.append(expr)

                query = query.filter(or_(*where_expr))

        return query


    def _select(self, args: dict, query_type: str) -> Query:
        """
        Creates a query with a `SELECT` clause.

        Args:
             args:
                A dictionary containing the different arguments for the query
                selection (argument name/value pairs).
             query_type:
                The target database name for the query (e.g. `'intercell'`).

        Returns:
            The newly created query instance with the `SELECT` clause based on
            the provided arguments.
        """

        param = self.query_param[query_type]
        synonyms = param.get('select', {})
        cols = param.get('select_default', set())
        tbl = self._schema(query_type)
        query_fields = set()

        fields_arg = set(self._parse_arg(args.get('fields', None)))
        fields_arg |= {
            f
            for f in param.get('select_args', set())
            if args.get(f, False)
        }

        for query_field in fields_arg:

            query_fields |= _misc.to_set(synonyms.get(query_field, query_field))

        cols.update(_misc.to_set(query_fields))
        select = [
            c
            for c in tbl.__table__.columns
            if c.name != 'id' and (not cols or c.name in cols)
        ]

        # Instance of sqlalchemy.orm.Query
        return self.con.session.query(*select)


    def _limit(self, query: Query, args: dict) -> Query:
        """
        Adds `LIMIT` clauses to the query instance.

        Args:
             query:
                The instance of the query to add the `LIMIT` clause.
             args:
                A dictionary containing the different arguments for the query
                search (argument name/value pairs). Only the one under the
                `'limit'` key will be used in this case.

        Returns:
            The updated query instance with the `LIMIT` clauses added according
            to the arguments.
        """

        if 'limit' in args:

            query = query.limit(self._parse_arg(args['limit'], typ = int))

        return query


    def _query(
            self,
            args: dict,
            query_type: str,
            extra_where: Iterable | None = None,
    ) -> tuple[Query | None, str | None]:
        """
        Generates the SQL query based on the request arguments.

        Args:
            args:
                The query arguments
            query_type:
                The database which to query (e.g. `'interactions'`,
                `'complexes'`, etc).
            extra_where:
                Extra arguments for the WHERE statement.

        Return:
            To be refined in the future: for now, either an SQL query, or an
            error message.
        """

        query = None
        bad_req = self._check_args(args, query_type)

        if not bad_req:

            # TODO: introduce systematic solution for synonyms
            if 'databases' in args:

                args['resources'] = args['databases']

            query = self._select(args, query_type)
            query = self._where(query, args, query_type)

            if extra_where := [
                w
                for w in _misc.to_list(extra_where)
                if w is not None
            ]:

                extra_where = and_(*extra_where)
                query = query.filter(extra_where)

            query = self._limit(query, args)

            # TODO: reimplement and enable license filtering
            # tbl = self._filter_by_license_complexes(tbl, license)

        return query, bad_req


    def _execute(self, query: Query) -> GEN_OF_TUPLES:
        """
        Executes a query and returns a generator of the response.

        Args:
            query:
                The query to execute.
            args:
                Not used.

        Yields:
            Tuples with the response rows.
        """

        for row in self.con.execute(query):

            yield tuple(row)


    def _request(
            self,
            args: dict,
            query_type: str,
            extra_where: Iterable | None = None,
            format: FORMATS | None = None,
            header: bool | None = None,
            postprocess: Callable[[tuple], tuple] | None = None,
            postformat: Callable[[str], str] | None = None,
            precontent: Iterable[str] | None = None,
            postcontent: Iterable[str] | None = None,
            path: str | None = None,
            license: LICENSE_LEVELS | None = None,
            **kwargs,
    ) -> Generator[tuple | str | dict, None, None]:
        """
        Generic request, each request should call this. Implements the
        query-execute-postprocess-format pipeline.

        Args:
            args:
                The query argument.
            query_type:
                The table to query (e.g. `'interactions'`, `'complexes'`, etc).
            extra_where:
                Extra arguments for the WHERE statement.
            format:
                The format to return (`'raw'`, `'json'`, `'tab'`, `'text'`,
                `'tsv'`, `'table'`, `'query'`); default is `'tsv'`. In case of
                raw format, the tuples will be streamed as they come from the
                database. In case of tsv or json, lines will be streamed, either
                tab joined or json encoded strings. The query format, returns
                the instance of the query object.
            header:
                Whether to include the column names in the response.
            postprocess:
                A function to post-process the result. This will be called on
                each tuple returned by the query.
            postformat:
                A function to be called after formatting the result. This will
                be called on each tab joined or json encoded line. The function
                must accept bool as its second argument, this signals the last
                element to enable its special formatting.
            precontent:
                A list of lines to be added to the beginning of the response.
            postcontent:
                A list of lines to be added to the end of the response.
            kwargs:
                Additional keyword arguments to be passed to the postprocess.

        Yields:
            Tuples with the result of the request after post-processing.
        """

        fields_to_remove = args.pop('fields_to_remove', set())
        args = self._clean_args(args)
        args = self._array_args(args, query_type)
        query, bad_req = self._query(
            args,
            query_type,
            extra_where=extra_where,
        )
        format = format or args.pop('format', None) or 'tsv'
        colnames = ['no_column_names']

        if format == 'query':
            result = ((query,),)

        elif query:

            result = self._execute(query)
            colnames = [c.name for c in query.statement.selected_columns]
            _log(
                'Finished executing query, columns in result: %s'
                % ', '.join(colnames),
            )
            result = self._license_filter(
                records = result,
                query_type = query_type,
                cols = colnames,
                license = license,
            )

            if fields_to_remove:

                idx = [i for i, c in enumerate(colnames) if c in fields_to_remove]
                colnames = [c for i, c in enumerate(colnames) if i not in idx]
                result = (
                    tuple(r[i] for i in range(len(r)) if i not in idx)
                    for r in result
                )

            if callable(postprocess):

                result = (postprocess(rec, **kwargs) for rec in result)

        else:

            result = ((bad_req,),)

        header = args.get('header', True) if header is None else header
        names = colnames if header or format in {'raw', 'json'} else None

        yield from self._output(
            result,
            names,
            format = format,
            postformat=postformat if query else None,
            precontent=precontent,
            postcontent=postcontent,
            **kwargs,
        )


    def _output(
        self,
        result,
        names,
        format: FORMATS | None = None,
        postformat: Callable[[str], str] | None = None,
        precontent: Iterable[str] | None = None,
        postcontent: Iterable[str] | None = None,
        **kwargs,
    ):
        """
        TODO
        """

        _ = kwargs.pop('path', None)

        result = self._format(result, format = format, names = names)

        if callable(postformat):

            result = (postformat(*rec, **kwargs) for rec in with_last(result))

        postcontent = () if postcontent is None else postcontent
        precontent = () if precontent is None else precontent

        yield from itertools.chain(precontent, result, postcontent)


    def _format(
            self,
            result: GEN_OF_TUPLES,
            format: FORMATS = 'raw',
            names: list[str] | None = None,
    ) -> GEN_OF_TUPLES:
        """
        Formats the result as Python generator, TSV or JSON.

        Args:
            result:
                A generator of tuples, each representing a record.
            format:
                One of the supported format literals (`'raw'`, `'tsv'`,
                `'json'`, ...).
            names:
                Column names.

        Yields:
            Tuples with the formatted results.
        """

        formatter = lambda x: x

        if format == 'raw':

            if names:

                record = collections.namedtuple('Record', names)
                formatter = lambda x: record(*x)

            for rec in result:

                yield formatter(rec)

        elif format == 'json':

            if names:

                formatter = lambda x: dict(zip(names, x))

            for rec in result:

                yield json.dumps(formatter(rec))

        elif format == 'query':

            yield from result

        else:

            formatter = self._table_formatter

            if names:

                yield formatter(names)

            for rec in result:

                yield formatter(rec)


    @classmethod
    def _table_formatter(cls, rec: tuple) -> str:
        """
        Fortmats result record as a tab-separated entry.

        Args:
            rec:
                A record entry as a tuple.

        Returns:
            The entry formatted as tab-separated text.
        """

        return '\t'.join(cls._table_field_formatter(f) for f in rec) + '\n'


    @staticmethod
    def _table_field_formatter(field: Any) -> str:
        """
        Formats an individual field from a record.

        Args:
            field:
                The field to be formatted.

        Returns:
            The formatted field.
        """

        return (
            ';'.join(field)
                if isinstance(field, _const.LIST_LIKE) else
            json.dumps(field)
                if isinstance(field, dict) else
            str(field)
        )


    def _interactions_defaults(self, args: dict) -> dict:
        """
        Handles default arguments depending on values of other arguments.

        If no sources, datasets or types (or types includes post_translational),
        includes by default the omnipath dataset to the query.
        If no sources or datasets provided and type includes transcriptional,
        adds collectri to datasets by default.
        If dorothea in datasets, sets default dorothea_levels to A and B.

        Args:
            args:
                The query arguments dictionary for the interactions database.

        Returns:
            The updated args dictionary with the described defaults above.
        """

        in_args = lambda k: args.get(k, [])

        if (
            (
                'dorothea_levels' in args or
                'dorothea_methods' in args
            ) and not 'dorothea' in in_args('datasets')
        ):

            args['datasets'] = in_args('datasets') + ['dorothea']

        if not in_args('resources') and not in_args('datasets'):

            if not in_args('types') or 'post_translational' in in_args('types'):

                args['datasets'] = ['omnipath']

            if 'transcriptional' in in_args('types'):

                args['datasets'] = in_args('datasets') + ['collectri']

        if 'dorothea' in in_args('datasets') and not in_args('dorothea_levels'):

            args['dorothea_levels'] = {'A', 'B'}

        # XXX: If dorothea in datasets and dorothea levels are specified, query
        #      does not have where clause for datasets

        return args


    def interactions(
            self,
            resources: list[str] | None = None,
            partners: list[str] | None = None,
            sources: list[str] | None = None,
            targets: list[str] | None = None,
            fields: list[str] | None = None,
            limit: int | None = None,
            format: FORMATS | None = None,
            source_target: Literal['OR', 'AND'] = 'OR',
            organisms: Collection[str | ORGANISMS] | None  = None,
            datasets: Collection[INTERACTION_DATASETS] | None  = None,
            dorothea_levels: Collection[DOROTHEA_LEVELS] | None = None,
            dorothea_methods: Collection[DOROTHEA_METHODS] | None = None,
            types: Collection[INTERACTION_TYPES] | None = None,
            directed: bool = True,
            signed: bool = None,
            loops: bool = False,
            entity_types: Collection[ENTITY_TYPES] | None = None,
            evidences: bool = False,
            genesymbols: bool = False,
            extra_attrs: bool = False,
            **kwargs,
    ) -> Generator[tuple | str, None, None]:
        """
        Creates the generator of entries based on the query arguments for the
        interactions service.

        Args:
            resources:
                Defines which resource(s) to use records from.
            partners:
                Entities to search interactions for, regardless of their role as
                source or target in the interaction.
            sources:
                Entities to search interactions for, acting as source nodes of
                the interaction.
            targets:
                Entities to search interactions for, acting as target nodes of
                the interaction.
            fields:
                Fields (columns) to include in the output result table.
            limit:
                Limit number of entries in the search result.
            format:
                The format to return (`'raw'`, `'json'`, `'tab'`, `'text'`,
                `'tsv'`, `'table'`, `'query'`); default is `'tsv'`. In case of
                raw format, the tuples will be streamed as they come from the
                database. In case of tsv or json, lines will be streamed, either
                tab joined or json encoded strings. The query format, returns
                the instance of the query object.
            source_target:
                Operator to use between sources and targets arguments.
            organisms:
                Organism to search interactions from.
            datasets:
                Datasets to include in the search space.
            dorothea_levels:
                Which levels of confidence to include in the search space for
                the DoRothEA interactions.
            dorothea_methods:
                Which methods to include from DoRothEA interactions.
            types:
                Types of interactions to include in the search.
            directed:
                Whether to search only directed interactions or all (both
                directed and undirected).
            signed:
                Whether to search only signed interactions or all (both
                signed and unsigned).
            loops:
                Whether to include self loops or not in the results.
            entity_types:
                Which types of entities to show interactions for (e.g.
                `'proteins'`, `'complex'`, `'drug'`, etc.).
            evidences:
                Whether or not to show evidences for each record.
            genesymbols:
                Whether or not to display genesymbols as identifiers.
            extra_attrs:
                Whether or not to show extra attributes assigned to each record.
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Yields:
            The search results in the interactions database in the requested
            format.
        """

        organisms = organisms or {9606}
        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'interactions')

        args = self._interactions_defaults(args)

        extra_where = self._where_partners('interactions', args)
        where_loops = self._where_loops('interactions', args)
        where_bool = self._where_bool('interactions', args)

        args = self._inject_fields(args)

        _log(f'Args: {_misc.dict_str(args)}')
        _log(f'Interactions where: {extra_where}, {where_bool}, {where_loops}')

        yield from self._request(
            args,
            query_type = 'interactions',
            extra_where = [extra_where, where_bool, where_loops],
            **kwargs,
        )

    @staticmethod
    def _inject_fields(args):

        req_fields = _misc.to_set(args.pop('fields', None))
        use_fields = req_fields | {'sources', 'references'}
        args['fields'] = use_fields
        args['fields_to_remove'] = use_fields - req_fields

        return args


    def _where_bool(
            self,
            query_type: str,
            args: dict,
    ) -> BooleanClauseList | None:
        """
        Generates WHERE statement(s) of a boolean variable(s).

        Args:
            query_type:
                The table to query (e.g. `'interactions'`, `'complexes'`, etc).
            args:
                The query arguments.

        Returns:
            The boolean variable clause (multiple ones joined by and operator).
        """

        def _override(col):

            if (arg_col := override.get(col)):

                expr = []

                # We are here: make this work with dorothea_methods
                for arg, _col in arg_col.items():

                    if (expr_part := override_expr.get(arg)) is not None:

                        expr.append(expr_part)

                    elif value := args.get(arg):

                        _col = columns[_col]
                        op, value = self._where_op(_col, value)
                        expr.append(_col.op(op)(value))

                expr = and_(*expr)

            else:

                expr = columns[col]

            return expr


        override = self.query_param[query_type].get('where_bool_override', {})
        in_override = {arg for key in override.values() for arg in key.keys()}
        override_expr = {}

        bool_args = self.query_param[query_type].get('where_bool', {})
        columns = self._columns(query_type)

        where = []

        for arg, cols in bool_args.items():

            arg_cols = _misc.to_set(args.get(arg, set()))

            if isinstance(cols, dict):

                arg_cols = {cols.get(x, x) for x in arg_cols}
                cols = set(cols.values())

            if arg == 'signed' and True in arg_cols:

                arg_cols = {'is_stimulation', 'is_inhibition'}

            if (cols := arg_cols & cols):

                expr = or_(*(_override(col) for col in sorted(cols)))

                if arg in in_override:

                    override_expr[arg] = expr

                else:

                    where.append(expr)

        return and_(True, *where)


    def enzsub(
            self,
            resources: list[str] | None = None,
            partners: list[str] | None = None,
            enzymes: list[str] | None = None,
            substrates: list[str] | None = None,
            types: list[str] | None = None,
            residues: list[str] | None = None,
            fields: list[str] | None = None,
            limit: int | None = None,
            format: FORMATS | None = None,
            enzyme_substrate = 'OR',
            organisms: Collection[int | str] | None = None,
            loops: bool = False,
            genesymbols: bool = False,
            **kwargs,
    ) -> Generator[tuple | str, None, None]:
        """
        Creates the generator of entries based on the query arguments for the
        enzyme-substrate service.

        Args:
            resources:
                Defines which resource(s) to use records from.
            partners:
                Entities to search interactions for, regardless of their role as
                enzyme or substrate in the interaction.
            enzymes:
                Entities to search interactions for, acting as enzymes of the
                interaction.
            substrates:
                Entities to search interactions for, acting as substrates of the
                interaction.
            types:
                Types of enzyme-substrate interactions to include in the search.
            residues:
                Search term for specific modified residues in a protein.
            fields:
                Fields (columns) to include in the output result table.
            limit:
                Limit number of entries in the search result.
            format:
                The format to return (`'raw'`, `'json'`, `'tab'`, `'text'`,
                `'tsv'`, `'table'`, `'query'`); default is `'tsv'`. In case of
                raw format, the tuples will be streamed as they come from the
                database. In case of tsv or json, lines will be streamed, either
                tab joined or json encoded strings. The query format, returns
                the instance of the query object.
            enzyme_substrate:
                Operator to use between enzymes and substrates arguments.
            organisms:
                Organism to search interactions from.
            loops:
                Whether to include self loops or not in the results.
            genesymbols:
                Whether or not to display genesymbols as identifiers.
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Yields:
            The search results in the enzsub database in the requested format.
        """

        organisms = organisms or {9606}
        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'enzsub')
        where_loops = self._where_loops('enzsub', args)
        extra_where = self._where_partners('enzsub', args)

        args = self._inject_fields(args)

        _log(f'Args: {_misc.dict_str(args)}')
        _log(f'Enzsub where: {extra_where}, {where_loops}')

        yield from self._request(
            args,
            query_type = 'enzsub',
            extra_where = [extra_where, where_loops],
            **kwargs,
        )


    # Synonym
    enz_sub = enzsub


    def _query_sqla(self, query_type: QUERY_TYPES, **kwargs) -> Query:
        """
        Returns the query instance of a search instead of the actual results.

        Args:
            query_type:
                The database which to query (e.g. `'interactions'`,
                `'complexes'`, etc).
            **kwargs:
                Arguments passed to the corresponding query method
                (`interactions`, `enzsub`, ...).

        Returns:
            Instance of the SQLalchemy query object.
        """

        kwargs['format'] = 'query'

        return next(getattr(self, query_type)(**kwargs))[0]


    def _query_str(self, query_type: QUERY_TYPES, **kwargs) -> str:
        """
        Returns the query string instead of the actual results.

        Args:
            query_type:
                The database which to query (e.g. `'interactions'`,
                `'complexes'`, etc).
            **kwargs:
                Arguments passed to the corresponding query method
                (`interactions`, `enzsub`, ...).

        Returns:
            The SQL query string.
        """

        q_str = str(self._query_sqla(query_type, **kwargs))

        return re.sub(r'\s+', ' ', q_str)


    def _where_loops(
            self,
            query_type: QUERY_TYPES,
            args: dict,
    ) -> BooleanClauseList | None:
        """
        Generates WHERE statement for loops option.

        Args:
            query_type:
                The table to query (e.g. `'interactions'`, `'enzsub'`, etc).
            args:
                The query arguments.

        Returns:
            The loops variable WHERE clause.
        """

        sides = self.query_param[query_type]['where_partners']['sides']
        columns = self._columns(query_type)

        if not args.get('loops', False):

            cols = [columns[side] for side in sides.values()]

            return cols[0].op('!=')(cols[1])


    def _where_partners(
            self,
            query_type: QUERY_TYPES,
            args: dict,
    ) -> BooleanClauseList | None:
        """
        Generates WHERE statement(s) that deal with filtering interactions by
        partners e.g. when source/target or enz/subs are provided in the query.

        Args:
            query_type:
                The table to query (e.g. `'interactions'`, `'enzsub'`, etc).
            args:
                The query arguments.

        Returns:
            The boolean variable clause (multiple ones joined according to
            specified operator in `args` otherwise, defaults to and).
        """

        sides = self.query_param[query_type]['where_partners']['sides']
        query_op = self.query_param[query_type]['where_partners']['operator']

        for side in sides:

            args[side] = args.get(side, None) or args.get('partners', None)

        columns = self._columns(query_type)

        partners_where = []

        for side, sidecol in sides.items():

            conditions = []

            # Skip if nothing is provided
            if not args[side]:

                continue

            for suffix in ('', '_genesymbol'):

                col = columns[f'{sidecol}{suffix}']
                op, val = self._where_op(col, args[side])
                expr = col.op(op)(val)
                conditions.append(expr)

            partners_where.append(or_(*conditions))

        if len(partners_where) == 1:

            return partners_where[0]

        elif len(partners_where) == 2:

            op = or_ if args[query_op].upper() == 'OR' else and_

            return op(*partners_where)


    def annotations(
            self,
            resources: list[str] | None = None,
            proteins: list[str] | None = None,
            entity_types: ENTITY_TYPES | None = None,
            fields: list[str] | None = None,
            limit: int | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ) -> Generator[tuple | str, None, None]:
        '''
        Creates the generator of entries based on the query arguments for the
        annotations service.

        Args:
            resources:
                Defines which resource(s) to use records from.
            proteins:
                Entities to search annotations for.
            entity_types:
                Type of entities to search annotations for (e.g. `'complex'`,
                `'protein'`, `'drug'`, `'mirna'`, etc.).
            fields:
                Fields (columns) to include in the output result table.
            limit:
                Limit number of entries in the search result.
            format:
                The format to return (`'raw'`, `'json'`, `'tab'`, `'text'`,
                `'tsv'`, `'table'`, `'query'`); default is `'tsv'`. In case of
                raw format, the tuples will be streamed as they come from the
                database. In case of tsv or json, lines will be streamed, either
                tab joined or json encoded strings. The query format, returns
                the instance of the query object.
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Yields:
            The search results in the annotations database in the requested
            format.
        '''

        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'annotations')

        _log(f'Args: {_misc.dict_str(args)}')

        yield from self._request(
            args,
            query_type = 'annotations',
            **kwargs,
        )


    def annotations_summary(
            self,
            resources: list[str] | None = None,
            cytoscape: bool = False,
            format: FORMATS | None = None,
            **kwargs,
    ):
        """
        Generates the summary of the annotations database (i.e. list of unique
        source, label, value triplets).
        """

        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'annotations')
        format = self._ensure_str(format)

        renum = re.compile(r'(?:[-\d\.]+|nan|-?inf)')

        summary = {
            (
                row[:-1] + ('<numeric>',)
                if all([re.match(renum, val) for val in row[-1]])
                else row[:-1] + ('#'.join(row[-1]),)
            )
            for row in self._cached_data["annotations_summary"]
        }

        if 'resources' in args:

            summary = {
                row for row in summary
                if row[0] in args['resources']
            }

        if args['cytoscape']:

            summary = {
                row for row in summary
                if (
                    row[0] in self.cytoscape_attributes.keys()
                    and row[1] in self.cytoscape_attributes.values()
                )
            }

        yield from self._output(
            summary,
            names = ["source", "label", "value"],
            format = format,
            **kwargs,
        )


    # TODO: Revisit handling of long/short synonym arguments
    # TODO: Addd missing causality and topology
    def intercell(
            self,
            resources: list[str] | None = None,
            proteins: list[str] | None = None,
            entity_types: ENTITY_TYPES | None = None,
            aspect: list[str] | None = None,
            scope: list[str] | None = None,
            source: list[str] | None = None,
            categories: list[str] | None = None,
            parent: list[str] | None = None,
            transmitter: bool | None = None,
            trans: bool | None = None, # Synonym
            receiver: bool | None = None,
            rec: bool | None = None, # Synonym
            secreted: bool | None = None,
            sec: bool | None = None, # Synonym
            plasma_membrane_transmembrane: bool | None = None,
            pmtm: bool | None = None, # Synonym
            plasma_membrane_peripheral: bool | None = None,
            pmp: bool | None = None, # Synonym
            fields: list[str] | None = None,
            limit: int | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ) -> Generator[tuple | str, None, None]:
        '''
        Creates the generator of entries based on the query arguments for the
        intercell service.

        Args:
            resources:
                Defines which resource(s) to use records from.
            proteins:
                Entities to search interactions for.
            entity_types:
                Type of entities to search annotations for (e.g. `'complex'`,
                `'protein'`, `'drug'`, `'mirna'`, etc.).
            aspect:
                Type of intercellular annotation (e.g. `'functional'` or
                `'locational'`).
            scope:
                Scope of the intercellular annotation (`'generic'` or
                `'specific'`).
            source:
                Type of source for the intercellular annotation (`'composite'`
                or `'resource_specific'`).
            categories:
                Type(s) of entity(ies) involved in the interaction(s) (e.g.
                `'adhesion'`, `'chemokine'`, `'gpcr'`, etc.).
            parent:
                Parent category(ies) of the entity(ies) involved in the
                interaction(s) (e.g. `'ecm'`, `'receptor'`, `'transporter'`,
                etc.).
            transmitter:
                Source node(s) of the interaction(s)/transmitter of the signal.
            trans:
                Synonym of `transmitter`.
            receiver:
                Target node(s) of the interaction(s)/receiver of the signal.
            rec:
                Synonym of `receiver`.
            secreted:
                Whether to search specifically for secreted molecules or not.
            sec:
                Synonym of `secreted`.
            plasma_membrane_transmembrane:
                Whether to search specifically for transmembrane molecules or
                not.
            pmtm:
                Synonym of `plasma_membrane_transmembrane`.
            plasma_membrane_peripheral:
                Whether to search specifically for peripheral to the plasma
                membrane molecules or not.
            pmp:
                Synonym of `plasma_membrane_peripheral`.
            fields:
                Fields (columns) to include in the output result table.
            limit:
                Limit number of entries in the search result.
            format:
                The format to return (`'raw'`, `'json'`, `'tab'`, `'text'`,
                `'tsv'`, `'table'`, `'query'`); default is `'tsv'`. In case of
                raw format, the tuples will be streamed as they come from the
                database. In case of tsv or json, lines will be streamed, either
                tab joined or json encoded strings. The query format, returns
                the instance of the query object.
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Yields:
            The search results in the intercell database in the requested
            format.
        '''

        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'intercell')

        _log(f'Args: {_misc.dict_str(args)}')

        yield from self._request(
            args,
            query_type = 'intercell',
            **kwargs,
        )


    def intercell_summary(
            self,
            aspect: str | Collection[str] | None = None,
            source: str | Collection[str] | None = None,
            scope: str | Collection[str] | None = None,
            transmitter: str | Collection[str] | None = None,
            receiver: str | Collection[str] | None = None,
            parent: str | Collection[str] | None = None,
            resources: str | Collection[str] | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ):
        """
        Generates the summary of the intercell database (i.e. list of unique
        category, parent, database triplets).
        """

        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'intercell')
        format = self._ensure_str(format)

        result = self._cached_data["intercell_summary"]

        for var in (
            'aspect',
            'source',
            'scope',
            'transmitter',
            'receiver',
            'parent',
            'resources',
        ):

            if values := args.get(var):

                result = [x for x in result if getattr(x, var) in values]

        yield from self._output(
            ((x.category, x.parent, x.database) for x in result),
            names = ['category', 'parent', 'database'],
            format = format,
            **kwargs,
        )


    def complexes(
            self,
            resources: list[str] | None = None,
            proteins: list[str] | None = None,
            fields: list[str] | None = None,
            limit: int | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ) -> Generator[tuple | str, None, None]:
        """
        Creates the generator of entries based on the query arguments for the
        complexes service.

        Args:
            resources:
                Defines which resource(s) to use records from.
            proteins:
                Entities to search complexes for.
            fields:
                Fields (columns) to include in the output result table.
            limit:
                Limit number of entries in the search result.
            format:
                The format to return (`'raw'`, `'json'`, `'tab'`, `'text'`,
                `'tsv'`, `'table'`, `'query'`); default is `'tsv'`. In case of
                raw format, the tuples will be streamed as they come from the
                database. In case of tsv or json, lines will be streamed, either
                tab joined or json encoded strings. The query format, returns
                the instance of the query object.
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Yields:
            The search results in the complexes database in the requested
            format.
        """

        args = locals()

        yield from self._request(args, 'complexes', **kwargs)


    def resources(
            self,
            datasets: Collection[INTERACTION_DATASETS] | None = None,
            license: LICENSE_LEVELS | None = None,
            format: FORMATS | None = None,
            **kwargs,
    ) -> Generator[tuple | str, None, None]:

        datasets = {
            self._query_type(dataset)
            for dataset in _misc.to_list(datasets)
        }

        license = self._query_license_level(license)
        resources_enabled = self._license_enables(license)

        result = {
            k: v
            for k, v in self._resources_meta.items()
            if (
                k in resources_enabled and
                (
                    not datasets or
                    datasets & v['datasets'] or
                    datasets & set(v['queries'].keys())
                )
            )
        }

        if format == 'raw':

            result = (result,)

        else:

            result = (json.dumps(result, cls=SetEncoder),)

        yield from result


    @staticmethod
    def _query_license_level(license: LICENSE_LEVELS | None = None):

        return (
            license
                if license in LICENSE_LEVELS.__args__ else
            DEFAULT_LICENSE
        )


    @staticmethod
    def _prefix(name):
        """
        Checks the prefix
        """

        return name.split(":", maxsplit = 1)[0]


    @functools.cache
    def _license_enables(self, license: LICENSE_LEVELS) -> set[str]:
        """
        TODO
        """

        license = self._query_license_level(license)
        rank = LICENSE_RANKS[license]

        enabled = {
            resource
            for resource, info in self._resources_meta.items()
            if LICENSE_RANKS[info['license']['purpose']] <= rank
        }

        enabled |= {
            resource
            for resource, info in self._resources_meta.items()
            if (
                info['license']['purpose'] == 'composite' and
                any(comp in enabled for comp in info['components'])
            )
        }

        enabled |= {res.lower() for res in enabled}

        return enabled


    def _license_filter(
            self,
            records: Iterable[tuple],
            query_type: QUERY_TYPES,
            cols: list[str],
            license: LICENSE_LEVELS | None = None,
    ):

        def filter_resources(res, prefix = False):

            pref = self._prefix if prefix else lambda x: x

            if isinstance(res, str):
                if pref(res) in enabled_resources:
                    return res

            else:
                return {r for r in res if pref(r) in enabled_resources}

        _log('Applying license filtering level: %s' % license)

        license = self._query_license_level(license)

        if license == LICENSE_IGNORE:

            _log('Skipping license filtering')

            yield from records

        else:

            enabled_resources = self._license_enables(license)

            res_col = cols.index(self._resource_col(query_type))
            prefix_cols_idx = [
                cols.index(i) for i in self._resource_prefix_cols(query_type)
            ]

            before = 0
            after = 0

            for rec in records:

                before += 1
                rec = list(rec)
                rec[res_col] = filter_resources(rec[res_col])

                if not rec[res_col]:

                    continue

                for c in prefix_cols_idx:

                    rec[c] = rec[c].split(';') if rec[c] else ()
                    rec[c] = filter_resources(rec[c], prefix = True)
                    rec[c] = ';'.join(rec[c])

                after += 1

                yield tuple(rec)

            _log(
                f'Parsed {before} records, '
                f'{after} records passed the filtering.',
            )


    def _parse_arg(self, arg: Any, typ: type = None) -> Any:
        """
        Arguments come as strings, here we parse individual arguments to the
        appropriate type. At least from the HTTP interface, we get them as
        strings. In case these come from elsewhere, and provided already as
        numeric or array types, this function simply passes them through without
        modification.

        Args:
            arg:
                The argument value to parse.
            typ:
                Type of the contents in the argument (only used when passing
                arg as a list).

        Returns:
            The argument formatted in the correct type.
        """

        if isinstance(arg, list) and typ in _const.SIMPLE_TYPES:

            arg = arg[0] if arg else None

        elif arg is None:

            arg = []

        if isinstance(arg, str):

            if _misc.is_int(arg):

                arg = int(arg)

            elif _misc.is_float(arg):

                arg = float(arg)

            elif ',' in arg:

                arg = arg.split(',')

        return arg


    def _parse_bool_arg(self, arg: Any) -> bool:
        """
        Normalizes various representations of Boolean values. These can be `0`
        or `1`, `True` or `False`, `"true"` or `"false"`, `"yes"` or `"no"`.

        Args:
            arg:
                The argument to parse.

        Returns:
            The corresponding boolean value according to the boolean-encoded
            argument.
        """

        if isinstance(arg, list) and arg:

            arg = arg[0]

        if hasattr(arg, 'decode'):

            arg = arg.decode('utf-8')

        if hasattr(arg, 'lower'):

            arg = arg.lower()

        if hasattr(arg, 'isdigit') and arg.isdigit():

            arg = int(arg)

        if arg in _const.BOOLEAN_FALSE:

            arg = False

        if arg in _const.BOOLEAN_TRUE:

            arg = True

        return bool(arg)


def with_last(iterable: Iterable[Any]) -> Generator[Any, bool]:
    """
    Iterate over an iterable with signaling the last element.

    Yields:
        Tuple of the elements in the iterable and a boolean value which is True
        only at the last element.
    """

    itr = iter(iterable)
    prev = next(itr, None)

    if prev is not None:

        for it in itr:

            yield prev, False

            prev = it

        yield prev, True
