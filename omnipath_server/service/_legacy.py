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
import os
import re
import json
import warnings
import importlib as imp
import itertools
import contextlib
import collections

from sqlalchemy import or_, and_, any_, not_
from pypath_common import _misc, _settings
from pypath_common import _constants as _const
from sqlalchemy.orm import Query
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.dialects.postgresql import array

import numpy as np
import pandas as pd

from omnipath_server import session
from .. import _log, _connection
from ..schema import _legacy as _schema

__all__ = [
    'FORMATS',
    'GEN_OF_STR',
    'GEN_OF_TUPLES',
    'LICENSE_IGNORE',
    'LegacyService',
    'ORGANISMS',
    'QUERY_TYPES',
    'ENTITY_TYPES',
    'ignore_pandas_copywarn',
    'with_last',
]


LICENSE_IGNORE = 'ignore'
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
                'genesymbol': {'source_genesymbol', 'target_genesymbol'},
                'organism': {'ncbi_tax_id_source', 'ncbi_tax_id_target'},
                'entity_type': {'entity_type_source', 'entity_type_target'},
                'extra_attrs': 'extra_attrs',
                'evidences': 'evidences',
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
            },
            'where': {
                'resources': 'sources',
                'types': 'type',
                'directed': 'is_directed',
                'organisms': 'ncbi_tax_id_source:ncbi_tax_id_target',
                'entity_types': 'entity_type_source:entity_type_target',
                'dorothea_levels': 'dorothea_level',
            },
            'where_partners': {
                'sides': {
                    'sources': 'source',
                    'targets': 'target',
                },
                'operator': 'source_target',
            },
            'where_bool': {
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
                'dorothea_methods': {
                    'curated': 'dorothea_curated',
                    'chipseq': 'dorothea_chipseq',
                    'tfbs': 'dorothea_tfbs',
                    'coexp': 'dorothea_coexp',
                },
                'signed': {
                    'is_stimulation',
                    'is_inhibition',
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
                'genesymbol': {'enzyme_genesymbol', 'substrate_genesymbol'},
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
                'dorothea_curated',
                'dorothea_chipseq',
                'dorothea_tfbs',
                'dorothea_coexp',
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
        ('Zhong2015', 'type'),
        ('MatrixDB', 'mainclass'),
        ('Matrisome', ('mainclass', 'subclass', 'subsubclass')),
        # ('TFcensus', 'in TFcensus'),
        ('Locate', ('location', 'cls')),
        (
            'Phosphatome',
            (
                'family',
                'subfamily',
                #'has_protein_substrates',
            ),
        ),
        ('CancerSEA', 'state'),
        ('GO_Intercell', 'mainclass'),
        ('Adhesome', 'mainclass'),
        ('SignaLink3', 'pathway'),
        (
            'HPA_secretome',
            (
                'mainclass',
                #'secreted',
            ),
        ),
        (
            'OPM',
            (
                'membrane',
                'family',
                #'transmembrane',
            ),
        ),
        ('KEGG', 'pathway'),
        #(
            #'CellPhoneDB',
            #(
                ## 'receptor',
                ## 'peripheral',
                ## 'secreted',
                ## 'transmembrane',
                ## 'receptor_class',
                ## 'secreted_class',
            #)
        #),
        ('kinase.com', ('group', 'family', 'subfamily')),
        ('Membranome', ('membrane',)),
        #('CSPA', 'in CSPA'),
        #('MSigDB', 'geneset'),
        #('Integrins', 'in Integrins'),
        ('HGNC', 'mainclass'),
        ('CPAD', ('pathway', 'effect_on_cancer', 'cancer')),
        ('Signor', 'pathway'),
        ('Ramilowski2015', 'mainclass'),
        ('HPA_subcellular', 'location'),
        #('DisGeNet', 'disease'),
        ('Surfaceome', ('mainclass', 'subclasses')),
        ('IntOGen', 'role'),
        ('HPMR', ('role', 'mainclass', 'subclass', 'subsubclass')),
        #('CancerGeneCensus',
            #(
                ##'hallmark',
                ##'somatic',
                ##'germline',
                #'tumour_types_somatic',
                #'tumour_types_germline',
            #)
        #),
        #('DGIdb', 'category'),
        ('ComPPI', 'location'),
        ('Exocarta', 'vesicle'),
        ('Vesiclepedia', 'vesicle'),
        ('Ramilowski_location', 'location'),
        ('LRdb', ('role', 'cell_type')),
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


        # self.input_files = copy.deepcopy(self.default_input_files)
        # self.input_files.update(input_files or {})
        # self.data = {}

        # self.to_load = (
        #     self.data_query_types - _misc.to_set(exclude_tables)
        #         if only_tables is None else
        #     _misc.to_set(only_tables)
        # )

        # _log('Datasets to load: %s.' % (', '.join(sorted(self.to_load))))

        # self._read_tables()

        # self._preprocess_interactions()
        # self._preprocess_enzsub()
        # self._preprocess_annotations()
        # self._preprocess_intercell()
        # self._update_resources()

        # _log(f'{self.__class__.__name__} startup ready.')

        self._connect(con)


    def reload(self):
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


    # XXX: Deprecated? (has no attribute input_files)
    def _read_tables(self):

        _log('Loading data tables.')

        for name, fname in self.input_files.items():

            if name not in self.to_load:

                continue

            fname_gz = f'{fname}.gz'
            fname = fname_gz if os.path.exists(fname_gz) else fname

            _log(f'Loading dataset `{name}` from file `{fname}`.')

            if not os.path.exists(fname):

                _log(
                    'Missing table: `%s`.' % fname,
                )
                continue

            dtype = self.default_dtypes[name]

            self.data[name] = pd.read_csv(
                fname,
                sep = '\t',
                index_col = False,
                dtype = dtype,
            )

            _log(
                f'Table `{name}` loaded from file `{fname}`.',
            )


    # XXX: Deprecated?
    def _network(self, req):

        hdr = ['nodes', 'edges', 'is_directed', 'sources']
        tbl = self.data['network'].field
        val = dict(zip(tbl.field, tbl.value))

        if b'format' in req.args and req.args['format'] == b'json':
            return json.dumps(val)
        else:
            return '{}\n{}'.format(
                '\t'.join(hdr), '\t'.join(
                [str(val[h]) for h in hdr],
                ),
            )


    # XXX: Deprecated?
    def _preprocess_interactions(self):

        if 'interactions' not in self.data:

            return

        _log('Preprocessing interactions.')
        tbl = self.data['interactions']
        tbl['set_sources'] = pd.Series(
            [set(s.split(';')) for s in tbl.sources],
        )
        tbl['set_dorothea_level'] = pd.Series(
            [
                set(s.split(';'))
                if not pd.isnull(s) else
                set()
                for s in tbl.dorothea_level
            ],
        )


    # XXX: Deprecated?
    def _preprocess_enzsub(self):

        if 'enzsub' not in self.data:

            return

        _log('Preprocessing enzyme-substrate relationships.')
        tbl = self.data['enzsub']
        tbl['set_sources'] = pd.Series(
            [set(s.split(';')) for s in tbl.sources],
        )


    # XXX: Deprecated?
    def _preprocess_annotations_old(self):

        if 'annotations' not in self.data:

            return

        renum = re.compile(r'[-\d\.]+')


        def _agg_values(vals):

            result = (
                '#'.join(sorted({str(ii) for ii in vals}))
                if not all(
                    isinstance(i, (int, float)) or (
                        isinstance(i, str) and
                        i and (
                            i is None or
                            renum.match(i)
                        )
                    )
                    for i in vals
                ) else
                '<numeric>'
            )

            return result


        _log('Preprocessing annotations.')

        self.data['annotations_summary'] = self.data['annotations'].groupby(
            ['source', 'label'],
        ).agg({'value': _agg_values}).reset_index(drop = False)


    # XXX: Deprecated?
    def _preprocess_annotations(self):

        if 'annotations' not in self.data:

            return

        renum = re.compile(r'[-\d\.]+')


        _log('Preprocessing annotations.')

        values_by_key = collections.defaultdict(set)

        # we need to do it this way as we are memory limited on the server
        # and pandas groupby is very memory intensive
        for row in self.data['annotations'].itertuples():

            value = (
                '<numeric>'
                if (
                    (
                        not isinstance(row.value, bool) and
                        isinstance(row.value, (int, float))
                    ) or
                    renum.match(row.value)
                ) else
                str(row.value)
            )

            values_by_key[(row.source, row.label)].add(value)

        for vals in values_by_key.values():

            if len(vals) > 1:

                vals.discard('<numeric>')

            vals.discard('')
            vals.discard('nan')

        self.data['annotations_summary'] = pd.DataFrame(
            list(
                (source, label, '#'.join(sorted(values)))
                for (source, label), values in values_by_key.items()
            ),
            columns = ['source', 'label', 'value'],
        )


    # XXX: Deprecated?
    def _preprocess_intercell(self):

        if 'intercell' not in self.data:

            return

        _log('Preprocessing intercell data.')
        tbl = self.data['intercell']
        tbl.drop('full_name', axis = 1, inplace = True, errors = 'ignore')
        self.data['intercell_summary'] = tbl.filter(
            ['category', 'parent', 'database'],
        ).drop_duplicates()


    # XXX: Deprecated?
    def _update_resources(self):

        _log('Updating resource information.')

        self._resources_dict = collections.defaultdict(dict)

        res_ctrl = resources_mod.get_controller()

        for query_type in self.data_query_types:

            if query_type not in self.data:

                continue

            tbl = self.data[query_type]

            # finding out what is the name of the column with the resources
            # as this is different across the tables
            for colname, argname in (
                ('database', 'databases'),
                ('sources', 'databases'),
                ('source', 'databases'),
                ('category', 'categories'),
            ):

                if colname in tbl.columns:

                    break

            # collecting all resource names
            values = sorted(
                set(
                    itertools.chain(
                        *(
                            val.split(';') for val in getattr(tbl, colname)
                        ),
                    ),
                ),
            )

            for db in values:

                if 'license' not in self._resources_dict[db]:

                    license = res_ctrl.license(db)

                    if license is None:

                        msg = 'No license for resource `%s`.' % str(db)
                        _log(msg)
                        raise RuntimeError(msg)

                    license_data = license.features
                    license_data['name'] = license.name
                    license_data['full_name'] = license.full_name
                    self._resources_dict[db]['license'] = license_data

                if 'queries' not in self._resources_dict[db]:

                    self._resources_dict[db]['queries'] = {}

                if query_type not in self._resources_dict[db]['queries']:

                    if query_type == 'interactions':

                        datasets = set()

                        for dataset in self.datasets_:

                            if dataset not in tbl.columns:

                                continue

                            for in_dataset, resources in zip(
                                getattr(tbl, dataset),
                                tbl.set_sources,
                            ):

                                if in_dataset and db in resources:

                                    datasets.add(dataset)
                                    break

                        self._resources_dict[db]['queries'][query_type] = {
                            'datasets': sorted(datasets),
                        }

                    elif query_type == 'intercell':

                        tbl_db = tbl[
                            (tbl.database == db) &
                            (tbl.scope == 'generic')
                        ]

                        self._resources_dict[db]['queries'][query_type] = {
                            'generic_categories': sorted(
                                set(tbl_db.category),
                            ),
                        }

                    else:

                        self._resources_dict[db]['queries'][query_type] = {}

            self.args_reference[query_type][argname] = values

        self._resources_dict = dict(self._resources_dict)

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


    # XXX: Deprecated?
    def queries(self, req):

        query_type = (
            req.postpath[1]
                if len(req.postpath) > 1 else
            'interactions'
        )

        query_type = self._query_type(query_type)

        query_param = (
            req.postpath[2]
                if len(req.postpath) > 2 else
            None
        )

        if query_type in self.args_reference:

            result = {
                    k:
                    sorted(v) if isinstance(v, _const.LIST_LIKE) else v
                for k, v in self.args_reference[query_type].items()
            }

            if query_param is not None and query_param in result:

                result = {query_param: result[query_param]}

        else:

            result = {}
            result[query_type] = (
                'No possible arguments defined for'
                'query `%s` or no such query available.' % query_type
            )

        result = self._dict_set_to_list(result)

        if b'format' in req.args and req.args['format'][0] == b'json':

            return json.dumps(result)

        else:

            return 'argument\tvalues\n%s' % '\n'.join(
                '{}\t{}'.format(
                    k,
                    ';'.join(v)
                        if isinstance(v, (list, set, tuple)) else
                    str(v),
                )
                for k, v in result.items()
            )


    # XXX: Deprecated?
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


    # XXX: Deprecated?
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


    # XXX: Deprecated?
    def _get_datasets(self):

        return list(self.data['interactions'].type.unique())


    # XXX: Deprecated?
    def datasets(self, req):

        query_type = (
            req.postpath[1]
                if len(req.postpath) > 1 else
            'interactions'
        )

        if query_type == 'interactions':

            result = self._get_datasets()

        else:

            result = []

        if b'format' in req.args and req.args['format'][0] == b'json':

            return json.dumps(result)

        else:

            return ';'.join(result)


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
                The target database name for the query (e.g. `'intercell`).

        Returns:
            The newly created query instance with the `SELECT` clause based on
            the provided arguments.
        """

        param = self.query_param[query_type]
        synonyms = param.get('select', {})
        cols = param.get('select_default', set())
        tbl = self._schema(query_type)
        query_fields = set()

        for query_field in self._parse_arg(args.get('fields', None)):

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

            if extra_where := [w for w in _misc.to_list(extra_where) if w is not None]:

                extra_where = and_(*extra_where)
                query = query.filter(extra_where)

            query = self._limit(query, args)

            # TODO: reimplement and enable license filtering
            # tbl = self._filter_by_license_complexes(tbl, license)

        return query, bad_req


    # XXX: args not used, remove?
    def _execute(self, query: Query, args: dict) -> GEN_OF_TUPLES:
        """
        Executes a query and returns a generator of the response.

        Args:
            query:
                The query to execute.
            args:
                Not used.

        Returns:
            Generator of tuples with the response rows.
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

        Returns:
            Generator of tuples with the result of the request after
            post-processing.
        """

        args = self._clean_args(args)
        args = self._array_args(args, query_type)
        query, bad_req = self._query(args, query_type, extra_where=extra_where)
        colnames = ['no_column_names']
        format = format or args.pop('format', None) or 'tsv'

        if format == 'query':
            result = ((query,),)

        elif query:

            result = self._execute(query, args)
            colnames = [c.name for c in query.statement.selected_columns]

            if callable(postprocess):

                result = (postprocess(rec, **kwargs) for rec in result)

        else:

            result = ((bad_req,),)

        header = args.get('header', True) if header is None else header
        names = colnames if header or format in {'raw', 'json'} else None
        result = self._format(result, format = format, names = names)

        if query and callable(postformat):

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

        Returns:
            Generator of tuples with the formatted results.
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
    

    def _dorothea_where(self, args):
        """
        TODO
        """
        # if dorothea_included FALSE, then we don't handle with anyting
        # if we have transcriptional interactions and NO datasets selected type = transcriptional

        # if collectri or dorothea in datasets, types must include transcriptional
        # if dorothea in datasets, force levels (taken care of by defaults)
        
        # WIP - need to add conditionals here

        # Only if dorothea in datasets or transcriptional in types
        args['dorothea_levels'] = args['dorothea_levels'] or {'A', 'B'}

        
        dorothea_included = (
            'dorothea' in args['datasets'] or
            any(res.endswith('DoRothEA') for res in args['resources']) or
            (
                'transcriptional' in args['types'] and
                not args['datasets']
            )
        )

        if dorothea_included and args['dorothea_levels']:

            tbl = tbl.loc[
                self._dorothea_dataset_filter(tbl, args) |
                [
                    bool(levels & args['dorothea_levels'])
                    for levels in tbl.set_dorothea_level
                ]
            ]

        # WHERE ... (collectri) AND (dorothea_level IN ARRAY['A', 'B'] OR NOT dorothea)

        if dorothea_included and args['dorothea_methods']:

            q = ['dorothea_%s' % m for m in args['dorothea_methods']]

            tbl = tbl.loc[
                self._dorothea_dataset_filter(tbl, args) |
                tbl[q].any(1)
            ]


    # Fix dorothea levels
    def interactions( # TODO: entity_types, evidences?, extra_attrs?
            self,
            resources: list[str] | None = None,
            partners: list[str] | None = None,
            sources: list[str] | None = None,
            targets: list[str] | None = None,
            fields: list[str] | None = None,
            limit: int | None = None,
            format: FORMATS | None = None,
            source_target: Literal['OR', 'AND'] = 'OR',
            organisms: Collection[str | int] | None  = None,
            datasets: Collection | None  = None,
            dorothea_levels: Collection | None = None,
            dorothea_methods: Collection | None = None,
            types: Collection | None = None,
            directed: bool = True,
            signed: bool = None,
            loops: bool = False,
            entity_types: Collection[ENTITY_TYPES.__args__] | None = None,
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
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Returns:
            Generator of the search results in the interactions database in the
            requested format.
        """

        datasets = datasets if types else datasets or {'omnipath'}
        organisms = organisms or {9606}
        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'interactions')
        extra_where = self._where_partners('interactions', args)
        where_loops = self._where_loops('interactions', args)
        where_bool = self._where_bool('interactions', args)

        _log(f'Args: {_misc.dict_str(args)}')
        _log(f'Interactions where: {extra_where}, {where_bool}, {where_loops}')

        yield from self._request(
            args,
            query_type = 'interactions',
            extra_where = [extra_where, where_bool, where_loops],
            **kwargs,
        )


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

        bool_args = self.query_param[query_type].get('where_bool', {})
        columns = self._columns(query_type)

        where = []

        for arg, cols in bool_args.items():

            arg_cols = _misc.to_set(args.get(arg, set()))

            if isinstance(cols, dict):

                arg_cols = {cols.get(x, x) for x in arg_cols}
                cols = set(cols.values())

            if (cols := arg_cols & cols):

                where.append(or_(*( columns[col] for col in cols)))

        return and_(*where)


    # XXX: Deprecated
    def old_interactions(
            self,
            req,
            datasets  = {'omnipath'},
            databases = None,
            dorothea_levels = {'A', 'B'},
            organisms = {9606},
            source_target = 'OR',
    ):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        hdr = [
            'source',
            'target',
            'is_directed',
            'is_stimulation',
            'is_inhibition',
            'consensus_direction',
            'consensus_stimulation',
            'consensus_inhibition',
        ]

        if b'source_target' in req.args:

            source_target = (
                req.args['source_target'][0].decode('utf-8').upper()
            )

        if b'databases' in req.args:

            req.args['resources'] = req.args['databases']

        args = {}

        for arg in (
            'datasets',
            'types',
            'sources',
            'targets',
            'partners',
            'resources',
            'organisms',
            'dorothea_levels',
            'dorothea_methods',
        ):

            args[arg] = self._args_set(req, arg)

        # here adjust on the defaults otherwise we serve empty
        # response by default
        if not args['types']:

            args['datasets'] = args['datasets'] or datasets

        # keep only valid dataset names
        args['datasets'] = args['datasets'] & self.datasets_

        args['organisms'] = {
            int(t) for t in args['organisms'] if t.isdigit()
        }
        args['organisms'] = args['organisms'] or organisms

        # do not allow impossible values
        # those would result KeyError later
        args['dorothea_levels'] = (
            args['dorothea_levels'] or
            dorothea_levels
        )
        args['dorothea_methods'] = (
            args['dorothea_methods'] & self.dorothea_methods
        )

        # provide genesymbols: yes or no
        if (
            b'genesymbols' in req.args and
            self._parse_bool_arg(req.args['genesymbols'])
        ):
            genesymbols = True
            hdr.insert(2, 'source_genesymbol')
            hdr.insert(3, 'target_genesymbol')
        else:
            genesymbols = False

        _log('Processed arguments: [%s].' % _misc.dict_str(args))

        # starting from the entire dataset
        tbl = self.data['interactions']

        # filter by type
        if args['types']:

            tbl = tbl.loc[tbl.type.isin(args['types'])]

        # if partners provided those will overwrite
        # sources and targets
        args['sources'] = args['sources'] or args['partners']
        args['targets'] = args['targets'] or args['partners']

        # then we filter by source and target
        # which matched against both standard names
        # and gene symbols
        if args['sources'] and args['targets'] and source_target == 'OR':

            tbl = tbl.loc[
                tbl.target.isin(args['targets']) |
                tbl.target_genesymbol.isin(args['targets']) |
                tbl.source.isin(args['sources']) |
                tbl.source_genesymbol.isin(args['sources'])
            ]

        else:

            if args['sources']:
                tbl = tbl.loc[
                    tbl.source.isin(args['sources']) |
                    tbl.source_genesymbol.isin(args['sources'])
                ]

            if args['targets']:
                tbl = tbl.loc[
                    tbl.target.isin(args['targets']) |
                    tbl.target_genesymbol.isin(args['targets'])
                ]

        # filter by datasets
        if args['datasets']:

            tbl = tbl.query(' or '.join(args['datasets']))

        # filter by organism
        tbl = tbl.loc[
            tbl.ncbi_tax_id_source.isin(args['organisms']) |
            tbl.ncbi_tax_id_target.isin(args['organisms'])
        ]

        dorothea_included = (
            'dorothea' in args['datasets'] or
            any(res.endswith('DoRothEA') for res in args['resources']) or
            (
                'transcriptional' in args['types'] and
                not args['datasets']
            )
        )

        # filter by DoRothEA confidence levels
        if dorothea_included and args['dorothea_levels']:

            tbl = tbl.loc[
                self._dorothea_dataset_filter(tbl, args) |
                [
                    bool(levels & args['dorothea_levels'])
                    for levels in tbl.set_dorothea_level
                ]
            ]

        # filter by databases
        if args['resources']:

            tbl = tbl.loc[
                [
                    bool(sources & args['resources'])
                    for sources in tbl.set_sources
                ]
            ]

         # filtering for entity types
        if b'entity_types' in req.args:

            entity_types = self._args_set(req, 'entity_types')

            if len(entity_types) == 1 and 'protein' in entity_types:

                # pandas is awful:
                tbl = tbl.loc[
                    np.logical_and(
                        tbl.entity_type_source.astype('string') == 'protein',
                        tbl.entity_type_target.astype('string') == 'protein',
                    )
                ]

            else:

                tbl = tbl.loc[
                    tbl.entity_type_source.isin(entity_types) |
                    tbl.entity_type_target.isin(entity_types)
                ]

        # filtering by DoRothEA methods
        if dorothea_included and args['dorothea_methods']:

            q = ['dorothea_%s' % m for m in args['dorothea_methods']]

            tbl = tbl.loc[
                self._dorothea_dataset_filter(tbl, args) |
                tbl[q].any(1)
            ]

        # filter directed & signed
        if (
            b'directed' not in req.args or
            self._parse_bool_arg(req.args['directed'])
        ):

            tbl = tbl.loc[tbl.is_directed == 1]

        if (
            b'signed' in req.args and
            self._parse_bool_arg(req.args['signed'])
        ):

            tbl = tbl.loc[
                np.logical_or(
                    tbl.is_stimulation == 1,
                    tbl.is_inhibition == 1,
                )
            ]

        # loops: remove by default
        if (
            b'loops' not in req.args or
            not self._parse_bool_arg(req.args['loops'])
        ):

            # pandas is a disaster:
            tbl = tbl.loc[
                tbl.source.astype('string') !=
                tbl.target.astype('string')
            ]

        req.args['fields'] = req.args['fields'] or [b'']

        _fields = [
            f for f in
            req.args['fields'][0].decode('utf-8').split(',')
            if f in self.interaction_fields
        ]

        for f in (b'evidences', b'extra_attrs'):

            if f in req.uri and f not in req.args['fields'][0]:

                _fields.append(f.decode('utf-8'))

        for f in _fields:

            if f == 'ncbi_tax_id' or f == 'organism':

                hdr.append('ncbi_tax_id_source')
                hdr.append('ncbi_tax_id_target')

            elif f == 'entity_type':

                hdr.append('entity_type_source')
                hdr.append('entity_type_target')

            elif f in {'databases', 'resources'}:

                hdr.append('sources')

            elif f == 'datasets':

                hdr.extend(
                    set(tbl.columns) &
                    self.args_reference['interactions']['datasets'] &
                    args['datasets'],
                )

            else:

                hdr.append(f)

        license = self._get_license(req)

        tbl = self._filter_by_license_interactions(tbl, license)

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


    # XXX: Deprecated?
    @classmethod
    def _dataset_included(cls, dataset: str, args: dict) -> bool:


        return (
            dataset in args['datasets'] or
            (
                not args['datasets'] and
                cls.dataset2type.get(dataset, None) in args['types']
            )
        )


    # XXX: Deprecated?
    @classmethod
    def _dorothea_dataset_filter(cls, tbl: pd.DataFrame, args: dict):

        return (
            (
                # if the tf_target dataset is requested
                # we need to serve it including the parts which
                # don't fit the filters below
                cls._dataset_included('tf_target', args) &
                tbl.tf_target
            ) |
            (
                cls._dataset_included('collectri', args) &
                tbl.collectri
            ) |
            (tbl.type != 'transcriptional')
        )


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
            **kwargs:
                Keyword arguments passed to the `_request` method.

        Returns:
            Generator of the search results in the enzsub database in the
            requested format.
        """

        organisms = organisms or {9606}
        args = locals()
        args = self._clean_args(args)
        args = self._array_args(args, 'enzsub')
        where_loops = self._where_loops('enzsub', args)
        extra_where = self._where_partners('enzsub', args)

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


    def query(self, query_type: QUERY_TYPES, **kwargs) -> Query:
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


    def query_str(self, query_type: QUERY_TYPES, **kwargs) -> str:
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

        q_str = str(self.query(query_type, **kwargs))

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

        Returns:
            Generator of the search results in the annotations database in the
            requested format.
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


    # XXX: Deprecated?
    def annotations_summary(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args['resources'] = req.args['databases']

        # starting from the entire dataset
        tbl = self.data['annotations_summary']

        hdr = tbl.columns

        # filtering for resources
        if b'resources' in req.args:

            resources = self._args_set(req, 'resources')

            tbl = tbl.loc[tbl.source.isin(resources)]

        if (
            b'cytoscape' in req.args and
            self._parse_bool_arg(req.args['cytoscape'])
        ):

            cytoscape = True

        else:

            cytoscape = False

        tbl = tbl.loc[:,hdr]

        if cytoscape:

            tbl = tbl.set_index(['source', 'label'], drop = False)

            cytoscape_keys = {
                (source, label)
                for source, labels in self.cytoscape_attributes
                for label in (
                    labels if isinstance(labels, tuple) else (labels,)
                )
            } & set(tbl.index)

            tbl = tbl.loc[list(cytoscape_keys)]

        return self._serve_dataframe(tbl, req)


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

        Returns:
            Generator of the search results in the intercell database in the
            requested format.
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


    # XXX: Deprecated
    def intercell_old(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args['resources'] = req.args['databases']


        # starting from the entire dataset
        tbl = self.data['intercell']

        hdr = tbl.columns

        # filtering for category types
        for var in (
            'aspect',
            'source',
            'scope',
            'parent',
            'resources',
        ):

            if var.encode('ascii') in req.args:

                values = self._args_set(req, var)

                if var in {'resources', 'databases'}:

                    var = 'database'

                tbl = tbl.loc[getattr(tbl, var).isin(values)]

        for (_long, short) in (
            ('transmitter', 'trans'),
            ('receiver', 'rec'),
            ('secreted', 'sec'),
            ('plasma_membrane_peripheral', 'pmp'),
            ('plasma_membrane_transmembrane', 'pmtm'),
        ):

            this_arg = None
            _long_b = _long.encode('ascii')
            short_b = short.encode('ascii')

            if _long_b in req.args:

                this_arg = self._parse_bool_arg(req.args[_long_b])

            elif short_b in req.args:

                this_arg = self._parse_bool_arg(req.args[short_b])

            if this_arg is not None:

                tbl = tbl.loc[getattr(tbl, _long) == this_arg]

        if b'causality' in req.args:

            causality = self._args_set(req, 'causality')

            trans = causality & {'transmitter', 'trans', 'both'}
            rec = causality & {'receiver', 'rec', 'both'}
            tbl = (
                tbl.loc[tbl.transmitter | tbl.receiver]
                    if trans and rec else
                tbl.loc[tbl.transmitter]
                    if trans else
                tbl.loc[tbl.receiver]
                    if rec else
                tbl
            )

        if b'topology' in req.args:

            topology = self._args_set(req, 'topology')
            query = ' or '.join(
                colname
                for enabled, colname in
                (
                    (topology & {'secreted', 'sec'}, 'secreted'),
                    (
                        topology & {'plasma_membrane_peripheral', 'pmp'},
                        'plasma_membrane_peripheral',
                    ),
                    (
                        topology & {'plasma_membrane_transmembrane', 'pmtm'},
                        'plasma_membrane_transmembrane',
                    ),
                )
                if enabled
            )

            if query:

                tbl = tbl.query(query)

        # filtering for categories
        if b'categories' in req.args:

            categories = self._args_set(req, 'categories')

            tbl = tbl.loc[tbl.category.isin(categories)]

        # filtering for entity types
        if b'entity_types' in req.args:

            entity_types = self._args_set(req, 'entity_types')

            tbl = tbl.loc[tbl.entity_type.isin(entity_types)]

        # filtering for proteins
        if b'proteins' in req.args:

            proteins = self._args_set(req, 'proteins')

            tbl = tbl.loc[
                np.logical_or(
                    tbl.uniprot.isin(proteins),
                    tbl.genesymbol.isin(proteins),
                )
            ]

        license = self._get_license(req)

        tbl = self._filter_by_license_intercell(tbl, license)

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


    # XXX: Deprecated?
    def intercell_summary(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args['resources'] = req.args['databases']

        # starting from the entire dataset
        tbl = self.data['intercell_summary']

        hdr = tbl.columns

        # filtering for category types
        for var in (
            'aspect',
            'source',
            'scope',
            'transmitter',
            'receiver',
            'parent',
            'resources',
        ):

            if var.encode('ascii') in req.args:

                values = self._args_set(req, var)

                tbl = tbl.loc[getattr(tbl, var).isin(values)]

        # filtering for categories
        if b'categories' in req.args:

            categories = self._args_set(req, 'categories')

            tbl = tbl.loc[tbl.category.isin(categories)]

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


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

        Returns:
            Generator of the search results in the complexes database in the
            requested format.
        """

        args = locals()

        yield from self._request(args, 'complexes', **kwargs)


    # XXX: Deprecated?
    def resources(self, req):

        datasets = (

            {
                self._query_type(dataset.decode('ascii'))
                for dataset in req.args['datasets']
            }

            if b'datasets' in req.args else

            None

        )

        res_ctrl = resources_mod.get_controller()
        license = self._get_license(req)

        return json.dumps(
            {
                k: v
                for k, v in self._resources_dict.items()
                if (
                    res_ctrl.license(k).enables(license) and
                    (
                        not datasets or
                        datasets & set(v['datasets'].keys())
                    )
                )
            },
        )


    # XXX: Deprecated?
    @classmethod
    def _filter_by_license_complexes(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'sources',
            simple = False,
            prefix_col = 'identifiers',
        )

    
    # XXX: Deprecated?
    @classmethod
    def _filter_by_license_interactions(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'sources',
            simple = False,
            prefix_col = 'references',
        )


    # XXX: Deprecated?
    @classmethod
    def _filter_by_license_annotations(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'source',
            simple = True,
        )


    # XXX: Deprecated?
    @classmethod
    def _filter_by_license_intercell(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'database',
            simple = True,
        )


    # XXX: Deprecated?
    @staticmethod
    def _filter_by_license(
            tbl,
            license,
            res_col,
            simple = False,
            prefix_col = None,
    ):

        def filter_resources(res):

            res = {
                r for r in res
                if res_ctrl.license(r).enables(license)
            }

            composite = [
                r for r in res
                if res_ctrl.license(r).name == 'Composite'
            ]

            if composite:

                composite_to_remove = {
                    comp_res
                    for comp_res in composite
                    if not res_ctrl.secondary_resources(comp_res, True) & res
                }

                res = res - composite_to_remove

            return res


        if license == LICENSE_IGNORE or tbl.shape[0] == 0:

            return tbl

        res_ctrl = resources_mod.get_controller()

        _res_col = getattr(tbl, res_col)

        if simple:

            bool_idx = [
                res_ctrl.license(res).enables(license)
                for res in _res_col
            ]

        else:

            _set_res_col = tbl.set_sources

            _res_to_keep = [
                filter_resources(ress)
                for ress in _set_res_col
            ]

            with ignore_pandas_copywarn():

                tbl[res_col] = [
                    ';'.join(sorted(ress))
                    for ress in _res_to_keep
                ]

            if prefix_col:

                _prefix_col = getattr(tbl, prefix_col)

                _new_prefix_col = [

                    ';'.join(
                        sorted(
                            pref_res
                            for pref_res in pref_ress.split(';')
                            if (
                                pref_res.split(':', maxsplit = 1)[0] in
                                _res_to_keep[i]
                            )
                        ),
                    )

                        if isinstance(pref_ress, str) else

                    pref_ress

                    for i, pref_ress in enumerate(_prefix_col)
                ]

                with ignore_pandas_copywarn():

                    tbl[prefix_col] = _new_prefix_col

            bool_idx = [bool(res) for res in tbl[res_col]]

        tbl = tbl.loc[bool_idx]

        return tbl


    # XXX: Deprecated?
    @classmethod
    def _serve_dataframe(cls, tbl, req):

        if b'limit' in req.args:

            limit = req.args['limit'][0].decode('utf-8')

            if limit.isdigit():

                limit = int(limit)
                tbl = tbl.head(limit)

        if b'format' in req.args and req.args['format'][0] == b'json':

            data_json = tbl.to_json(orient = 'records')
            # this is necessary because in the data frame we keep lists
            # as `;` separated strings but in json is nicer to serve
            # them as lists
            data_json = json.loads(data_json)

            for i in data_json:

                for k, v in i.items():

                    if k in cls.list_fields:

                        i[k] = (
                            [
                                (
                                    int(f)
                                    if (
                                        k in cls.int_list_fields and
                                        f.isdigit()
                                    ) else
                                    f
                                )
                                for f in v.split(';')
                            ]
                            if isinstance(v, str) else
                            []
                        )

            return json.dumps(data_json)

        else:

            return tbl.to_csv(
                sep = '\t',
                index = False,
                header = bool(req.args['header']),
                chunksize = 2e5,
            )


    def _parse_arg(self, arg: Any, typ: type = None) -> Any:
        """
        Arguments come as strings, here we parse them to the appropriate type.

        At least from the HTTP interface, we get them as strings. In case these
        come from elsewhere, and provided already as numeric or array types,
        this function simply passes them through.
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
        Normalize various representations of Boolean values.

        These can be 0 or 1, True or False, "true" or "false", "yes" or "no".
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

@contextlib.contextmanager
def ignore_pandas_copywarn():

    try:

        with warnings.catch_warnings():

            warnings.simplefilter('ignore', pd.errors.SettingWithCopyWarning)

            yield

    finally:

        pass


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
