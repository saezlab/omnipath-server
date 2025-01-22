#!/usr/bin/env python

#
# This file is part of the `omnipath_server` Python module
#
# Copyright 2024
# Heidelberg University Hospital
#
# File author(s): OmniPath Team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

import os
import re
import copy
import collections
import itertools
import json
import warnings
import contextlib

import numpy as np
import pandas as pd
from pypath_common import _misc
from pypath_common import _constants as _const
from pypath_common import _settings
from pypath import resources as resources_mod

from .. import _log, _connection
from ..schema import _legacy as _schema


LICENSE_IGNORE = 'ignore'

class LegacyService:

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
        'tfregulons_level': 'dorothea_level',
        'tfregulons_curated': 'dorothea_curated',
        'tfregulons_chipseq': 'dorothea_chipseq',
        'tfregulons_tfbs': 'dorothea_tfbs',
        'tfregulons_coexp': 'dorothea_coexp',
        'sources': 'resources',
        'databases': 'resources',
    }

    args_reference = {
        'interactions': {
            'header': None,
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table'
            },
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
                'tfregulons',
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
                'tfregulons_level',
                'tfregulons_curated',
                'tfregulons_chipseq',
                'tfregulons_tfbs',
                'tfregulons_coexp',
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
            'tfregulons_levels':  {'A', 'B', 'C', 'D', 'E'},
            'tfregulons_methods': {
                'curated',
                'chipseq',
                'coexp',
                'tfbs',
            },
            'dorothea_levels':  {'A', 'B', 'C', 'D', 'E'},
            'dorothea_methods': {
                'curated',
                'chipseq',
                'coexp',
                'tfbs',
            },
            'organisms': {
                '9606',
                '10090',
                '10116',
            },
            'source_target': {
                'AND',
                'OR',
                'and',
                'or',
            },
            'directed': _const.BOOLEAN_VALUES,
            'signed': _const.BOOLEAN_VALUES,
            'loops': _const.BOOLEAN_VALUES,
            'entity_types': {
                'protein',
                'complex',
                'mirna',
                'lncrna',
                'small_molecule',
                'drug',
                'metabolite',
                'lipid',
            },
        },
        'enzsub': {
            'header':      None,
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table',
            },
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
            'organisms': {
                '9606',
                '10090',
                '10116',
            },
            'databases': None,
            'resources': None,
            'residues':  None,
            'modification': None,
            'types': None,
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
            }
        },
        'annotations': {
            'header': None,
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table',
            },
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
            'entity_types': {
                'protein',
                'complex',
                'mirna',
                'lncrna',
                'small_molecule',
                'drug',
                'metabolite',
                'lipid',
            },
        },
        'annotations_summary': {
            'header': None,
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table',
            },
            'databases': None,
            'resources': None,
            'fields': None,
            'cytoscape': _const.BOOLEAN_VALUES,
        },
        'intercell': {
            'header': None,
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table',
            },
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
            'entity_types': {
                'protein',
                'complex',
                'mirna',
                'lncrna',
                'small_molecule',
                'drug',
                'metabolite',
                'lipid',
            },
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
                'both'
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
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table',
            },
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
            'format': {
                'json',
                'tab',
                'text',
                'tsv',
                'table',
            },
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
            'format': {
                'tab',
                'text',
                'tsv',
                'table',
                'json',
            },
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
        'tfregulons',
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
        'tfregulons': 'transcriptional',
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
        'tfregulons_level', 'tfregulons_curated',
        'tfregulons_chipseq', 'tfregulons_tfbs', 'tfregulons_coexp',
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
        }
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
            )
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
            )
        ),
        (
            'OPM',
            (
                'membrane',
                'family',
                #'transmembrane',
            )
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
        ('CPAD', ('pathway', 'effect_on_cancer', 'cancer', )),
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

    def __init__(
            self,
            input_files = None,
            only_tables = None,
            exclude_tables = None,
        ):
        """
        Server based on ``pandas`` data frames.

        :param dict input_files:
            Paths to tables exported by the ``pypath.websrvtab`` module.
        """

        _log('TableServer starting up.')

        self.input_files = copy.deepcopy(self.default_input_files)
        self.input_files.update(input_files or {})
        self.data = {}

        self.to_load = (
            self.data_query_types - _misc.to_set(exclude_tables)
                if only_tables is None else
            _misc.to_set(only_tables)
        )

        _log('Datasets to load: %s.' % (', '.join(sorted(self.to_load))))

        self._read_tables()

        self._preprocess_interactions()
        self._preprocess_enzsub()
        self._preprocess_annotations()
        self._preprocess_complexes()
        self._preprocess_intercell()
        self._update_resources()

        _log(f'{self.__class__.__name__} startup ready.')


    def _read_tables(self):

        _log('Loading data tables.')

        for name, fname in self.input_files.items():

            if name not in self.to_load:

                continue

            fname_gz = f'{fname}.gz'
            fname = fname_gz if os.path.exists(fname_gz) else fname

            _log('Loading dataset `%s` from file `%s`.' % (name, fname))

            if not os.path.exists(fname):

                _log(
                    'Missing table: `%s`.' % fname
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
                'Table `%s` loaded from file `%s`.' % (name, fname)
            )


    def _network(self, req):

        hdr = ['nodes', 'edges', 'is_directed', 'sources']
        tbl = self.data['network'].field
        val = dict(zip(tbl.field, tbl.value))

        if b'format' in req.args and req.args[b'format'] == b'json':
            return json.dumps(val)
        else:
            return '%s\n%s' % ('\t'.join(hdr), '\t'.join(
                [str(val[h]) for h in hdr]))


    def _preprocess_interactions(self):

        if 'interactions' not in self.data:

            return

        _log('Preprocessing interactions.')
        tbl = self.data['interactions']
        tbl['set_sources'] = pd.Series(
            [set(s.split(';')) for s in tbl.sources]
        )
        tbl['set_dorothea_level'] = pd.Series(
            [
                set(s.split(';'))
                if not pd.isnull(s) else
                set([])
                for s in tbl.dorothea_level
            ]
        )


    def _preprocess_enzsub(self):

        if 'enzsub' not in self.data:

            return

        _log('Preprocessing enzyme-substrate relationships.')
        tbl = self.data['enzsub']
        tbl['set_sources'] = pd.Series(
            [set(s.split(';')) for s in tbl.sources]
        )


    def _preprocess_complexes(self):

        if 'complexes' not in self.data:

            return

        _log('Preprocessing complexes.')
        tbl = self.data['complexes']

        tbl = tbl[~tbl.components.isna()]

        with ignore_pandas_copywarn():

            tbl['set_sources'] = [set(s.split(';')) for s in tbl.sources]
            tbl['set_proteins'] = [set(c.split('_')) for c in tbl.components]

        self.data['complexes'] = tbl


    def _preprocess_annotations_old(self):

        if 'annotations' not in self.data:

            return

        renum = re.compile(r'[-\d\.]+')


        def _agg_values(vals):

            result = (
                '#'.join(sorted(set(str(ii) for ii in vals)))
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


    def _preprocess_intercell(self):

        if 'intercell' not in self.data:

            return

        _log('Preprocessing intercell data.')
        tbl = self.data['intercell']
        tbl.drop('full_name', axis = 1, inplace = True, errors = 'ignore')
        self.data['intercell_summary'] = tbl.filter(
            ['category', 'parent', 'database'],
        ).drop_duplicates()


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
                ('category', 'categories')
            ):

                if colname in tbl.columns:

                    break

            # collecting all resource names
            values = sorted(set(
                itertools.chain(*(
                    val.split(';') for val in getattr(tbl, colname)
                ))
            ))

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
                                set(tbl_db.category)
                            ),
                        }

                    else:

                        self._resources_dict[db]['queries'][query_type] = {}

            self.args_reference[query_type][argname] = values

        self._resources_dict = dict(self._resources_dict)

        _log('Finished updating resource information.')


    def _check_args(self, req):

        result = []
        argname = req.postpath[0]
        ref = (
            self.args_reference['resources']
                if argname == 'databases' else
            self.args_reference[argname]
        )

        for arg, val in req.args.items():

            arg = arg.decode('utf-8')

            if arg in ref:

                if not ref[arg] or not val:

                    continue

                val = (
                    {val[0]}
                    if type(val[0]) is int else
                    set(val[0].decode('utf-8').split(','))
                )

                unknowns = val - set(ref[arg])

                if unknowns:

                    result.append(
                        ' ==> Unknown values for argument `%s`: `%s`' % (
                            arg,
                            ', '.join(str(u) for u in unknowns)
                        )
                    )

            else:

                result.append(' ==> Unknown argument: `%s`' % arg)

        req.args[b'header'] = self._parse_arg(req.args[b'header'])

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

            result = dict(
                (
                    k,
                    sorted(v) if isinstance(v, _const.LIST_LIKE) else v
                )
                for k, v in self.args_reference[query_type].items()
            )

            if query_param is not None and query_param in result:

                result = {query_param: result[query_param]}

        else:

            result = {}
            result[query_type] = (
                'No possible arguments defined for'
                'query `%s` or no such query available.' % query_type
            )

        result = self._dict_set_to_list(result)

        if b'format' in req.args and req.args[b'format'][0] == b'json':

            return json.dumps(result)

        else:

            return 'argument\tvalues\n%s' % '\n'.join(
                '%s\t%s' % (
                    k,
                    ';'.join(v)
                        if isinstance(v, (list, set, tuple)) else
                    str(v)
                )
                for k, v in result.items()
            )


    @classmethod
    def _dict_set_to_list(cls, dct):

        return dict(
            (
                key,
                (
                    sorted(val)
                        if isinstance(val, _const.LIST_LIKE) else
                    cls._dict_set_to_list(val)
                        if isinstance(val, dict) else
                    val
                )
            )
            for key, val in dct.items()
        )


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

                result[dataset] = sorted(set.union(
                    *tbl[tbl.type == dataset].set_sources)
                )

        else:

            result = {}
            result['*'] = sorted(set.union(*tbl.set_sources))

        if b'format' in req.args and req.args[b'format'][0] == b'json':

            return json.dumps(result)

        else:

            return 'dataset\tresources\n%s' % '\n'.join(
                '%s\t%s' % (k, ';'.join(v)) for k, v in result.items()
            )


    def _get_datasets(self):

        return list(self.data['interactions'].type.unique())


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

        if b'format' in req.args and req.args[b'format'][0] == b'json':

            return json.dumps(result)

        else:

            return ';'.join(result)


    def interactions(
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
                req.args[b'source_target'][0].decode('utf-8').upper()
            )

        # changes the old, "tfregulons" names to new "dorothea"
        self._tfregulons_dorothea(req)

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']

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

        args['organisms'] = set(
            int(t) for t in args['organisms'] if t.isdigit()
        )
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
            self._parse_arg(req.args[b'genesymbols'])
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
            self._parse_arg(req.args[b'directed'])
        ):

            tbl = tbl.loc[tbl.is_directed == 1]

        if (
            b'signed' in req.args and
            self._parse_arg(req.args[b'signed'])
        ):

            tbl = tbl.loc[np.logical_or(
                tbl.is_stimulation == 1,
                tbl.is_inhibition == 1
            )]

        # loops: remove by default
        if (
            b'loops' not in req.args or
            not self._parse_arg(req.args[b'loops'])
        ):

            # pandas is a disaster:
            tbl = tbl.loc[
                tbl.source.astype('string') !=
                tbl.target.astype('string')
            ]

        req.args[b'fields'] = req.args[b'fields'] or [b'']

        _fields = [
            f for f in
            req.args[b'fields'][0].decode('utf-8').split(',')
            if f in self.interaction_fields
        ]

        for f in (b'evidences', b'extra_attrs'):

            if f in req.uri and f not in req.args[b'fields'][0]:

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
                    args['datasets']
                )

            else:

                hdr.append(f)

        license = self._get_license(req)

        tbl = self._filter_by_license_interactions(tbl, license)

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


    @classmethod
    def _dataset_included(cls, dataset: str, args: dict) -> bool:

        return (
            dataset in args['datasets'] or
            (
                not args['datasets'] and
                cls.dataset2type.get(dataset, None) in args['types']
            )
        )


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


    def _tfregulons_dorothea(self, req):

        for arg in (b'datasets', b'fields'):

            if arg in req.args:

                req.args[arg] = [
                    it.replace(b'tfregulons', b'dorothea')
                    for it in req.args[arg]
                ]

        for postfix in (b'levels', b'methods'):

            key = b'tfregulons_%s' % postfix
            new_key = b'dorothea_%s' % postfix

            if key in req.args and new_key not in req.args:

                req.args[new_key] = req.args[key]
                _ = req.args.pop(key)


    def enzsub(
            self,
            req,
            organisms = {9606},
            enzyme_substrate = 'OR'
        ):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        hdr = [
            'enzyme', 'substrate', 'residue_type',
            'residue_offset', 'modification'
        ]

        if b'enzyme_substrate' in req.args:

            enzyme_substrate = (
                req.args[b'enzyme_substrate'][0].decode('utf-8').upper()
            )

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']

        args = {}

        for arg in (
            'enzymes', 'substrates', 'partners',
            'resources', 'organisms', 'types',
            'residues'
        ):

            args[arg] = self._args_set(req, arg)

        args['organisms'] = set(
            int(t) for t in args['organisms'] if t.isdigit()
        )
        args['organisms'] = args['organisms'] or organisms

        # provide genesymbols: yes or no
        if (
            b'genesymbols' in req.args and
            self._parse_arg(req.args[b'genesymbols'])
        ):
            genesymbols = True
            hdr.insert(2, 'enzyme_genesymbol')
            hdr.insert(3, 'substrate_genesymbol')
        else:
            genesymbols = False

        # starting from the entire dataset
        tbl = self.data['enzsub']

        # filter by type
        if args['types']:
            tbl = tbl.loc[tbl.modification.isin(args['types'])]

        # if partners provided those will overwrite
        # enzymes and substrates
        args['enzymes'] = args['enzymes'] or args['partners']
        args['substrates'] = args['substrates'] or args['partners']

        # then we filter by enzyme and substrate
        # which matched against both standard names
        # and gene symbols
        if (
            args['enzymes'] and
            args['substrates'] and
            enzyme_substrate == 'OR'
        ):

            tbl = tbl.loc[
                tbl.substrate.isin(args['substrates']) |
                tbl.substrate_genesymbol.isin(args['substrates']) |
                tbl.enzyme.isin(args['enzymes']) |
                tbl.enzyme_genesymbol.isin(args['enzymes'])
            ]

        else:

            if args['enzymes']:
                tbl = tbl.loc[
                    tbl.enzyme.isin(args['enzymes']) |
                    tbl.enzyme_genesymbol.isin(args['enzymes'])
                ]

            if args['substrates']:
                tbl = tbl.loc[
                    tbl.substrate.isin(args['substrates']) |
                    tbl.substrate_genesymbol.isin(args['substrates'])
                ]

        # filter by organism
        tbl = tbl.loc[tbl.ncbi_tax_id.isin(args['organisms'])]

        # filter by databases
        if args['resources']:

            tbl = tbl.loc[
                [
                    bool(args['resources'] & sources)
                    for sources in tbl.set_sources
                ]
            ]

        if req.args[b'fields']:

            _fields = [
                f for f in
                req.args[b'fields'][0].decode('utf-8').split(',')
                if f in self.enzsub_fields
            ]

            for f in _fields:

                if f == 'ncbi_tax_id' or f == 'organism':

                    hdr.append('ncbi_tax_id')

                elif f in {'databases', 'resources'}:

                    hdr.append('sources')

                else:

                    hdr.append(f)

        license = self._get_license(req)

        tbl = self._filter_by_license_interactions(tbl, license)

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


    def ptms(self, req):

        req.postpath[0] = 'enzsub'

        return self.enzsub(req)


    def enz_sub(self, req):

        req.postpath[0] = 'enzsub'

        return self.enzsub(req)


    def annotations(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']

        if (
            not _settings.get('server_annotations_full_download') and
            not b'resources' in req.args and
            not b'proteins' in req.args
        ):

            return (
                'Downloading the entire annotations database by the REST '
                'API is not allowed because of its huge size (>1GB). '
                'We recommend to query a set of proteins or a few '
                'resources, depending on your interest. '
                'You can always download the full database from '
                'https://archive.omnipathdb.org/'
                'omnipath_webservice_annotations__recent.tsv'
            )

        # starting from the entire dataset
        tbl = self.data['annotations']

        hdr = tbl.columns

        # filtering for resources
        if b'resources' in req.args:

            resources = self._args_set(req, 'resources')

            tbl = tbl.loc[tbl.source.isin(resources)]

        # filtering for entity types
        if b'entity_types' in req.args:

            entity_types = self._args_set(req, 'entity_types')

            tbl = tbl.loc[tbl.entity_type.isin(entity_types)]

        # filtering for proteins
        if b'proteins' in req.args:

            proteins = self._args_set(req, 'proteins')

            tbl = tbl.loc[
                tbl.uniprot.isin(proteins) |
                tbl.genesymbol.isin(proteins)
            ]

        # provide genesymbols: yes or no
        if (
            b'genesymbols' in req.args and
            self._parse_arg(req.args[b'genesymbols'])
        ):
            genesymbols = True
            hdr.insert(1, 'genesymbol')
        else:
            genesymbols = False

        license = self._get_license(req)

        tbl = self._filter_by_license_annotations(tbl, license)

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


    def annotations_summary(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']

        # starting from the entire dataset
        tbl = self.data['annotations_summary']

        hdr = tbl.columns

        # filtering for resources
        if b'resources' in req.args:

            resources = self._args_set(req, 'resources')

            tbl = tbl.loc[tbl.source.isin(resources)]

        if (
            b'cytoscape' in req.args and
            self._parse_arg(req.args[b'cytoscape'])
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


    def intercell(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']


        # starting from the entire dataset
        tbl = self.data['intercell']

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

                this_arg = self._parse_arg(req.args[_long_b])

            elif short_b in req.args:

                this_arg = self._parse_arg(req.args[short_b])

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
                        'plasma_membrane_peripheral'
                    ),
                    (
                        topology & {'plasma_membrane_transmembrane', 'pmtm'},
                        'plasma_membrane_transmembrane'
                    )
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


    def intercell_summary(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']

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


    def complexes(self, req):

        bad_req = self._check_args(req)

        if bad_req:

            return bad_req

        if b'databases' in req.args:

            req.args[b'resources'] = req.args[b'databases']

        # starting from the entire dataset
        tbl = self.data['complexes']

        hdr = list(tbl.columns)
        hdr.remove('set_sources')
        hdr.remove('set_proteins')

        # filtering for resources
        if b'resources' in req.args:

            resources = self._args_set(req, 'resources')

            tbl = tbl.loc[
                [
                    bool(sources & resources)
                    for sources in tbl.set_sources
                ]
            ]

        # filtering for proteins
        if b'proteins' in req.args:

            proteins = self._args_set(req, 'proteins')

            tbl = tbl.loc[
                [
                    bool(this_proteins & proteins)
                    for this_proteins in tbl.set_proteins
                ]
            ]

        license = self._get_license(req)

        tbl = self._filter_by_license_complexes(tbl, license)

        tbl = tbl.loc[:,hdr]

        return self._serve_dataframe(tbl, req)


    def resources(self, req):

        datasets = (

            {
                self._query_type(dataset.decode('ascii'))
                for dataset in req.args[b'datasets']
            }

            if b'datasets' in req.args else

            None

        )

        res_ctrl = resources_mod.get_controller()
        license = self._get_license(req)

        return json.dumps(
            dict(
                (k, v)
                for k, v in self._resources_dict.items()
                if (
                    res_ctrl.license(k).enables(license) and
                    (
                        not datasets or
                        datasets & set(v['datasets'].keys())
                    )
                )
            )
        )



    @staticmethod
    def _get_license(req):

        return req.args[b'license'][0].decode('utf-8')


    @classmethod
    def _filter_by_license_complexes(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'sources',
            simple = False,
            prefix_col = 'identifiers',
        )


    @classmethod
    def _filter_by_license_interactions(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'sources',
            simple = False,
            prefix_col = 'references',
        )


    @classmethod
    def _filter_by_license_annotations(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'source',
            simple = True,
        )


    @classmethod
    def _filter_by_license_intercell(cls, tbl, license):

        return cls._filter_by_license(
            tbl = tbl,
            license = license,
            res_col = 'database',
            simple = True,
        )


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

                    ';'.join(sorted(
                        pref_res
                        for pref_res in pref_ress.split(';')
                        if (
                            pref_res.split(':', maxsplit = 1)[0] in
                            _res_to_keep[i]
                        )
                    ))

                        if isinstance(pref_ress, str) else

                    pref_ress

                    for i, pref_ress in enumerate(_prefix_col)
                ]

                with ignore_pandas_copywarn():

                    tbl[prefix_col] = _new_prefix_col

            bool_idx = [bool(res) for res in tbl[res_col]]

        tbl = tbl.loc[bool_idx]

        return tbl


    @classmethod
    def _serve_dataframe(cls, tbl, req):

        if b'limit' in req.args:

            limit = req.args[b'limit'][0].decode('utf-8')

            if limit.isdigit():

                limit = int(limit)
                tbl = tbl.head(limit)

        if b'format' in req.args and req.args[b'format'][0] == b'json':

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
                header = bool(req.args[b'header']),
                chunksize = 2e5,
            )


    @staticmethod
    def _args_set(req, arg):

        arg = arg.encode('utf-8')

        return (
            set(req.args[arg][0].decode('utf-8').split(','))
            if arg in req.args
            else set()
        )

@contextlib.contextmanager
def ignore_pandas_copywarn():

    try:

        with warnings.catch_warnings():

            warnings.simplefilter('ignore', pd.errors.SettingWithCopyWarning)

            yield

    finally:

        pass
