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

from sqlalchemy import ARRAY, Index, Column, String, Boolean, Integer
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

__all__ = [
    'Annotations',
    'Base',
    'Complexes',
    'Enzsub',
    'Interactions',
    'Intercell',
    'Licenses',
]

Base = declarative_base()

class Annotations(Base):
    """
    Definition for the `annotations` table columns and types.
    """

    __tablename__ = 'annotations'
    id = Column(Integer, primary_key = True)
    uniprot = Column(String)
    genesymbol = Column(String)
    entity_type = Column(String)
    source = Column(String)
    label = Column(String)
    value = Column(String)
    record_id = Column(Integer)
    #  The service filters annotations by protein (uniprot/genesymbol) and source.
    __table_args__ = (
        Index('ix_annotations_uniprot', 'uniprot'),
        Index('ix_annotations_genesymbol', 'genesymbol'),
        Index('ix_annotations_source', 'source'),
    )


class Complexes(Base):
    """
    Definition for the `complexes` table columns and types.
    """

    __tablename__ = 'complexes'
    _array_sep = {'components': '_', 'components_genesymbols': '_'}
    id = Column(Integer, primary_key = True)
    name = Column(String)
    components = Column(ARRAY(String))
    components_genesymbols = Column(ARRAY(String))
    stoichiometry = Column(String)
    sources = Column(ARRAY(String))
    references = Column(String)  # Could be array
    identifiers = Column(String)  # Could be array


class Enzsub(Base):
    """
    Definition for the `enzyme-substrate` table columns and types.
    """

    __tablename__ = 'enzsub'
    id = Column(Integer, primary_key = True)
    enzyme = Column(String)
    enzyme_genesymbol = Column(String)
    substrate = Column(String)
    substrate_genesymbol = Column(String)
    isoforms = Column(String)
    residue_type = Column(String)
    residue_offset = Column(Integer)
    modification = Column(String)
    sources = Column(ARRAY(String))
    references = Column(String)
    curation_effort = Column(Integer)
    ncbi_tax_id = Column(Integer)


class Interactions(Base):
    """
    Definition for the `interactions` table columns and types.
    """

    __tablename__ = 'interactions'
    id = Column(Integer, primary_key = True)
    source = Column(String, nullable = True)
    target = Column(String, nullable = True)
    source_genesymbol = Column(String)
    target_genesymbol = Column(String)
    is_directed = Column(Boolean)
    is_stimulation = Column(Boolean)
    is_inhibition = Column(Boolean)
    consensus_direction = Column(Boolean)
    consensus_stimulation = Column(Boolean)
    consensus_inhibition = Column(Boolean)
    sources = Column(ARRAY(String))
    references = Column(String)
    omnipath = Column(Boolean)
    kinaseextra = Column(Boolean)
    ligrecextra = Column(Boolean)
    pathwayextra = Column(Boolean)
    mirnatarget = Column(Boolean)
    dorothea = Column(Boolean)
    collectri = Column(Boolean)
    collectri2 = Column(Boolean)
    tf_target = Column(Boolean)
    lncrna_mrna = Column(Boolean)
    tf_mirna = Column(Boolean)
    small_molecule = Column(Boolean)
    dorothea_curated = Column(Boolean)
    dorothea_chipseq = Column(Boolean)
    dorothea_tfbs = Column(Boolean)
    dorothea_coexp = Column(Boolean)
    dorothea_level = Column(ARRAY(String))
    type = Column(String)
    curation_effort = Column(Integer)
    extra_attrs = Column(JSONB, nullable = True)
    evidences = Column(JSONB, nullable = True)
    ncbi_tax_id_source = Column(Integer)
    entity_type_source = Column(String)
    ncbi_tax_id_target = Column(Integer)
    entity_type_target = Column(String)
    #  The service filters interactions by entity (uniprot or gene symbol, on
    #  either partner) and by resource (membership in the `sources` array). The
    #  table is ~2M rows with no index beyond the PK, so these are seq-scanned;
    #  a GIN on `sources` and btrees on the entity columns cover the hot paths.
    __table_args__ = (
        Index('ix_interactions_source', 'source'),
        Index('ix_interactions_target', 'target'),
        Index('ix_interactions_source_genesymbol', 'source_genesymbol'),
        Index('ix_interactions_target_genesymbol', 'target_genesymbol'),
        Index('ix_interactions_sources', 'sources', postgresql_using = 'gin'),
    )


class Intercell(Base):
    """
    Definition for the `intercell` table columns and types.
    """

    __tablename__ = 'intercell'
    id = Column(Integer, primary_key = True)
    category = Column(String)
    parent = Column(String)
    database = Column(String)
    scope = Column(String)
    aspect = Column(String)
    source = Column(String)
    uniprot = Column(String)
    genesymbol = Column(String)
    entity_type = Column(String)
    consensus_score = Column(Integer)
    transmitter = Column(Boolean)
    receiver = Column(Boolean)
    secreted = Column(Boolean)
    plasma_membrane_transmembrane = Column(Boolean)
    plasma_membrane_peripheral = Column(Boolean)
    #  The service filters intercell by protein (uniprot/genesymbol), category and source.
    __table_args__ = (
        Index('ix_intercell_uniprot', 'uniprot'),
        Index('ix_intercell_genesymbol', 'genesymbol'),
        Index('ix_intercell_category', 'category'),
        Index('ix_intercell_source', 'source'),
    )


class Licenses(Base):
    """
    License information.
    """

    __tablename__ = 'licenses'
    id = Column(Integer, primary_key = True)
    resource = Column(String)
    name = Column(String)
    full_name = Column(String)
    purpose = Column(String)
    attrib = Column(String)
    sharing = Column(String)
    url = Column(String)
