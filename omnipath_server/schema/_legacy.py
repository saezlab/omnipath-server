from sqlalchemy import Column, String, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base

__all__ = [
    'Base',
    'Interactions',
]

Base = declarative_base()


class Interactions(Base):

    __tablename__ = 'interactions'
    id = Column(Integer, primary_key = True)
    source = Column(String)
    target = Column(String)
    source_genesymbol = Column(String)
    target_genesymbol = Column(String)
    is_directed = Column(Boolean)
    is_stimulation = Column(Boolean)
    is_inhibition = Column(Boolean)
    consensus_direction = Column(Boolean)
    consensus_stimulation = Column(Boolean)
    consensus_inhibition = Column(Boolean)
    sources = Column(String)
    references = Column(String)
    omnipath = Column(Boolean)
    kinaseextra = Column(Boolean)
    ligrecextra = Column(Boolean)
    pathwayextra = Column(Boolean)
    mirnatarget = Column(Boolean)
    dorothea = Column(Boolean)
    collectri = Column(Boolean)
    tf_target = Column(Boolean)
    lncrna_mrna = Column(Boolean)
    tf_mirna = Column(Boolean)
    small_molecule = Column(Boolean)
    dorothea_curated = Column(Boolean)
    dorothea_chipseq = Column(Boolean)
    dorothea_tfbs = Column(Boolean)
    dorothea_coexp = Column(Boolean)
    dorothea_level = Column(String)
    type = Column(String)
    curation_effort = Column(Integer)
    # TODO: JSON blob
    extra_attrs = Column(String)
    evidences = Column(String)
    ncbi_tax_id_source = Column(Integer)
    entity_type_source = Column(String)
    ncbi_tax_id_target = Column(Integer)
    entity_type_target = Column(String)
