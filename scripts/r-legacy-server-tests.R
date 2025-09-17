#!/usr/bin/env Rscript

library(OmnipathR)
library(purrr)

options(omnipathr.url = 'http://localhost:44444')
.optrace()

args <- c(
    interactions = list(
        organisms = c(9606, 10090, 10116),
        genesymbols = list('yes', 'no', TRUE, FALSE, 1, 0),
        datasets = c(
            'omnipath',
            'collectri',
            'kinaseextra',
            'ligrecextra',
            'pathwayextra',
            'mirnatarget'
        ),
        resources = c(
            'SIGNOR',
            'ACSN',
            'BioGRID',
            'CellChatDB',
            'HPRD',
            'ProtMapper',
            'PhosphoSite',
            'CellCall',
            'ICELLNET',
            'iTALK',
            'talklr',
            'Baccin2019',
            'EMBRACE',
            'Wang'
        ),
        entity_types = c(
             'protein',
             'mirna'
        ),
        types = c(
            'transcriptional',
            'post_translational'
        ),
        license = c(
            'commercial',
            'academic'
        ),
        loops = c(
            'no',
            'yes'
        ),
        evidences = c(
            'no',
            'yes'
        ),
        dorothea_levels = c(
            NA,
            'A,B,C'
        ),
        dorothea_methods = c(
            NA,
            'coexp,tfbs'
        ),
        directed = c(
            'no',
            'yes'
        ),
        signed = c(
            'no',
            'yes'
        )
    ),
    annotations = list(organisms = c(9606, 10090, 10116),
        genesymbols = list('yes', 'no', TRUE, FALSE, 1, 0),
        resources = c(
            'PROGENy',
            'CellPhoneDB',
            'UniProt_tissue',
            'LOCATE',
            'KEGG'
        ),
        entity_types = c(
             'protein',
             'mirna'
        ),
        license = c(
            'commercial',
            'academic'
        )
    ),
    complexes = list(
        license = c(
            'commercial',
            'academic'
        ),
        resources = c(
            'hu.MAP',
            'Compleat',
            'SIGNOR'
        )
    ),
    enzsub = list(
        license = c(
            'commercial',
            'academic'
        ),
        organisms = c(9606, 10090, 10116),
        genesymbols = list('yes', 'no', TRUE, FALSE, 1, 0),
        resources = c(
            'SIGNOR',
            'KEA',
            'HPRD',
            'ProtMapper',
            'PhosphoSite'
        ),
        modification = c(NA, 'phosphorylation', 'dephosphorylation'),
        residues = c('S', 'T')
    ),
    intercell = list(
        license = c(
            'commercial',
            'academic'
        ),
        resources = c(
            NA,
            'CellChatDB'
        ),
        causality = c(
            'both',
            'transmitter',
            NA
        ),
        topology = c(
            'pmp',
            'secreted',
            NA
        ),
        sec = c(NA, TRUE, 'no'),
        trans c(TRUE, NA),
        rec = c(NA, FALSE),
        pmtm = c(NA, TRUE),
        pmp = c('yes', 'no'),
        entity_types = c(
            'protein',
            'complex'
        ),
        aspect = c(NA, 'functional'),
        categories = c('receptor', NA),
        scope = c(NA, 'specific')
    )
)

#purrr::cross
