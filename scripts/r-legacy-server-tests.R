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
    annotations = list(),
    complexes = list(),
    enzsub = list(),
    intercell = list()
)

#purrr::cross
