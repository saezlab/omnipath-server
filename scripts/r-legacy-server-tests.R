#!/usr/bin/env Rscript

library(OmnipathR)
library(magrittr)
library(purrr)
library(tidyr)
library(dplyr)

options(
    omnipathr.retry_downloads = 1L,
    omnipathr.url = 'http://localhost:44444',
    omnipathr.notls_fallback = FALSE
)
OmnipathR:::.optrace()

omnipath_set_cachedir(tempdir())

single_query <- function(query_type, args){
    args %<>% discard(~length(.x) > 1 || is.na(.x))

    # Converts comma-separated strings to list
    args <- map(args, function(x){
        if(length(x) != 0 && is.character(x) && !is.na(x) && stringr::str_detect(x, ",")){
            stringr::str_split(x, ",")[[1]]
        }
        else{
            x
        }
    })
    get(query_type, envir = asNamespace('OmnipathR')) %>% exec(!!!args)

    # TODO: after that also, JSON blobs decoding error in evidences column
}

ARGS <- list(
    omnipath_interactions = list(
        organisms = c(9606, 10090, 10116),
        genesymbols = c('yes', 'no', TRUE, FALSE, 1, 0),
        datasets = c(
            'omnipath',
            'collectri',
            'kinaseextra'
        ),
        resources = c(
            'SIGNOR',
            'CellChatDB',
            'HPRD',
            'PhosphoSite',
            'iTALK',
            'Baccin2019'
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
        evidences = c( # TODO: fix handling of boolean/yes/no, etc
        # We should make the service handle array with single values as values
            FALSE,
            TRUE
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
        genesymbols = c('yes', 'no', TRUE, FALSE, 1, 0),
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
        genesymbols = c('yes', 'no', TRUE, FALSE, 1, 0),
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
        trans = c(TRUE, NA),
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


main <- function() {

    ARGS %>%
    map2(
        names(.),
        .,
        function(query_type, args) {
            args %>%
            # Fix this
            expand_grid(!!!.) %>%
            rowwise() %>%
            mutate(
                results = list(
                    single_query(query_type, as.list(cur_data()))
                )
            )
        }
    )

}

main()
