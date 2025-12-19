#!/usr/bin/env Rscript

library(OmnipathR)
library(magrittr)
library(purrr)
library(stringr)
library(dplyr)

options(
    omnipathr.retry_downloads = 1L,
    omnipathr.url = 'http://localhost:44444',
    omnipathr.notls_fallback = FALSE
)
OmnipathR:::.optrace()

omnipath_set_cachedir(tempdir())

`%||%` <- function(x, y) if (is.null(x)) y else x

parse_bool <- function(value) {
    if (length(value) == 0 || is.na(value) || identical(value, "")) {
        return(FALSE)
    }

    tolower(value) %in% c('1', 'true', 't', 'yes', 'y')
}

single_query <- function(query_type, args){
    args %<>% discard(~length(.x) == 1 && (is.na(.x) || identical(.x, "")))

    # Converts comma-separated strings to list
    args <- map(args, function(x){
        if(length(x) == 1 && is.character(x) && !is.na(x) && stringr::str_detect(x, ",")){
            stringr::str_split(x, ",")[[1]]
        } else {
            x
        }
    })

    get(query_type, envir = asNamespace('OmnipathR')) %>% exec(!!!args)
}

# Scenarios tagged with `full-db` are skipped unless OMNIPATH_FULL_DB=1
# or the script is invoked with --full-db/--complete. Use --list-scenarios
# to inspect the available IDs and --scenario=<id> to focus on a subset.
SCENARIOS <- list(
    list(
        id = 'interactions_basic',
        query = 'omnipath_interactions',
        description = 'Baseline OmniPath dataset via SIGNOR resources.',
        args = list(
            organisms = 9606,
            datasets = 'omnipath',
            resources = 'SIGNOR',
            genesymbols = TRUE,
            fields = c('sources', 'datasets')
        ),
        check = function(result) {
            (result$sources %>% str_detect('SIGNOR') %>% all) &&
            (result$omnipath %>% all) &&
            (c('source_genesymbol', 'target_genesymbol') %in% names(result) %>% all)
        },
        tags = c('smoke', 'core')
    ),
    list(
        id = 'interactions_basic_mice',
        query = 'omnipath_interactions',
        description = 'Baseline OmniPath dataset via SIGNOR resources in mice.',
        args = list(
            organisms = 10090,
            datasets = 'omnipath',
            resources = 'SIGNOR',
            genesymbols = TRUE,
            fields = c('ncbi_tax_id')
        ),
        check = function(result) {
            result$ncbi_tax_id_source %>%
            c(result$ncbi_tax_id_target) %>%
            setdiff(10090) %>%
            length() %>%
            equals(0L)
        },
        tags = c('smoke', 'core')
    ),
    list(
        id = 'interactions_evidences',
        query = 'omnipath_interactions',
        description = 'Covers evidences JSON payload and column selection.',
        args = list(
            organisms = 9606,
            datasets = 'omnipath',
            resources = 'SIGNOR',
            evidences = TRUE,
            fields = c('sources', 'references', 'curation_effort', 'evidences')
        ),
        tags = c('smoke', 'json')
    ),
    list(
        id = 'interactions_mirna',
        query = 'omnipath_interactions',
        description = 'miRNA / post-transcriptional interactions.',
        args = list(
            organisms = 9606,
            entity_types = 'mirna',
            types = 'post_transcriptional',
            datasets = 'mirnatarget',
            signed = FALSE
        ),
        tags = c('core')
    ),
    list(
        id = 'interactions_small_molecule',
        query = 'omnipath_interactions',
        description = 'small molecule interactions.',
        args = list(
            organisms = 9606,
            datasets = 'small_molecule'
        ),
        tags = c('core')
    ),
    list(
        id = 'interactions_tf_mirna',
        query = 'omnipath_interactions',
        description = 'TF-miRNA interactions.',
        args = list(
            organisms = 9606,
            datasets = 'tf_mirna'
        ),
        tags = c('core')
    ),
    list(
        id = 'interactions_tf_target',
        query = 'omnipath_interactions',
        description = 'TF-target interactions.',
        args = list(
            organisms = 9606,
            datasets = 'tf_target',
            fields = 'type'
        ),
        check = function(result) {
            (result$type == 'transcriptional') %>% all
        },
        tags = c('core')
    ),
    list(
        id = 'interactions_loops_collectri',
        query = 'omnipath_interactions',
        description = 'Collectri transcriptional subset with loops enabled.',
        args = list(
            organisms = 9606,
            datasets = 'collectri',
            types = 'transcriptional',
            loops = TRUE,
            directed = TRUE
        ),
        check = function(result) {
            result %>% filter(source == target) %>% nrow() %>% is_greater_than(0L)
        },
        tags = c('core')
    ),
    list(
        id = 'interactions_dorothea_full',
        query = 'omnipath_interactions',
        description = 'High coverage DoRothEA call (needs complete DB).',
        args = list(
            datasets = 'dorothea',
            dorothea_levels = 'A,B',
            dorothea_methods = 'coexp,tfbs',
            organisms = 9606
        ),
        tags = c('full-db')
    ),
    list(
        id = 'annotations_basic',
        query = 'annotations',
        description = 'Protein annotations from UniProt tissue.',
        args = list(
            resources = 'UniProt_tissue',
            entity_types = 'protein',
            genesymbols = TRUE
        ),
        tags = c('smoke', 'core')
    ),
    list(
        id = 'annotations_complex',
        query = 'annotations',
        description = 'Complex level annotations from CellPhoneDB.',
        args = list(
            resources = 'CellPhoneDB',
            entity_types = 'complex'
        ),
        tags = c('full-db')
    ),
    list(
        id = 'complexes_basic',
        query = 'complexes',
        description = 'hu.MAP complexes sanity check.',
        args = list(
            resources = 'hu.MAP'
        ),
        tags = c('smoke', 'core')
    ),
    list(
        id = 'enzsub_phospho',
        query = 'enzyme_substrate',
        description = 'Phosphorylation queries on PhosphoSite.',
        args = list(
            resources = 'PhosphoSite',
            organisms = 9606,
            genesymbols = TRUE,
            modification = 'phosphorylation',
            residues = 'S'
        ),
        tags = c('smoke', 'core')
    ),
    list(
        id = 'enzsub_dephos_full',
        query = 'enzyme_substrate',
        description = 'Dephosphorylation coverage on DEPOD.',
        args = list(
            resources = 'DEPOD',
            modification = 'dephosphorylation',
            residues = 'Y'
        ),
        tags = c('full-db')
    ),
    list(
        id = 'intercell_secreted',
        query = 'intercell',
        description = 'Secreted transmitters from CellChatDB.',
        args = list(
            resources = 'CellChatDB',
            trans = TRUE,
            sec = TRUE,
            categories = 'ligand'
        ),
        tags = c('smoke', 'core')
    ),
    list(
        id = 'intercell_membrane_full',
        query = 'intercell',
        description = 'Membrane topology / complex view.',
        args = list(
            entity_types = 'complex',
            pmtm = TRUE,
            pmp = 'yes',
            aspect = 'functional',
            scope = 'specific'
        ),
        tags = c('full-db')
    )
)

safe_row_count <- function(result){
    if (inherits(result, 'data.frame')) {
        return(nrow(result))
    }

    if (inherits(result, 'list')) {
        return(length(result))
    }

    length(result)
}

check_results <- function(result, scenario) {
    # Placeholder for future result checks

    if (!is.null(scenario$check)) {
        scenario$check(result)
    }
}

run_scenario <- function(scenario, include_full_db){
    tags <- scenario$tags %||% character(0)

    if (!include_full_db && 'full-db' %in% tags) {
        message(sprintf(
            '\n[%s] %s -- skipped (requires complete database)',
            scenario$query,
            scenario$id
        ))
        return(list(
            id = scenario$id,
            query = scenario$query,
            status = 'skipped',
            reason = 'requires full database'
        ))
    }

    message(sprintf('\n[%s] %s', scenario$query, scenario$id))
    if (!is.null(scenario$description)) {
        message(sprintf('  %s', scenario$description))
    }

    start <- Sys.time()
    outcome <- tryCatch(
        {
            res <- single_query(scenario$query, scenario$args)
            checkres <- check_results(res, scenario)
            rows <- safe_row_count(res)
            message(sprintf('  rows: %s', rows))
            list(status = 'success', rows = rows, check = checkres)
        },
        error = function(err){
            message(sprintf('  ERROR: %s', err$message))
            list(status = 'error', error = err$message)
        }
    )

    outcome$id <- scenario$id
    outcome$query <- scenario$query
    outcome$duration <- as.numeric(difftime(Sys.time(), start, units = 'secs'))

    outcome
}

print_summary <- function(results){
    message('\nSummary:')

    message('Test\tResult\tNrows\tCheck')

    walk(results, function(res){
        extra <- ''

        if (!is.null(res$rows)) {
            extra <- sprintf(' (%s rows)', res$rows)
        } else if (!is.null(res$reason)) {
            extra <- sprintf(' [%s]', res$reason)
        } else if (!is.null(res$error)) {
            extra <- sprintf(' [%s]', res$error)
        }

        message(sprintf(
            ' - %-24s %s%s\t%s',
            res$id,
            toupper(res$status %||% 'UNKNOWN'),
            extra,
            res$check %||% 'NO CHECK'
        ))
    })

    status_count <- function(status){
        sum(map_lgl(results, ~(.x$status %||% '') == status))
    }

    message(sprintf(
        '\nSucceeded: %d | Skipped: %d | Failed: %d | Checks: %d',
        status_count('success'),
        status_count('skipped'),
        status_count('error'),
        results %>% map(~pluck(.x, 'check')) %>% unlist() %>% sum()
    ))
}

main <- function() {
    cmd_args <- commandArgs(TRUE)

    include_full_db <- parse_bool(Sys.getenv('OMNIPATH_FULL_DB', '')) ||
        any(cmd_args %in% c('--full-db', '--complete'))

    requested_ids <- sub(
        '^--scenario=',
        '',
        grep('^--scenario=', cmd_args, value = TRUE)
    )

    list_only <- any(cmd_args %in% c('--list', '--list-scenarios'))

    if (list_only) {
        walk(
            SCENARIOS,
            ~cat(sprintf('%-24s %s\n', .x$id, .x$query))
        )
        quit(status = 0)
    }

    selected <- if (length(requested_ids) > 0) {
        keep(SCENARIOS, ~.x$id %in% requested_ids)
    } else {
        SCENARIOS
    }

    if (length(selected) == 0) {
        message('No scenarios selected. Use --list-scenarios to inspect available ids.')
        quit(status = 1)
    }

    results <- map(selected, run_scenario, include_full_db = include_full_db)
    print_summary(results)

    exit_code <- ifelse(
        any(map_lgl(results, ~(.x$status %||% '') == 'error')),
        1,
        0
    )

    quit(status = exit_code)
}

main()

# TODO: Add callback to check results
# TODO: Check other args
