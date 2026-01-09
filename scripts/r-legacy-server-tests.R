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

# ============================================================================
# Validation Helper Functions
# ============================================================================

check_columns_exist <- function(result, required_cols) {
    # Check if all required columns exist in the result data frame.
    #
    # Args:
    #   result: Data frame to check
    #   required_cols: Character vector of required column names
    #
    # Returns:
    #   TRUE if all required columns exist, FALSE otherwise

    if (!inherits(result, 'data.frame')) {
        return(FALSE)
    }

    all(required_cols %in% names(result))
}

check_column_types <- function(result, type_map) {
    # Check if columns have expected types.
    #
    # Args:
    #   result: Data frame to check
    #   type_map: Named list mapping column names to expected types
    #             (e.g., list(source = 'character', is_directed = 'logical'))
    #
    # Returns:
    #   TRUE if all specified columns have correct types, FALSE otherwise
    if (!inherits(result, 'data.frame')) {
        return(FALSE)
    }

    all(map2_lgl(names(type_map), type_map, function(col_name, expected_type) {
        if (!(col_name %in% names(result))) {
            return(FALSE)
        }

        actual_type <- class(result[[col_name]])[1]

        # Handle numeric types (integer counts as numeric)
        if (expected_type == 'numeric' && actual_type %in% c('numeric', 'integer', 'double')) {
            return(TRUE)
        }

        actual_type == expected_type
    }))
}

check_organism_filter <- function(result, expected_organism) {
    # Verify organism filtering is correctly applied.
    #
    # Args:
    #   result: Data frame to check
    #   expected_organism: Expected NCBI taxonomy ID (integer)
    #
    # Returns:
    #   TRUE if all organism-related columns match expected_organism
    if (!inherits(result, 'data.frame') || nrow(result) == 0) {
        return(FALSE)
    }

    # Find all ncbi_tax_id columns
    tax_cols <- names(result)[str_detect(names(result), 'ncbi_tax_id')]

    if (length(tax_cols) == 0) {
        # No organism columns, can't verify but don't fail
        return(TRUE)
    }

    # Check all taxonomy columns contain only the expected organism
    all(map_lgl(tax_cols, ~all(result[[.x]] == expected_organism, na.rm = TRUE)))
}

check_boolean_column <- function(result, col_name, expected_value) {
    # Check if a boolean column has the expected value.
    #
    # Args:
    #   result: Data frame to check
    #   col_name: Name of the boolean column
    #   expected_value: Expected boolean value (TRUE/FALSE)
    #
    # Returns:
    #   TRUE if all values in the column match expected_value
    if (!inherits(result, 'data.frame') || !(col_name %in% names(result))) {
        return(FALSE)
    }

    all(result[[col_name]] == expected_value, na.rm = TRUE)
}

check_resource_filter <- function(result, resource_name) {
    # Verify resource filtering is correctly applied.
    #
    # Args:
    #   result: Data frame to check
    #   resource_name: Expected resource name (string or vector)
    #
    # Returns:
    #   TRUE if resource appears in sources/resources columns

    if (!inherits(result, 'data.frame') || nrow(result) == 0) {
        return(FALSE)
    }

    # Check sources column (interactions, enzsub, complexes)
    if ('sources' %in% names(result)) {
        return(any(str_detect(result$sources, resource_name)))
    }

    # Check database column (intercell)
    if ('database' %in% names(result)) {
        return(any(result$database %in% resource_name))
    }

    # Check source column (annotations)
    if ('source' %in% names(result)) {
        return(any(result$source %in% resource_name))
    }

    FALSE
}

check_has_rows <- function(result, min_rows = 1) {
    # Check if result has minimum number of rows.
    #
    # Args:
    #   result: Data frame to check
    #   min_rows: Minimum required number of rows (default: 1)
    #
    # Returns:
    #   TRUE if result has at least min_rows

    if (!inherits(result, 'data.frame')) {
        return(FALSE)
    }

    nrow(result) >= min_rows
}

check_only_requested_fields <- function(result, requested_fields,
                                       base_fields = c('uniprot', 'genesymbol')) {
    # Verify only requested fields are present in result.
    #
    # Args:
    #   result: Data frame to check
    #   requested_fields: Character vector of requested field names
    #   base_fields: Base fields that are always present
    #
    # Returns:
    #   TRUE if no extra fields beyond requested and base fields

    if (!inherits(result, 'data.frame')) {
        return(FALSE)
    }

    allowed_fields <- c(base_fields, requested_fields)
    extra_fields <- setdiff(names(result), allowed_fields)

    length(extra_fields) == 0
}

check_contains_value <- function(result, col_name, expected_values) {
    # Check if column contains any of the expected values.
    #
    # Args:
    #   result: Data frame to check
    #   col_name: Name of the column
    #   expected_values: Vector of expected values
    #
    # Returns:
    #   TRUE if column contains at least one of the expected values

    if (!inherits(result, 'data.frame') || !(col_name %in% names(result))) {
        return(FALSE)
    }

    any(result[[col_name]] %in% expected_values)
}

check_all_values_in_set <- function(result, col_name, allowed_values) {
    # Check if all values in column are within allowed set.
    #
    # Args:
    #   result: Data frame to check
    #   col_name: Name of the column
    #   allowed_values: Vector of allowed values
    #
    # Returns:
    #   TRUE if all values are in the allowed set

    if (!inherits(result, 'data.frame') || !(col_name %in% names(result))) {
        return(FALSE)
    }

    all(result[[col_name]] %in% allowed_values, na.rm = TRUE)
}

check_no_duplicates <- function(result, key_cols) {
    # Check if result has no duplicate rows based on key columns.
    #
    # Args:
    #   result: Data frame to check
    #   key_cols: Character vector of column names to check for uniqueness
    #
    # Returns:
    #   TRUE if no duplicate rows based on key columns

    if (!inherits(result, 'data.frame')) {
        return(FALSE)
    }

    if (!all(key_cols %in% names(result))) {
        return(FALSE)
    }

    result %>%
        select(all_of(key_cols)) %>%
        duplicated() %>%
        any() %>%
        not()
}

# ============================================================================
# End of Validation Helper Functions
# ============================================================================

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
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol', 'omnipath')))
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
            equals(0L) &&
            (result %>% check_columns_exist(c('ncbi_tax_id_source', 'ncbi_tax_id_target')))
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
        check = function(result){
            ev1 <- result$evidences[[1]]

            return (ev1$id_a == result$source[[1]] &&
                ev1$id_b == result$target[[1]]) &&
            (result %>% check_columns_exist(c('sources', 'references', 'curation_effort', 'evidences')))
        },
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
        check = function(result) {
            (result$type == 'post_transcriptional') %>% all &&
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol')))
        },
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
        check = function(result) {
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol')))
        },
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
        check = function(result) {
            (result$type == 'transcriptional') %>% all &&
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol')))
        },
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
            (result$type == 'transcriptional') %>% all &&
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol', 'type')))
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
            result %>% filter(source == target) %>% nrow() %>% is_greater_than(0L) &&
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol')))
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
        check = function(result) {
            (result$sources %>% str_detect('DoRothEA') %>% all) &&
            (result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol')))
        },
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
        check = function(result) {
            (result$source %>% unique() %>% equals('UniProt_tissue')) &&
            (result %>% check_columns_exist(c('genesymbol', 'value', 'label')))
        },
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
        check = function(result) {
            (result$source %>% unique() %>% equals('CellPhoneDB')) &&
            (result %>% check_columns_exist(c('genesymbol', 'value', 'label')))
        },
        tags = c('full-db')
    ),
    list(
        id = 'complexes_basic',
        query = 'complexes',
        description = 'hu.MAP complexes sanity check.',
        args = list(
            resources = 'hu.MAP'
        ),
        check = function(result) {
            (result$source %>% unique() %>% equals('hu.MAP')) &&
            (result %>% check_columns_exist(c(
                'name',
                'components',
                'stoichiometry'
            )))
        },
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
        check = function(result) {
            (result$residue_type %>% unique() %>% equals('S')) &&
            (result$modification %>% unique() %>% equals('phosphorylation')) &&
            (result %>% check_columns_exist(c(
                'enzyme',
                'substrate',
                'enzyme_genesymbol',
                'substrate_genesymbol'
            )))
        },
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
