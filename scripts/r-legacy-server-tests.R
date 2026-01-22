#!/usr/bin/env Rscript

library(OmnipathR)
library(magrittr)
library(purrr)
library(stringr)
library(logger)
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

check_columns_exist <- function(result, cols, negate = FALSE) {
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
    negate %<>% `if`(not, identity)

    all(cols %in% names(result) %>% negate)
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
        check = function(result) {c(
            result$sources %>% str_detect('SIGNOR') %>% all,
            result$omnipath %>% all,
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol',
                'omnipath'
            ))
        )},
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
        check = function(result) {c(
            result %>% check_columns_exist(c('ncbi_tax_id_source', 'ncbi_tax_id_target')),
            result$ncbi_tax_id_source %>%
            c(result$ncbi_tax_id_target) %>%
            setdiff(10090) %>%
            length() %>%
            equals(0L)
        )},
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

            return(c(
                ev1$id_a == result$source[[1]],
                ev1$id_b == result$target[[1]],
                result %>% check_columns_exist(c(
                    'sources',
                    'references',
                    'curation_effort',
                    'evidences'
                ))
            ))
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
            signed = FALSE,
            fields = 'type'
        ),
        check = function(result) {c(
            (result$type == 'post_transcriptional') %>% all,
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol'
            ))
        )},
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
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol'
            ))
        },
        tags = c('core')
    ),
    list(
        id = 'interactions_tf_mirna',
        query = 'omnipath_interactions',
        description = 'TF-miRNA interactions.',
        args = list(
            organisms = 9606,
            datasets = 'tf_mirna',
            fields = 'type'
        ),
        check = function(result) {c(
            # FIXME: CollecTRI is set to transcriptional in the database build
            (result$type %>% is_in(c('transcriptional', 'mirna_transcriptional'))) %>% all,
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol'
            ))
        )},
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
        check = function(result) {c(
            (result$type == 'transcriptional') %>% all,
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol',
                'type'
            ))
        )},
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
        check = function(result) {c(
            result %>%
            dplyr::filter(source == target) %>%
            nrow() %>% is_greater_than(0L),
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol'
            ))
        )},
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
        check = function(result) {c(
            result$sources %>% str_detect('DoRothEA') %>% all,
            result %>% check_columns_exist(c(
                'source_genesymbol',
                'target_genesymbol'
            ))
        )},
        tags = c('full-db')
    ),
    # ========================================================================
    # Phase 2: Additional Interactions Test Scenarios
    # ========================================================================
    list(
        id = 'interactions_sources_filter',
        query = 'omnipath_interactions',
        description = 'Test filtering by multiple specific sources.',
        args = list(
            organisms = 9606,
            datasets = 'omnipath',
            sources = c('TP53', 'EGFR'),
            genesymbols = TRUE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$source_genesymbol %in% c('TP53', 'EGFR') %>% any,
            result$target_genesymbol %in% c('TP53', 'EGFR') %>% any,
            result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol'))
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_extra_attrs',
        query = 'omnipath_interactions',
        description = 'Validate extra_attrs parameter returns additional columns.',
        args = list(
            organisms = 9606,
            datasets = 'omnipath',
            resources = 'SIGNOR',
            extra_attrs = TRUE,
            fields = c('sources', 'extra_attrs'),
            limit = 50
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result %>% check_columns_exist(c('extra_attrs')),
            result$sources %>% str_detect('SIGNOR') %>% all
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_unsigned',
        query = 'omnipath_interactions',
        description = 'Test signed=FALSE filtering for unsigned interactions.',
        args = list(
            signed = FALSE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            # Unsigned means neither stimulation nor inhibition
            (result$is_stimulation | result$is_inhibition) %>% not %>% any
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_undirected',
        query = 'omnipath_interactions',
        description = 'Test directed=FALSE for undirected interactions.',
        args = list(
            directed = FALSE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            !(result$is_directed) %>% any
        )},
        tags = c('core')
    ),
    # list(
    #     id = 'interactions_limit',
    #     query = 'omnipath_interactions',
    #     description = 'Test SQL LIMIT clause functionality.',
    #     args = list(
    #         limit = 25
    #     ),
    #     check = function(result) {c(
    #         nrow(result) == 25
    #     )},
    #     tags = c('core')
    # ),
    list(
        id = 'interactions_fields_comprehensive',
        query = 'omnipath_interactions',
        description = 'Test multiple field combinations.',
        args = list(
            resources = 'SIGNOR',
            fields = c('sources', 'references', 'curation_effort', 'type', 'datasets', 'organism'),
            limit = 50
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result %>% check_columns_exist(c(
                'sources',
                'references',
                'curation_effort',
                'type'
            )),
            result %>% check_columns_exist(c(
                'ncbi_tax_id_source',
                'ncbi_tax_id_target',
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
                'small_molecule'
            ))
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_protein_specific',
        query = 'omnipath_interactions',
        description = 'Test with specific protein identifiers.',
        args = list(
            partners = c('TP53', 'MDM2'),
            genesymbols = TRUE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            # Check that TP53 or MDM2 appears in source or target
            (result$source_genesymbol %in% c('TP53', 'MDM2') |
             result$target_genesymbol %in% c('TP53', 'MDM2')) %>% all,
            result %>% check_columns_exist(c('source_genesymbol', 'target_genesymbol'))
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_pathwayextra',
        query = 'omnipath_interactions',
        description = 'Test pathwayextra dataset.',
        args = list(
            datasets = 'pathwayextra',
            fields = 'datasets',
            limit = 100
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$pathwayextra %>% all
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_ligrecextra',
        query = 'omnipath_interactions',
        description = 'Test ligrecextra dataset with ligand-receptor interactions.',
        args = list(
            datasets = 'ligrecextra',
            fields = c('type', 'datasets')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$ligrecextra %>% all,
            result$type %>% unique() %in% c('post_translational') %>% all,
            result %>% check_columns_exist('type')
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_types',
        query = 'omnipath_interactions',
        description = 'Test interactions of a specific type.',
        args = list(
            datasets = 'mirnatarget',
            types = 'post_transcriptional',
            fields = c('type', 'datasets', 'entity_type')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            (result$omnipath) %>% not %>%  all,
            (result$dorothea) %>% not %>% all,
            (result$pathwayextra) %>% not %>% all,
            result$type %>% unique() %>% equals('post_transcriptional'),
            # FIXME: there are some DB content issues therefore not all are mirna as they should
            result$entity_type_source %>% equals('mirna') %>% mean %>% is_greater_than(.99),
            result %>% check_columns_exist(c(
                'entity_type_target',
                'entity_type_source',
                'type'
            ))
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_lncrna_datasets',
        query = 'omnipath_interactions',
        description = 'Test lncRNA-mRNA interactions from datasets argument.',
        args = list(
            datasets = 'lncrna_mrna',
            fields = c('type', 'entity_type')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$type %>% unique() %>% equals('lncrna_post_transcriptional'),
            # Source should be lncrn and target should be protein
            (result$entity_type_source == 'lncrna') %>% unique %>% all,
            (result$entity_type_target == 'protein') %>% unique %>% all,
            result$lncrna_mrna %>% all
        )},
        tags = c('core')
    ),
        list(
        id = 'interactions_lncrna_entity_types',
        query = 'omnipath_interactions',
        description = 'Test lncRNA-mRNA interactions from entity_types argument.',
        args = list(
            datasets = 'lncrna_mrna',
            entity_types = 'lncrna',
            fields = c('type', 'entity_type')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$type %>% unique() %>% equals('lncrna_post_transcriptional'),
            # Source should be lncrn and target should be protein
            (result$entity_type_source == 'lncrna') %>% unique %>% all,
            (result$entity_type_target == 'protein') %>% unique %>% all,
            result$lncrna_mrna %>% all
        )},
        tags = c('core')
    ),
        list(
        id = 'interactions_lncrna_types',
        query = 'omnipath_interactions',
        description = 'Test lncRNA-mRNA interactions from types argument.',
        args = list(
            datasets = 'lncrna_mrna',
            types = 'lncrna_post_transcriptional',
            fields = c('type', 'entity_type')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$type %>% unique() %>% equals('lncrna_post_transcriptional'),
            # Source should be lncrn and target should be protein
            (result$entity_type_source == 'lncrna') %>% unique %>% all,
            (result$entity_type_target == 'protein') %>% unique %>% all,
            result$lncrna_mrna %>% all
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_multi_organism',
        query = 'omnipath_interactions',
        description = 'Test organism parameter with multiple values.',
        args = list(
            organisms = c(9606, 10090),
            resources = 'SIGNOR',
            fields = c('ncbi_tax_id')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result %>% check_columns_exist(c('ncbi_tax_id_source', 'ncbi_tax_id_target')),
            # All organisms should be either 9606 or 10090
            (result$ncbi_tax_id_source %in% c(9606, 10090)) %>% all,
            (result$ncbi_tax_id_target %in% c(9606, 10090)) %>% all,
            # Make sure mouse is at least somewhere
            (result$ncbi_tax_id_source == 10090) %>% any,
            (result$ncbi_tax_id_target == 10090) %>% any,
            # Check there's no inter-species interaction (we should not have for now)
            (result$ncbi_tax_id_source == result$ncbi_tax_id_target) %>% all
        )},
        tags = c('core')
    ),
    list(
        id = 'interactions_entity_combinations',
        query = 'omnipath_interactions',
        description = 'Test complex+protein entity types.',
        args = list(
            entity_types = 'complex',
            fields = c('entity_type')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result %>% check_columns_exist(c('entity_type_source', 'entity_type_target')),
            # At least some results should have the specified entity types
            (result$entity_type_source == 'complex' |
             result$entity_type_target == 'complex') %>% all
        )},
        tags = c('core')
    ),
    # ========================================================================
    # End of Phase 2: Additional Interactions Test Scenarios
    # ========================================================================
    list(
        id = 'annotations_basic',
        query = 'annotations',
        description = 'Protein annotations from UniProt tissue.',
        args = list(
            resources = 'UniProt_tissue',
            entity_types = 'protein',
            genesymbols = TRUE
        ),
        check = function(result) {c(
            result$source %>% unique() %>% equals('UniProt_tissue'),
            result %>% check_columns_exist(c('genesymbol', 'value', 'label'))
        )},
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
        check = function(result) {c(
            result$source %>% unique() %>% equals('CellPhoneDB'),
            result %>% check_columns_exist(c('genesymbol', 'value', 'label'))
        )},
        tags = c('full-db')
    ),
    list(
        id = 'complexes_basic',
        query = 'complexes',
        description = 'hu.MAP complexes sanity check.',
        args = list(
            resources = 'hu.MAP'
        ),
        check = function(result) {c(
            result$sources %>% unique() %>% str_detect('hu.MAP') %>% all,
            result %>% check_columns_exist(c(
                'name',
                'components',
                'stoichiometry'
            ))
        )},
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
        check = function(result) {c(
            result$residue_type %>% unique() %>% equals('S'),
            result$modification %>% unique() %>% equals('phosphorylation'),
            result %>% check_columns_exist(c(
                'enzyme',
                'substrate',
                'enzyme_genesymbol',
                'substrate_genesymbol'
            ))
        )},
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
        check = function(result) {c(
            result$residue_type %>% unique() %>% equals('Y'),
            result$modification %>% unique() %>% equals('dephosphorylation'),
            result %>% check_columns_exist(c(
                'enzyme',
                'substrate'
            )),
            result %>% check_columns_exist(c(
                'enzyme_genesymbol',
                'substrate_genesymbol'
            ))
        )},
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
            categories = 'ligand',
            fields = 'topology'
        ),
        check = function(result) {c(
            result$transmitter %>% all,
            result$secreted %>% all,
            result$receiver %>% all %>% not,
            result$category %>% unique %>% equals('ligand') %>% all,
            result$database %>% unique %>% equals('CellChatDB') %>% all,
            result %>% check_columns_exist(c(
                'plasma_membrane_transmembrane',
                'plasma_membrane_peripheral'
            ))
        )},
        tags = c('smoke', 'core')
    ),
    list(
        id = 'intercell_membrane_full',
        query = 'intercell',
        description = 'Membrane topology / complex view.',
        args = list(
            entity_types = 'complex',
            plasma_membrane_transmembrane = TRUE,
            plasma_membrane_peripheral = TRUE,
            aspect = 'functional',
            scope = 'specific'
        ),
        check = function(result) {c(
            result$plasma_membrane_transmembrane %>% all,
            result$plasma_membrane_peripheral %>% all,
            result$entity_type %>% unique %>% equals('complex'),
            result$uniprot %>% str_starts('COMPLEX:') %>% all,
            result$genesymbol %>% str_starts('COMPLEX:') %>% all
        )},
        tags = c('full-db')
    ),
    # ========================================================================
    # Phase 3: Additional Intercell Test Scenarios
    # ========================================================================
    list(
        id = 'intercell_receiver',
        query = 'intercell',
        description = 'Test receiver=TRUE with receptor categories.',
        args = list(
            receiver = TRUE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$receiver %>% all,
            'receptor' %in% result$category
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_transmitter',
        query = 'intercell',
        description = 'Test transmitter=TRUE with ligand categories.',
        args = list(
            transmitter = TRUE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$transmitter %>% all,
            'ligand' %in% result$category
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_causality_transmitter',
        query = 'intercell',
        description = 'Test causality=transmitter parameter.',
        args = list(causality = 'transmitter'),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$transmitter %>% all,
            result$receiver %>% all %>% not,
            'ligand' %in% result$category
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_causality_receiver',
        query = 'intercell',
        description = 'Test causality=receiver parameter.',
        args = list(causality = 'receiver'),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$receiver %>% all,
            result$transmitter %>% all %>% not,
            'receptor' %in% result$category
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_source_composite',
        query = 'intercell',
        description = 'Test source=composite filtering.',
        args = list(
            source = 'composite',
            categories = 'ligand',
            fields = c('source')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$source %>% unique() %>% equals('composite'),
            result$category %>% unique() %>% equals('ligand')
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_parent_categories',
        query = 'intercell',
        description = 'Test parent category filtering.',
        args = list(
            parent = 'adhesion',
            fields = c('parent', 'category')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$parent %>% str_detect('adhesion') %>% any,
            result %>% check_columns_exist(c('parent', 'category'))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_proteins',
        query = 'intercell',
        description = 'Test specific protein queries.',
        args = list(
            proteins = c('EGFR', 'ERBB2', 'TGFB1'),
            categories = 'receptor',
            genesymbols = TRUE
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$genesymbol %in% c('EGFR', 'ERBB2', 'TGFB1') %>% all,
            result %>% check_columns_exist(c('genesymbol', 'category'))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_fields',
        query = 'intercell',
        description = 'Test field selection.',
        args = list(
            categories = 'ligand',
            limit = 50
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1)
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_multiple_categories',
        query = 'intercell',
        description = 'Test category combinations.',
        args = list(
            categories = c('ligand', 'receptor'),
            fields = c('category')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$category %in% c('ligand', 'receptor') %>% all,
            result %>% check_columns_exist(c('category'))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_topology_secreted',
        query = 'intercell',
        description = 'Test secreted topology filtering.',
        args = list(
            topology = 'secreted',
            categories = 'ligand',
            fields = c('topology')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$secreted %>% all,
            result %>% check_columns_exist(c(
                'secreted',
                'plasma_membrane_transmembrane',
                'plasma_membrane_peripheral'
            ))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_topology_pmtm',
        query = 'intercell',
        description = 'Test plasma membrane transmembrane topology.',
        args = list(
            plasma_membrane_transmembrane = TRUE,
            categories = 'receptor',
            fields = c('topology')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$plasma_membrane_transmembrane %>% all,
            result %>% check_columns_exist(c(
                'plasma_membrane_transmembrane',
                'plasma_membrane_peripheral',
                'secreted'
            ))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_topology_pmp',
        query = 'intercell',
        description = 'Test plasma membrane peripheral topology.',
        args = list(
            plasma_membrane_peripheral = TRUE,
            categories = 'adhesion',
            fields = c('topology')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$plasma_membrane_peripheral %>% all,
            result %>% check_columns_exist(c(
                'plasma_membrane_transmembrane',
                'plasma_membrane_peripheral',
                'secreted'
            ))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_transmitter_secreted',
        query = 'intercell',
        description = 'Test transmitter with secreted flag.',
        args = list(
            transmitter = TRUE,
            secreted = TRUE,
            categories = 'ligand'
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$transmitter %>% all,
            result$secreted %>% all,
            result$category %>% unique() %>% equals('ligand')
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_generic_scope',
        query = 'intercell',
        description = 'Test scope=generic with categories.',
        args = list(
            scope = 'generic',
            aspect = 'functional',
            categories = 'ligand',
            fields = c('scope')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$scope %>% unique() %>% equals('generic'),
            result %>% check_columns_exist(c('scope', 'category'))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_specific_scope',
        query = 'intercell',
        description = 'Test scope=specific with categories.',
        args = list(
            scope = 'specific',
            aspect = 'functional',
            parent = 'receptor',
            fields = c('scope')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$scope %>% unique() %>% equals('specific'),
            result %>% check_columns_exist(c('scope', 'category'))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_locational_aspect',
        query = 'intercell',
        description = 'Test aspect=locational.',
        args = list(
            aspect = 'locational',
            topology = 'plasma_membrane_transmembrane',
            fields = c('aspect')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$aspect %>% unique() %>% equals('locational'),
            result$plasma_membrane_transmembrane %>% all,
            result %>% check_columns_exist(c('aspect'))
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_functional_aspect',
        query = 'intercell',
        description = 'Test aspect=functional.',
        args = list(
            aspect = 'functional',
            categories = 'ligand',
            fields = c('aspect', 'category')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$aspect %>% unique() %>% equals('functional'),
            result$category %>% unique() %>% equals('ligand')
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_limit',
        query = 'intercell',
        description = 'Test SQL LIMIT functionality.',
        args = list(
            categories = 'ligand',
            limit = 30
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            nrow(result) <= 30,
            result$category %>% unique() %>% equals('ligand')
        )},
        tags = c('core')
    ),
    list(
        id = 'intercell_entity_complex',
        query = 'intercell',
        description = 'Test complex entity types.',
        args = list(
            entity_types = 'complex',
            categories = 'receptor',
            fields = c('entity_type')
        ),
        check = function(result) {c(
            result %>% check_has_rows(min_rows = 1),
            result$entity_type %>% unique() %>% equals('complex'),
            result$uniprot %>% str_starts('COMPLEX:') %>% all
        )},
        tags = c('core')
    )
    # ========================================================================
    # End of Phase 3: Additional Intercell Test Scenarios
    # ========================================================================
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
        res <- scenario$check(result)
        res_all <- res %>% all

        if (!res_all) {
            log_warn(paste(res, collapse = ' | '))
            capture.output(print(result)) %>% head(-1) %>% walk(log_warn)
        }

        return(res_all)
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
