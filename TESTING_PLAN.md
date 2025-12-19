# LegacyService Testing Plan

## Overview

This document outlines a comprehensive plan to extend test coverage for the LegacyService in the OmniPath server. The goal is to ensure the service correctly handles all HTTP API parameters before production deployment.

**Current Status:** 15 test scenarios
**Target:** 60+ test scenarios
**Parameter Coverage:** ~30% → ~85%
**Data Validation:** 4 checks → 50+ checks

## Current Test Coverage Summary

The `scripts/r-legacy-server-tests.R` script currently covers all 5 query types:

- **interactions** (8 scenarios) - Best coverage
- **intercell** (2 scenarios)
- **annotations** (2 scenarios)
- **enzyme_substrate** (2 scenarios)
- **complexes** (1 scenario) - Minimal coverage

### Key Gaps Identified

1. **Limited parameter coverage** - Many API parameters are untested
2. **Weak data validation** - Only 4 scenarios have `check` functions
3. **Missing edge cases** - No limit testing, format variations, or error conditions
4. **Insufficient field testing** - Field selection parameter barely tested
5. **No protein-specific queries** - The `proteins` parameter is never used

---

## Test Extension Plan

### Phase 1: Enhanced Data Validation (Priority: HIGH)

**Goal:** Add robust `check` functions to all existing and new scenarios

**Reusable validation helpers to add:**

```r
# Column existence validation
check_columns_exist <- function(result, required_cols) {
  all(required_cols %in% names(result))
}

# Column type validation
check_column_types <- function(result, type_map) {
  all(map2_lgl(names(type_map), type_map,
    ~inherits(result[[.x]], .y)))
}

# Organism filtering validation
check_organism_filter <- function(result, expected_organism) {
  tax_cols <- names(result)[str_detect(names(result), 'ncbi_tax_id')]
  if (length(tax_cols) == 0) return(TRUE)
  all(map_lgl(tax_cols, ~all(result[[.x]] == expected_organism)))
}

# Boolean column validation
check_boolean_column <- function(result, col_name, expected_value) {
  all(result[[col_name]] == expected_value)
}

# Resource filtering validation
check_resource_filter <- function(result, resource_name) {
  any(str_detect(result$sources, resource_name) |
      str_detect(result$resources, resource_name))
}

# Non-empty result validation
check_has_rows <- function(result, min_rows = 1) {
  nrow(result) >= min_rows
}

# Field selection validation
check_only_requested_fields <- function(result, requested_fields, base_fields = c('uniprot', 'genesymbol')) {
  allowed_fields <- c(base_fields, requested_fields)
  extra_fields <- setdiff(names(result), allowed_fields)
  length(extra_fields) == 0
}
```

**Actions:**
1. Add column validation checks to verify expected columns exist
2. Add data type validation for numeric/boolean/character columns
3. Add value range checks for organism IDs, booleans, enum fields
4. Add non-empty result checks for queries expected to return data
5. Add relationship validation to verify filters were applied correctly

---

### Phase 2: Interactions Query Extension (Priority: HIGH)

**Current: 8 scenarios → Target: 15+ scenarios**

#### New Test Scenarios

1. **interactions_sources_filter**
   - Test filtering by multiple specific sources
   - Args: `sources = c('BioGRID', 'IntAct')`, `datasets = 'omnipath'`
   - Check: Verify all rows have one of the specified sources

2. **interactions_extra_attrs**
   - Validate extra_attrs parameter returns additional resource-specific columns
   - Args: `resources = 'SIGNOR'`, `extra_attrs = TRUE`
   - Check: Verify presence of resource-specific attribute columns

3. **interactions_unsigned**
   - Test signed=FALSE filtering for unsigned interactions
   - Args: `datasets = 'pathwayextra'`, `signed = FALSE`
   - Check: Verify no is_stimulation/is_inhibition columns or all FALSE

4. **interactions_undirected**
   - Test directed=FALSE for undirected interactions
   - Args: `datasets = 'omnipath'`, `directed = FALSE`
   - Check: Verify is_directed column is FALSE

5. **interactions_limit**
   - Test SQL LIMIT clause functionality
   - Args: `datasets = 'omnipath'`, `limit = 100`
   - Check: Verify exactly 100 or fewer rows returned

6. **interactions_fields_comprehensive**
   - Test multiple field combinations
   - Args: `fields = c('sources', 'references', 'curation_effort', 'databases')`
   - Check: Verify all requested fields are present

7. **interactions_protein_specific**
   - Test with specific protein identifiers
   - Args: `sources = c('TP53', 'MDM2')`, `datasets = 'omnipath'`
   - Check: Verify all rows contain TP53 or MDM2

8. **interactions_pathwayextra**
   - Test pathwayextra dataset
   - Args: `datasets = 'pathwayextra'`, `organisms = 9606`
   - Check: Verify dataset field contains 'pathwayextra'

9. **interactions_ligrecextra**
   - Test ligrecextra dataset with ligand-receptor interactions
   - Args: `datasets = 'ligrecextra'`, `fields = 'type'`
   - Check: Verify type field indicates ligand-receptor relationships

10. **interactions_kinaseextra**
    - Test kinase interactions
    - Args: `datasets = 'kinaseextra'`, `types = 'post_translational'`
    - Check: Verify post-translational modification relationships

11. **interactions_tfregulons**
    - Test tfregulons dataset
    - Args: `datasets = 'tfregulons'`, `types = 'transcriptional'`
    - Check: Verify transcriptional regulation type

12. **interactions_lncrna**
    - Test lncRNA-mRNA interactions
    - Args: `datasets = 'lncrna_mrna'`, `entity_types = 'lncrna'`
    - Check: Verify lncRNA entity types present

13. **interactions_multi_organism**
    - Test organism parameter validation
    - Args: `organisms = c(9606, 10090)`, `datasets = 'omnipath'`
    - Check: Verify organism filtering works correctly

14. **interactions_entity_combinations**
    - Test complex+protein entity types
    - Args: `entity_types = c('protein', 'complex')`, `datasets = 'omnipath'`
    - Check: Verify both entity types appear in results

---

### Phase 3: Intercell Query Extension (Priority: HIGH)

**Current: 2 scenarios → Target: 10+ scenarios**

#### New Test Scenarios

1. **intercell_receiver**
   - Test receiver=TRUE with receptor categories
   - Args: `receiver = TRUE`, `categories = 'receptor'`
   - Check: Verify receiver column is TRUE

2. **intercell_causality**
   - Test causality parameter filtering
   - Args: `causality = 'transmitter'`, `categories = 'ligand'`
   - Check: Verify transmitter-specific results

3. **intercell_source_composite**
   - Test source=composite vs resource_specific
   - Args: `source = 'composite'`, `categories = 'ligand'`
   - Check: Compare with resource_specific results

4. **intercell_parent_categories**
   - Test parent category filtering
   - Args: `parent = 'adhesion'`
   - Check: Verify hierarchical category filtering

5. **intercell_proteins**
   - Test specific protein queries
   - Args: `proteins = c('EGFR', 'ERBB2')`, `categories = 'receptor'`
   - Check: Verify only requested proteins appear

6. **intercell_fields**
   - Test field selection
   - Args: `fields = c('sources', 'databases')`, `categories = 'ligand'`
   - Check: Verify requested fields are present

7. **intercell_multiple_categories**
   - Test category combinations
   - Args: `categories = c('ligand', 'receptor')`
   - Check: Verify both categories present in results

8. **intercell_all_topologies**
   - Test topology filters individually
   - Args: `topology = 'secreted'`, `categories = 'ligand'`
   - Check: Verify secreted topology

9. **intercell_transmitter_secreted**
   - Test transmitter with secreted flag
   - Args: `transmitter = TRUE`, `secreted = TRUE`
   - Check: Verify both conditions met

10. **intercell_generic_scope**
    - Test scope=generic with categories
    - Args: `scope = 'generic'`, `aspect = 'functional'`
    - Check: Verify generic vs specific scope differences

11. **intercell_locational_aspect**
    - Test aspect=locational
    - Args: `aspect = 'locational'`, `topology = 'plasma_membrane_transmembrane'`
    - Check: Verify locational aspect results

12. **intercell_limit**
    - Test SQL LIMIT functionality
    - Args: `categories = 'ligand'`, `limit = 50`
    - Check: Verify 50 or fewer rows returned

---

### Phase 4: Enzyme-Substrate Query Extension (Priority: MEDIUM)

**Current: 2 scenarios → Target: 8+ scenarios**

#### New Test Scenarios

1. **enzsub_specific_enzyme**
   - Test enzymes parameter with specific kinases
   - Args: `enzymes = c('AKT1', 'MAPK1')`, `modification = 'phosphorylation'`
   - Check: Verify enzyme column contains only specified enzymes

2. **enzsub_specific_substrate**
   - Test substrates parameter
   - Args: `substrates = c('TP53', 'RB1')`, `modification = 'phosphorylation'`
   - Check: Verify substrate column contains only specified substrates

3. **enzsub_partners**
   - Test partners parameter (either enzyme or substrate)
   - Args: `partners = c('AKT1', 'TP53')`
   - Check: Verify partners appear as either enzyme or substrate

4. **enzsub_enzyme_substrate_OR**
   - Test enzyme_substrate=OR logic
   - Args: `enzymes = 'AKT1'`, `substrates = 'TP53'`, `enzyme_substrate = 'OR'`
   - Check: Verify OR logic (either condition satisfied)

5. **enzsub_multiple_residues**
   - Test residues with multiple values
   - Args: `residues = c('S', 'T', 'Y')`, `modification = 'phosphorylation'`
   - Check: Verify residue column contains only S, T, or Y

6. **enzsub_modification_types**
   - Test different modification types
   - Args: `modification = 'ubiquitination'`, `organisms = 9606`
   - Check: Verify modification column contains ubiquitination

7. **enzsub_fields**
   - Test field selection
   - Args: `fields = c('curation_effort', 'references', 'sources')`
   - Check: Verify requested fields are present

8. **enzsub_multiple_resources**
   - Test resource combination queries
   - Args: `resources = c('PhosphoSite', 'SIGNOR')`
   - Check: Verify resources column contains specified resources

9. **enzsub_organism_mice**
   - Test mouse organism filtering
   - Args: `organisms = 10090`, `modification = 'phosphorylation'`
   - Check: Verify organism filtering

10. **enzsub_limit**
    - Test SQL LIMIT functionality
    - Args: `modification = 'phosphorylation'`, `limit = 100`
    - Check: Verify 100 or fewer rows returned

---

### Phase 5: Annotations Query Extension (Priority: MEDIUM)

**Current: 2 scenarios → Target: 8+ scenarios**

#### New Test Scenarios

1. **annotations_protein_specific**
   - Test proteins parameter
   - Args: `proteins = c('TP53', 'EGFR')`, `resources = 'UniProt_keyword'`
   - Check: Verify only requested proteins appear

2. **annotations_fields**
   - Test field selection
   - Args: `fields = c('value', 'source')`, `resources = 'UniProt_tissue'`
   - Check: Verify only requested fields present

3. **annotations_multiple_resources**
   - Test resource combinations
   - Args: `resources = c('UniProt_tissue', 'UniProt_keyword')`
   - Check: Verify both resources present

4. **annotations_entity_types**
   - Test different entity types
   - Args: `entity_types = 'mirna'`, `resources = 'miRBase'` (if available)
   - Check: Verify entity type filtering

5. **annotations_subcellular**
   - Test UniProt subcellular location
   - Args: `resources = 'UniProt_location'`, `genesymbols = TRUE`
   - Check: Verify subcellular location annotations

6. **annotations_go_terms**
   - Test GO annotations (if available)
   - Args: `resources = 'GO_'` (check available GO resources)
   - Check: Verify GO term structure

7. **annotations_pathway**
   - Test pathway annotations
   - Args: `resources = c('KEGG', 'Reactome')` (if available)
   - Check: Verify pathway annotation format

8. **annotations_limit**
   - Test SQL LIMIT functionality
   - Args: `resources = 'UniProt_keyword'`, `limit = 100`
   - Check: Verify 100 or fewer rows returned

---

### Phase 6: Complexes Query Extension (Priority: MEDIUM)

**Current: 1 scenario → Target: 6+ scenarios**

#### New Test Scenarios

1. **complexes_corum**
   - Test CORUM database
   - Args: `resources = 'CORUM'`
   - Check: Verify CORUM resource in results

2. **complexes_complexportal**
   - Test ComplexPortal database
   - Args: `resources = 'ComplexPortal'`
   - Check: Verify ComplexPortal annotations

3. **complexes_proteins**
   - Test protein-specific complex queries
   - Args: `proteins = c('TP53', 'MDM2')`
   - Check: Verify complexes containing specified proteins

4. **complexes_fields**
   - Test field selection
   - Args: `resources = 'CORUM'`, `fields = c('sources', 'databases')`
   - Check: Verify requested fields present

5. **complexes_multiple_resources**
   - Test resource combinations
   - Args: `resources = c('CORUM', 'ComplexPortal', 'hu.MAP')`
   - Check: Verify all resources present

6. **complexes_cellphonedb**
   - Test CellPhoneDB complexes
   - Args: `resources = 'CellPhoneDB'`
   - Check: Verify CellPhoneDB complex annotations
   - Tags: `full-db`

7. **complexes_limit**
   - Test SQL LIMIT functionality
   - Args: `resources = 'hu.MAP'`, `limit = 50`
   - Check: Verify 50 or fewer rows returned

---

### Phase 7: Cross-Cutting Concerns (Priority: MEDIUM)

**Actions for ALL query types:**

1. **Limit parameter testing**
   - Add limit parameter to representative scenarios
   - Verify SQL LIMIT clause correctly restricts result count
   - Note: This is NOT pagination, just SQL query limiting

2. **Format testing**
   - Test format=tsv on select scenarios
   - Verify TSV parsing works correctly with OmnipathR
   - Compare TSV vs JSON results for consistency

3. **Empty result handling**
   - Create scenarios with filters that return no results
   - Verify graceful handling of empty data frames
   - Check for proper error messages vs empty results

4. **Large result sets**
   - Test queries without filters (tag as 'full-db')
   - Monitor performance and memory usage
   - Verify data integrity on large responses

5. **Field selection validation**
   - Verify only requested fields are returned
   - Test field combinations across query types
   - Validate special fields like 'evidences' (JSON)

6. **genesymbols parameter**
   - Test TRUE vs FALSE consistently across query types
   - Verify gene symbol columns appear/disappear correctly
   - Check UniProt ID vs gene symbol consistency

---

### Phase 8: Enhanced Check Functions (Priority: HIGH)

**Implementation plan for validation helpers:**

1. Add helper functions section after the `parse_bool` function
2. Use helpers consistently across all scenarios
3. Create test-specific validators for complex checks
4. Document validation logic with comments

**Example enhanced scenario:**

```r
list(
    id = 'interactions_comprehensive_check',
    query = 'omnipath_interactions',
    description = 'Comprehensive validation of interaction query',
    args = list(
        organisms = 9606,
        datasets = 'omnipath',
        resources = 'SIGNOR',
        genesymbols = TRUE,
        fields = c('sources', 'references', 'is_directed', 'is_stimulation')
    ),
    check = function(result) {
        check_columns_exist(result, c('source', 'target', 'sources', 'references')) &&
        check_columns_exist(result, c('source_genesymbol', 'target_genesymbol')) &&
        check_resource_filter(result, 'SIGNOR') &&
        check_has_rows(result, min_rows = 1) &&
        check_boolean_column(result, 'is_directed', TRUE)
    },
    tags = c('smoke', 'validation')
)
```

---

### Phase 9: Error Condition Testing (Priority: LOW)

**Goal:** Verify graceful error handling

1. **Invalid parameters**
   - Test with invalid organism IDs (e.g., `organisms = 9999999`)
   - Test with non-existent resources
   - Verify appropriate error messages

2. **Malformed queries**
   - Test with incorrect parameter types (if not handled by OmnipathR)
   - Test with out-of-range values

3. **Empty filters**
   - Test contradictory parameter combinations
   - Verify empty result vs error distinction

4. **Missing required params**
   - Identify any required parameters
   - Test behavior when omitted

---

## Implementation Strategy

### Quick Wins (Week 1)
- [ ] Add validation helper functions to the script
- [ ] Add `check` functions to all existing 15 scenarios
- [ ] Implement 5 high-priority interactions scenarios
- [ ] Implement 5 high-priority intercell scenarios
- [ ] Run test suite and fix any failures

### Medium-term (Week 2-3)
- [ ] Complete all Phase 2 interactions extensions (15+ scenarios)
- [ ] Complete all Phase 3 intercell extensions (10+ scenarios)
- [ ] Implement Phase 4 enzsub scenarios (8+ scenarios)
- [ ] Implement Phase 5 annotations scenarios (8+ scenarios)
- [ ] Add cross-cutting limit tests

### Long-term (Week 4+)
- [ ] Complete Phase 6 complexes scenarios (6+ scenarios)
- [ ] Add cross-cutting tests (format, empty results, large datasets)
- [ ] Implement Phase 9 error condition tests
- [ ] Performance testing for large queries
- [ ] Documentation of test patterns and conventions

---

## Test Execution Strategy

### Enhanced Tag System

- **smoke** - Fast, critical path tests (5-8 scenarios, < 30 seconds total)
- **core** - Standard test suite (30-40 scenarios, < 5 minutes total)
- **comprehensive** - All parameter combinations (60+ scenarios, < 15 minutes)
- **full-db** - Tests requiring complete database (may be slow)
- **validation** - Tests with complex check functions
- **edge-case** - Error conditions and boundaries

### Command-line Usage Examples

```bash
# Quick validation (smoke tests only)
Rscript scripts/r-legacy-server-tests.R

# Core test suite
Rscript scripts/r-legacy-server-tests.R --scenario=*_core

# With full database
OMNIPATH_FULL_DB=1 Rscript scripts/r-legacy-server-tests.R

# Specific query type
Rscript scripts/r-legacy-server-tests.R --scenario=interactions_*

# List all available scenarios
Rscript scripts/r-legacy-server-tests.R --list-scenarios

# Run specific scenario by ID
Rscript scripts/r-legacy-server-tests.R --scenario=interactions_basic
```

### CI/CD Integration

1. **Pre-commit hook** - Run smoke tests (< 30 seconds)
2. **Pull request checks** - Run core tests (< 5 minutes)
3. **Nightly builds** - Run comprehensive + full-db tests
4. **Release validation** - Full test suite with error condition tests

---

## Expected Outcomes

| Metric | Current | Target |
|--------|---------|--------|
| Total test scenarios | 15 | 60+ |
| Parameter coverage | ~30% | ~85% |
| Data validation checks | 4 | 50+ |
| Query types with 10+ tests | 0 | 3 (interactions, intercell, enzsub) |
| Test execution time (core) | ~2 min | ~5 min |
| Confidence for production | Medium | High |

---

## Success Criteria for Production Deployment

Before deploying to production, ensure:

1. **Test Coverage**
   - [ ] All 5 query types have at least 6 test scenarios
   - [ ] All major API parameters are tested at least once
   - [ ] At least 50% of scenarios have validation checks

2. **Test Results**
   - [ ] 100% of smoke tests pass
   - [ ] 95%+ of core tests pass
   - [ ] All critical parameters validated

3. **Data Integrity**
   - [ ] Validation checks confirm correct filtering
   - [ ] Field selection works correctly
   - [ ] Organism filtering validated
   - [ ] Resource filtering validated

4. **Performance**
   - [ ] Core test suite completes in < 5 minutes
   - [ ] No memory issues with large queries
   - [ ] No timeout issues with complex queries

5. **Compatibility**
   - [ ] OmnipathR client handles all responses correctly
   - [ ] Results match old API (for regression testing)
   - [ ] Format variations (JSON/TSV) work correctly

---

## Notes and Considerations

- **Limit parameter:** Not pagination, just SQL LIMIT clause - useful for testing but not for production pagination needs
- **Full-db scenarios:** Should be clearly tagged and skippable for fast iteration
- **Cache management:** Reset OmnipathR cache between test runs to avoid false positives
- **Trace logging:** Use `OmnipathR:::.optrace()` for debugging
- **Parallel execution:** Consider parallelizing independent scenarios for faster CI/CD
- **Resource discovery:** Use metadata endpoints to discover available resources for test scenarios

---

## References

- **API Documentation:** https://r.omnipathdb.org/articles/bioc_workshop.html
- **Query Parameters:** https://omnipathdb.org/queries/{query_type}
- **OmnipathR Package:** https://github.com/saezlab/OmnipathR
- **Project README:** `README.md`
- **Contributor Guide:** `AGENTS.md`
- **Current Test Script:** `scripts/r-legacy-server-tests.R`
