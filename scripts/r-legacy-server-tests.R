#!/usr/bin/env Rscript

library(OmnipathR)
library(purrr)

options(omnipathr.url = 'http://localhost:44444')
.optrace()

args <- c(
    interactions = list(
        organisms = c(9606, 10090, 10116),
        genesymbols = list('yes', 'no', TRUE, FALSE, 1, 0),
        datasets = c('omnipath', 'collectri')
    ),
    annotations = list(),
    complexes = list(),
    enzsub = list(),
    intercell = list(),
)


#purrr::cross