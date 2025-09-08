#!/bin/bash

N_LINES=20
SAMPLES=(
    "interactions,post_translational,omnipath;kinaseextra",
)

for sample in ${SAMPLES[@]}; do

    IFS="," read -r query_type filter1 filter2 <<< "$sample"

    INFILE="data/legacy/omnipath_webservice_$query_type.tsv"
    OUTFILE="data/legacy/sample_$query_type.tsv"

    IFS=';' read -ra datasets <<< "$filter2"

    for dataset in "${datasets[@]}"; do
        grep $filter1 $INFILE | grep $dataset | shuf -n $N_LINES >> $OUTFILE
        # TODO: >> only appends, make sure to create new one
        # TODO: add header
    done

done
