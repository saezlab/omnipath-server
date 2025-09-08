#!/bin/bash

DIRECTORY="data/legacy"
N_LINES=20
QUERY_TYPES=(
    "interactions"
    "enzsub"
    "intercell"
    "complexes"
    "annotations"
)
SAMPLES=(
    "interactions,post_translational,omnipath;kinaseextra"
)


for query_type in ${QUERY_TYPES[@]}; do
    infile="$DIRECTORY/omnipath_webservice_$query_type.tsv.gz"
    outfile="$DIRECTORY/$query_type-sample.tsv"

    zcat $infile | head -n 1 > $outfile
done

for sample in ${SAMPLES[@]}; do

    IFS="," read -r query_type filter1 filter2 <<< "$sample"

    infile="$DIRECTORY/omnipath_webservice_$query_type.tsv.gz"
    outfile="$DIRECTORY/$query_type-sample.tsv"

    IFS=';' read -ra datasets <<< "$filter2"

    for dataset in "${datasets[@]}"; do
        zcat $infile | grep $filter1 | grep $dataset | shuf -n $N_LINES >> $outfile
    done

done
