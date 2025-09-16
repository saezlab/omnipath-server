#!/bin/bash

IN_DIRECTORY="data/legacy"
OUT_DIRECTORY="tests/data/legacy"
N_LINES=20
QUERY_TYPES=(
    "interactions"
    "enzsub"
    "intercell"
    "complexes"
    "annotations"
)
SAMPLES=(
    "interactions,post_translational,omnipath;kinaseextra;pathwayextra;ligrecextra"
    "interactions,post_transcriptional"
    "interactions,transcriptional,collectri;dorothea"
    "enzsub"
    "intercell,functional,composite;resource_specific"
    "intercell,locational,composite;resource_specific"
    "complexes"
)
#TODO: fix DBs with empty args + annotations not being populated

for query_type in ${QUERY_TYPES[@]}; do
    infile="$IN_DIRECTORY/omnipath_webservice_$query_type.tsv.gz"
    outfile="$OUT_DIRECTORY/omnipath_webservice_$query_type.tsv"

    zcat $infile | head -n 1 > $outfile
done

for sample in ${SAMPLES[@]}; do

    IFS="," read -r query_type filter1 filter2 <<< "$sample"

    infile="$IN_DIRECTORY/omnipath_webservice_$query_type.tsv.gz"
    outfile="$OUT_DIRECTORY/omnipath_webservice_$query_type.tsv"

    if [[ -z $filter2 ]]; then
        if [[ -z $filter1 ]]; then
            zcat $infile | shuf -n $N_LINES >> $outfile
        else
            zcat $infile | grep $filter1 | shuf -n $N_LINES >> $outfile
        fi
    else
        IFS=';' read -ra datasets <<< "$filter2"

        for dataset in "${datasets[@]}"; do
            zcat $infile | grep $filter1 | grep $dataset | shuf -n $N_LINES >> $outfile
        done
    fi

done

# Annotations case
annotation_resources=(
    "MSigDB"
    "UniProt"
    "PROGENy"
    "CellPhoneDB"
)
for resource in ${annotation_resources[@]}; do

    infile="$IN_DIRECTORY/omnipath_webservice_annotations.tsv.gz"
    outfile="$OUT_DIRECTORY/omnipath_webservice_annotations.tsv"

    zcat $infile | grep $resource | grep -P '\t(12|43|57)$' >> $outfile

done
