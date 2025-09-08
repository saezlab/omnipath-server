#!/bin/bash

SAMPLES=(
    "interactions,post_translational,omnipath;kinaseextra",
)

for sample in ${SAMPLES[@]}; do

    IFS="," read -r query_type filter1 filter2 <<< "$sample"

    for word in $filter2; do
        echo "$query_type, $filter1, $filter2"
    done

done
