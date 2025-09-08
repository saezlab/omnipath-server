#!/bin/bash

PKG_DIR=~/omnipath-server-git
OUTFILE=~/omnipath-new-server-output

mkdir -p "$PKG_DIR"
cd "$PKG_DIR"
rm -rf "$PKG_DIR"/*
rm -rf "$PKG_DIR"/.*

git clone --branch deploy --depth 1 https://github.com/saezlab/omnipath-server.git ./
cp -p tests/data/legacy/omnipath_webservice_licenses.tsv ./

>> "$OUTFILE"
poetry install >> "$OUTFILE"
ln -s ../omnipath_webservice_annotations.tsv.gz
ln -s ../omnipath_webservice_complexes.tsv.gz
ln -s ../omnipath_webservice_enzsub.tsv.gz
ln -s ../omnipath_webservice_interactions.tsv.gz
ln -s ../omnipath_webservice_intercell.tsv.gz
nohup poetry run python scripts/run-in-production.py &>> "$OUTFILE" &
