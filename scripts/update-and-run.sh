#!/bin/bash

PKG_DIR=~/omnipath-server-git
OUTFILE=~/omnipath-new-server-output

mkdir -p "$PKG_DIR"
cd "$PKG_DIR"
rm -rf "$PKG_DIR/*"

git clone --branch deploy --depth 1 https://github.com/saezlab/omnipath-server.git "$PKG_DIR"
cp -p omnipath-server/tests/data/legacy/omnipath_server_licenses.tsv ~/

>> "$OUTFILE"
poetry install >> "$OUTFILE"
nohup poetry run python scripts/run-in-production.py &>> "$OUTFILE" &
