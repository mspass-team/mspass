#!/bin/bash

# Create necessary directories
mkdir -p $SRC_DIR/cxx
mkdir -p $SRC_DIR/data
mkdir -p $SRC_DIR/python/mspasspy

# Copy necessary files and directories
cp -r cxx/* $SRC_DIR/cxx/
cp -r data/* $SRC_DIR/data/
cp -r python/mspasspy/* $SRC_DIR/python/mspasspy/
cp setup.py $SRC_DIR/
cp pyproject.toml $SRC_DIR/
cp requirements.txt $SRC_DIR/

