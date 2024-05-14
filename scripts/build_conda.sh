#!/bin/bash

# Install the package
${PYTHON} -m pip install . -vv

rm -r $SRC_DIR/cxx/test
rm -r $SRC_DIR/python/tests
rm -r $SRC_DIR/docs
rm -r $SRC_DIR/scripts
rm -r $SRC_DIR/.github
rm $SRC_DIR/Dockerfile*
rm $SRC_DIR/docker-*
