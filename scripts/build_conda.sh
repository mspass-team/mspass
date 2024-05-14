#!/bin/bash

# Install the package
${PYTHON} -m pip install . -vv

rm -r cxx/test
rm -r python/tests
rm -r docs
rm -r scripts
rm -r .github
rm Dockerfile*
rm docker-*
