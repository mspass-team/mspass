#!/bin/bash

# Install the package
${PYTHON} -m pip install . --no-deps -vv

# BEGIN_PIP_ONLY_DEPS
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "earthscope-sdk"
# END_PIP_ONLY_DEPS
