#!/bin/bash

# Install the package
${PYTHON} -m pip install . -vv

# BEGIN_PIP_ONLY_DEPS
${PYTHON} -m pip install "earthscope-sdk"
# END_PIP_ONLY_DEPS
