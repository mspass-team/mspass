#!/bin/bash

# Install the package
${PYTHON} -m pip install . --no-deps -vv

# BEGIN_PIP_ONLY_DEPS
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "earthscope-sdk>=1.10.0"
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "earthscope-cli>=1.3.0"
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "typer-di>=0.1.3"
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "pymseed>=1.0.1,<2"
# END_PIP_ONLY_DEPS
