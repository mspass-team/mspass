#!/bin/bash

# Install the package
${PYTHON} -m pip install . --no-deps -vv

# Debug block for intermittent pip resolver/index issues in CI.
echo "===== MSPASS CONDA BUILD DEBUG START ====="
${PYTHON} -V
${PYTHON} -m pip --version
${PYTHON} -m pip config debug || true
env | grep -E '^(PIP_|HTTP_PROXY|HTTPS_PROXY|NO_PROXY|CONDA|SSL_CERT_FILE|REQUESTS_CA_BUNDLE)=' || true
${PYTHON} -m pip index versions earthscope-sdk || true
echo "===== MSPASS CONDA BUILD DEBUG END ====="

# BEGIN_PIP_ONLY_DEPS
${PYTHON} -m pip install "earthscope-sdk"
# END_PIP_ONLY_DEPS
