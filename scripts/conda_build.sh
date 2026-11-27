#!/bin/bash
set -e

if [[ "$(uname -s)" == Linux ]]; then
    # Boost's GCC bootstrap clears CXX and looks up g++ by name.
    # Use the activated conda compiler for both Boost and MsPASS.
    export MSPASS_CONDA_CXX
    MSPASS_CONDA_CXX="$(command -v "${CXX:?Conda C++ compiler is not set}")"
    boost_bin="$(mktemp -d)"
    trap 'rm -rf -- "$boost_bin"' EXIT
    cat > "$boost_bin/g++" <<'COMPILER'
#!/bin/bash
exec "$MSPASS_CONDA_CXX" "$@"
COMPILER
    chmod +x "$boost_bin/g++"
    export PATH="$boost_bin:$PATH"
fi

# Install the package
${PYTHON} -m pip install . --no-deps -vv

# BEGIN_PIP_ONLY_DEPS
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "earthscope-sdk>=1.10.0"
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "earthscope-cli>=1.3.0"
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "typer-di>=0.1.3"
PIP_NO_INDEX=False ${PYTHON} -m pip install --no-deps "pymseed>=1.0.1,<2"
# END_PIP_ONLY_DEPS
