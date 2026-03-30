# MsPASS: Massive Parallel Analysis System for Seismologists

[![Docker Build](https://img.shields.io/github/actions/workflow/status/mspass-team/mspass/docker-publish.yml?branch=master&label=docker%20build&logo=docker)](https://github.com/mspass-team/mspass/actions/workflows/docker-publish.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/mspass/mspass?logo=docker)](https://hub.docker.com/r/mspass/mspass)
[![Docker Image Size](https://img.shields.io/docker/image-size/mspass/mspass/latest?label=docker%20image%20size&logo=docker)](https://hub.docker.com/r/mspass/mspass)
[![Conda Version](https://img.shields.io/conda/vn/mspass/mspasspy?label=Conda)](https://anaconda.org/mspass/mspasspy)
[![PyPI Version](https://img.shields.io/pypi/v/mspasspy?label=PyPI)](https://pypi.org/project/mspasspy/)

MsPASS is an open-source framework for scalable seismic data processing and data management. It combines:

- A parallel processing framework based on a scheduler/worker model (Dask and Spark integration)
- A MongoDB-centered data management model for waveform and metadata workflows
- A container-first runtime model for reproducible desktop, cluster, and cloud execution

For full user and API documentation, visit [mspass.org](https://www.mspass.org/).

## Table of Contents

- [How to Get MsPASS](#how-to-get-mspass)
- [Quick Start (Recommended: Docker)](#quick-start-recommended-docker)
- [Conda Installation (Alternative)](#conda-installation-alternative)
- [PyPI Package Status](#pypi-package-status)
- [Documentation](#documentation)
- [Development and Source Builds](#development-and-source-builds)
- [Project Links](#project-links)
- [Contributing](#contributing)
- [License](#license)

## How to Get MsPASS

MsPASS is distributed through multiple channels with different intended use cases:

1. **Docker (recommended for most users)**
	- Primary, fully provisioned runtime path
	- Published to Docker Hub: [mspass/mspass](https://hub.docker.com/r/mspass/mspass)
	- Also published to GitHub Container Registry: [ghcr.io/mspass-team/mspass](https://github.com/mspass-team/mspass/pkgs/container/mspass)

2. **Conda (alternative local package install)**
	- Published as `mspasspy` on Anaconda Cloud: [anaconda.org/mspass/mspasspy](https://anaconda.org/mspass/mspasspy)
	- Appropriate when you need a local Conda-managed environment

3. **PyPI (source distribution only)**
	- The PyPI release is a source distribution (sdist), not a prebuilt binary runtime
	- Best suited for packaging workflows and source-based consumers

## Quick Start (Recommended: Docker)

Install Docker Desktop (or Docker Engine on Linux), then pull the image:

```bash
docker pull mspass/mspass
```

Launch MsPASS in a project directory (Jupyter exposed on port `8888`):

```bash
docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass
```

Then open the Jupyter URL printed in the container logs (typically `http://127.0.0.1:8888/...`).

For repeated runs and multi-service operation, use Docker Compose. A baseline compose configuration is available in [data/yaml/compose.yaml](data/yaml/compose.yaml).

## Conda Installation (Alternative)

If you prefer Conda over containers:

```bash
conda create --name mspass_env
conda activate mspass_env
conda config --add channels mspass
conda config --add channels conda-forge
conda install -y mspasspy
```

Conda package: [anaconda.org/mspass/mspasspy](https://anaconda.org/mspass/mspasspy)

Note: many workflows still rely on MongoDB and are easiest to operate via the MsPASS Docker image, even when Python libraries are installed via Conda.

## PyPI Package Status

MsPASS publishes a **source distribution** to PyPI on tagged releases.

- This channel is intended for source consumption.
- It is not the recommended end-user runtime path.
- For the most complete and reproducible environment, use Docker.

## Documentation

- Documentation home: [www.mspass.org](https://www.mspass.org/)
- Running MsPASS on a desktop: [mspass_desktop](https://www.mspass.org/getting_started/mspass_desktop.html)
- Command-line Docker workflow: [command_line_desktop](https://www.mspass.org/getting_started/command_line_desktop.html)
- Deploy with Conda: [deploy_mspass_with_conda](https://www.mspass.org/getting_started/deploy_mspass_with_conda.html)
- Python API reference: [python_api](https://www.mspass.org/python_api/index.html)
- C++ API reference: [cxx_api](https://www.mspass.org/cxx_api/index.html)

## Development and Source Builds

For contributors and source builds:

- Build/setup guide: [Compiling MsPASS from source code](https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code)
- Contributor onboarding: [Get started instructions for contributors](https://github.com/mspass-team/mspass/wiki/Get-started-instructions-for-contributors)

## Project Links

For users interested in releases and package channels:

- Docker image: [Docker Hub](https://hub.docker.com/r/mspass/mspass)
- Conda package: [Anaconda Cloud](https://anaconda.org/mspass/mspasspy)
- Source package: [PyPI](https://pypi.org/project/mspasspy/)
- Container mirror: [GitHub Container Registry](https://github.com/mspass-team/mspass/pkgs/container/mspass)
- Source repository: [GitHub](https://github.com/mspass-team/mspass)

Maintainer and contributor automation (CI, packaging, release jobs) is implemented with GitHub Actions workflows in this repository.

## Contributing

Contributions are welcome. Please use issues and pull requests for bug reports, feature requests, and code changes.

Before opening a pull request:

1. Follow the contributor setup instructions in the project wiki.
2. Run relevant tests locally when possible.
3. Keep documentation in sync with user-facing behavior changes.

## License

This project is licensed under the **BSD 3-Clause License**. See [LICENSE](LICENSE) for details.
