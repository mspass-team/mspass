# syntax=docker/dockerfile:1
# Image: mspass/mspass
# Version: 1.0.0
#
# Build targets:
#   runtime - standard MsPASS image, also used by the default final stage
#   dev     - debug build with development and documentation dependencies
#   geolab  - GeoLab/Kubernetes image with non-root Jupyter runtime
#   mpi     - standard image built with the MPI base image
#             docker build --target mpi --build-arg MSPASS_BASE_IMAGE=ghcr.io/seisscoped/container-base:latest .
#   tacc    - standard runtime image with TACC interactive access enabled

ARG MSPASS_BASE_IMAGE=ghcr.io/seisscoped/container-base:ubuntu22.04_jupyterlab
ARG GEOLAB_BASE_IMAGE=ghcr.io/mspass-team/geolab-base-mirror@sha256:7aa0b713de225288188163c13519efce1ac5248ea386e734d88ad7c54b0abe27
ARG DASK_LABEXTENSION_VERSION=7.0.0
ARG SPARK_VERSION=3.0.0
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7
ARG SPARK_ARCHIVE=${SPARK_PACKAGE}.tgz
ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG SPARK_URL=${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}
ARG SPARK_SHA512=f5652835094d9f69eb3260e20ca9c2d58e8bdf85a8ed15797549a518b23c862b75a329b38d4248f8427e4310718238c60fae0f9d1afb3c70fb390d3e9cce2e49

FROM scratch AS spark-build-assets

FROM alpine:3.20 AS spark-archive

ARG SPARK_ARCHIVE
ARG SPARK_SHA512
ARG SPARK_URL

RUN --mount=type=bind,from=spark-build-assets,source=/,target=/mnt/build-assets,readonly \
    set -eux; \
    if [ -f "/mnt/build-assets/${SPARK_ARCHIVE}" ]; then \
        cp "/mnt/build-assets/${SPARK_ARCHIVE}" /spark.tgz; \
    else \
        apk add --no-cache ca-certificates curl; \
        curl -fL --retry 5 --retry-all-errors --retry-delay 10 \
            --connect-timeout 20 --speed-limit 1024 --speed-time 60 --max-time 900 \
            -o /spark.tgz "${SPARK_URL}"; \
    fi; \
    echo "${SPARK_SHA512}  /spark.tgz" | sha512sum -c -

FROM ${MSPASS_BASE_IMAGE} AS mspass-base

LABEL maintainer="Ian Wang <yinzhi.wang.cug@gmail.com>"

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN set -eux; \
	groupadd --gid 999 --system mongodb; \
	useradd --uid 999 --system --gid mongodb --home-dir /data/db mongodb; \
	mkdir -p /data/db /data/configdb; \
	chown -R mongodb:mongodb /data/db /data/configdb \
	&& docker-clean

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		ca-certificates \
		dirmngr \
		gnupg \
		jq \
		numactl \
		procps \
	; \
	rm -rf /var/lib/apt/lists/* \
	&& docker-clean

# grab "js-yaml" for parsing mongod's YAML config files (https://github.com/nodeca/js-yaml/releases)
ENV JSYAML_VERSION=3.13.1

RUN set -ex; \
	\
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		wget \
	; \
	rm -rf /var/lib/apt/lists/*; \
	\
	dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
	gpgconf --kill all; \
	\
	wget -O /js-yaml.js "https://github.com/nodeca/js-yaml/raw/${JSYAML_VERSION}/dist/js-yaml.js"; \
# TODO some sort of download verification here
	\
	apt-mark auto '.*' > /dev/null; \
	apt-mark manual $savedAptMark > /dev/null; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
	&& docker-clean

RUN mkdir /docker-entrypoint-initdb.d

RUN set -ex; \
	export GNUPGHOME="$(mktemp -d)"; \
	set -- '39BD841E4BE5FB195A65400E6A26B1AE64C3C388'; \
	for key; do \
		gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$key"; \
	done; \
	mkdir -p /etc/apt/keyrings; \
	gpg --batch --export "$@" > /etc/apt/keyrings/mongodb.gpg; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME" \
	&& docker-clean

# Allow build-time overrides (eg. to build image with MongoDB Enterprise version)
# Options for MONGO_PACKAGE: mongodb-org OR mongodb-enterprise
# Options for MONGO_REPO: repo.mongodb.org OR repo.mongodb.com
# Example: docker build --build-arg MONGO_PACKAGE=mongodb-enterprise --build-arg MONGO_REPO=repo.mongodb.com .
ARG MONGO_PACKAGE=mongodb-org
ARG MONGO_REPO=repo.mongodb.org
ENV MONGO_PACKAGE=${MONGO_PACKAGE} MONGO_REPO=${MONGO_REPO}

ENV MONGO_MAJOR=6.0
RUN echo "deb [ signed-by=/etc/apt/keyrings/mongodb.gpg ] http://$MONGO_REPO/apt/ubuntu jammy/${MONGO_PACKAGE%-unstable}/$MONGO_MAJOR multiverse" | tee "/etc/apt/sources.list.d/${MONGO_PACKAGE%-unstable}.list"

# https://docs.mongodb.org/master/release-notes/6.0/
ENV MONGO_VERSION=6.0.5
# 03/08/2023, https://github.com/mongodb/mongo/tree/c9a99c120371d4d4c52cbb15dac34a36ce8d3b1d

RUN set -x \
# installing "mongodb-enterprise" pulls in "tzdata" which prompts for input
	&& export DEBIAN_FRONTEND=noninteractive \
	&& apt-get update \
	&& apt-get install -y \
		${MONGO_PACKAGE}=$MONGO_VERSION \
		${MONGO_PACKAGE}-server=$MONGO_VERSION \
		${MONGO_PACKAGE}-shell=$MONGO_VERSION \
		${MONGO_PACKAGE}-mongos=$MONGO_VERSION \
		${MONGO_PACKAGE}-tools=$MONGO_VERSION \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/lib/mongodb \
	&& mv /etc/mongod.conf /etc/mongod.conf.orig \
	&& docker-clean

VOLUME /data/db /data/configdb

# ensure that if running as custom user that "mongosh" has a valid "HOME"
# https://github.com/docker-library/mongo/issues/524
ENV HOME=/data/db

COPY docker-entrypoint.sh /usr/local/bin/

EXPOSE 27017

RUN apt-get update \
    && apt-get install -y wget ssh rsync vim-tiny less \
       build-essential python3-setuptools \
       python3-dev python3-pip \
       openjdk-8-jdk \
       git cmake gfortran gdb \
       liblapack-dev libboost-dev libboost-serialization-dev libyaml-dev \
       zip unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
	&& docker-clean

ARG TARGETARCH

# Prepare the environment
ARG SPARK_VERSION
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-${TARGETARCH}
ENV SPARK_HOME=/usr/local/spark
ENV PYSPARK_PYTHON=python3

# Install Spark from the CI-provided build asset when present; otherwise the
# spark-archive stage downloads it with retries, speed limits, and checksum verification.
COPY --from=spark-archive /spark.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /usr/local/ \
    && cd /usr/local && ln -s ${SPARK_PACKAGE} spark \
    && rm /tmp/spark.tgz \
	&& docker-clean
RUN ln -s /usr/local/spark/bin/pyspark /usr/bin/pyspark
RUN python -c "import site; print(site.getsitepackages()[0])" > site_packages_path.txt && \
	PYTHON_SITE_PACKAGES_PATH=$(cat site_packages_path.txt) && \
	ln -s /usr/local/spark/python/pyspark ${PYTHON_SITE_PACKAGES_PATH}/pyspark && \
	unzip /usr/local/spark/python/lib/py4j-0.10.9-src.zip -d ${PYTHON_SITE_PACKAGES_PATH}/  \
	&& docker-clean

# Patch pyspark for machines don't have localhost defined in /etc/hosts
RUN sed -i 's/localhost/127.0.0.1/' /usr/local/spark/python/pyspark/accumulators.py
RUN unzip /usr/local/spark/python/lib/pyspark.zip \
    && sed -i 's/localhost/127.0.0.1/' ./pyspark/accumulators.py \
    && zip /usr/local/spark/python/lib/pyspark.zip pyspark/accumulators.py \
    && rm -r ./pyspark \
	&& docker-clean

# Install Python tooling
RUN pip3 --no-cache-dir install --upgrade pip \
	&& docker-clean
RUN if [ "$TARGETARCH" = "arm64" ]; then export CFLAGS="-O3" \
	&& export DISABLE_NUMCODECS_SSE2=true && export DISABLE_NUMCODECS_AVX2=true; fi \
	&& pip3 --no-cache-dir install numpy \
	&& docker-clean

# Download & install pybind11
ARG PYBIND11_VERSION=2.13.6
ARG PYBIND11_URL=https://github.com/pybind/pybind11/archive/v${PYBIND11_VERSION}.tar.gz
RUN wget -qO - ${PYBIND11_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local/pybind11-${PYBIND11_VERSION} \
    && mkdir build && cd build && cmake .. -DPYBIND11_TEST=OFF && make install && docker-clean
RUN rm -r /usr/local/pybind11-${PYBIND11_VERSION}

# Upgrade setuptools to enable namespace package
RUN pip3 --no-cache-dir install --upgrade setuptools \
	&& docker-clean

FROM mspass-base AS mspass-source

# Add source needed by the C++ build first so Python-only changes can reuse it.
ADD cxx /mspass/cxx
ENV MSPASS_HOME=/mspass

FROM mspass-source AS runtime-package

# Add cxx library
RUN ln -s /opt/conda/include/yaml-cpp /usr/include/yaml-cpp && cd /mspass/cxx \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && make install \
    && rm -rf ../build \
	&& docker-clean

# Install python components
ADD data /mspass/data
ADD setup.py /mspass/setup.py
ADD pyproject.toml /mspass/pyproject.toml
ADD python /mspass/python
ADD .git /mspass/.git
RUN pip3 install /mspass -v \
	&& rm -rf /mspass/build /mspass/.git && docker-clean

FROM mspass-source AS dev-package

# Add cxx library with debug symbols
RUN ln -s /opt/conda/include/yaml-cpp /usr/include/yaml-cpp && cd /mspass/cxx \
    && mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Debug .. \
    && make \
    && make install \
    && rm -rf ../build \
	&& docker-clean

# Add seisbench dependency in the dev container for development purpose
ADD data /mspass/data
ADD setup.py /mspass/setup.py
ADD pyproject.toml /mspass/pyproject.toml
ADD python /mspass/python
ADD .git /mspass/.git
RUN MSPASS_CMAKE_BUILD_TYPE=Debug pip3 install '/mspass[seisbench]' -v \
	&& rm -rf /mspass/build /mspass/.git && docker-clean

# Add docs and dependencies to build docs
ADD docs /mspass/docs
RUN apt-get update && apt-get install -yq doxygen pandoc \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install -r /mspass/docs/requirements.txt \
    && docker-clean

FROM runtime-package AS runtime-common

# Install jedi
RUN pip3 --no-cache-dir install jedi==0.17.2 && docker-clean

# Tini operates as a process subreaper for jupyter.
ARG TARGETARCH
ARG TINI_VERSION=v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH} /usr/sbin/tini
RUN chmod +x /usr/sbin/tini

RUN chmod +x /usr/local/bin/docker-entrypoint.sh
RUN sed -i '/set -- mongod "$@"/i [[ -d data ]] || mkdir data' /usr/local/bin/docker-entrypoint.sh

# replace localhost to 127.0.0.1 in pymongo to run on HPC
RUN python -c "import site; print(site.getsitepackages()[0])" > site_packages_path.txt && \
	PYTHON_SITE_PACKAGES_PATH=$(cat site_packages_path.txt) && \
	[ -f "${PYTHON_SITE_PACKAGES_PATH}/pymongo/encryption_options.py" ] && sed -i "s/localhost:27020,/127.0.0.1:27020,/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/encryption_options.py" || true && \
	[ -f "${PYTHON_SITE_PACKAGES_PATH}/pymongo/mongo_client.py" ] && sed -i 's/HOST = "localhost"/HOST = "127.0.0.1"/g' "${PYTHON_SITE_PACKAGES_PATH}/pymongo/mongo_client.py" || true && \
	[ -f "${PYTHON_SITE_PACKAGES_PATH}/pymongo/pool.py" ] && sed -i "s/'localhost'/'127.0.0.1'/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/pool.py" || true && \
	rm site_packages_path.txt

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Set the default behavior of this container
ENV SPARK_MASTER_PORT=7077
ENV DASK_SCHEDULER_PORT=8786
ENV MONGODB_PORT=27017
ENV JUPYTER_PORT=8888
ENV MSPASS_ROLE=all
# ENV MSPASS_SCHEDULER dask

FROM runtime-common AS runtime

ARG DASK_LABEXTENSION_VERSION

# install Dask JupyterLab extension
RUN pip3 install dask-labextension==${DASK_LABEXTENSION_VERSION} && docker-clean

# Add startup script
ADD scripts/start-mspass.sh /usr/sbin/start-mspass.sh
RUN chmod +x /usr/sbin/start-mspass.sh

ENTRYPOINT ["/usr/sbin/tini", "-s", "-g", "--", "/usr/sbin/start-mspass.sh"]

FROM runtime AS mpi

FROM ${GEOLAB_BASE_IMAGE} AS geolab

# Build a GeoLab-compatible image from the same Pangeo-style base contract used
# by EarthScope GeoLab.  Non-JupyterHub commands must pass through directly so
# Dask Gateway scheduler and worker pods can run the command selected by the
# Gateway server.
USER root

# Keep the base GeoLab uid/gid/user identity so mounted /home/jovyan workspaces
# and Dask Gateway scheduler/worker pods match the official GeoLab runtime.
ARG NB_USER=jovyan
ARG NB_UID=1000
ARG NB_GID=1000
ARG NB_HOME=/home/jovyan
ARG MONGO_MAJOR=8.0
ENV NB_USER=${NB_USER} \
    NB_UID=${NB_UID} \
    NB_GID=${NB_GID} \
    NB_HOME=${NB_HOME} \
    HOME=${NB_HOME} \
    MSPASS_WORK_DIR=${NB_HOME} \
    MSPASS_SCHEDULER=none \
    MSPASS_ENABLE_LOCAL_DASK=false \
    MSPASS_DB_ADDRESS=127.0.0.1 \
    MONGODB_PORT=27017 \
    DASK_SCHEDULER_PORT=8786 \
    PATH=/srv/conda/envs/notebook/bin:/srv/conda/condabin:/srv/conda/bin:${PATH}

RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        build-essential \
        cmake \
        gfortran \
        git \
        libblas-dev \
        libboost-dev \
        libboost-serialization-dev \
        libgsl-dev \
        liblapack-dev \
        libyaml-cpp-dev \
    ; \
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL "https://pgp.mongodb.com/server-${MONGO_MAJOR}.asc" \
        | gpg --dearmor -o "/etc/apt/keyrings/mongodb-server-${MONGO_MAJOR}.gpg"; \
    echo "deb [ arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/mongodb-server-${MONGO_MAJOR}.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/${MONGO_MAJOR} multiverse" \
        > "/etc/apt/sources.list.d/mongodb-org-${MONGO_MAJOR}.list"; \
    apt-get update; \
    apt-get install -y --no-install-recommends mongodb-org; \
    rm -rf /var/lib/apt/lists/*

ADD cxx /mspass/cxx
ADD data /mspass/data
ADD setup.py /mspass/setup.py
ADD pyproject.toml /mspass/pyproject.toml
ADD python /mspass/python
ADD .git /mspass/.git

RUN set -eux; \
    printf '%s\n' \
        'dask==2026.3.0' \
        'distributed==2026.3.0' \
        'dask-gateway==2026.3.0' \
        > /tmp/geolab-dask-constraints.txt; \
    /srv/conda/envs/notebook/bin/python -m pip install --no-cache-dir --upgrade pip setuptools wheel setuptools_scm; \
    /srv/conda/envs/notebook/bin/python -m pip install --no-cache-dir -v \
        --constraint /tmp/geolab-dask-constraints.txt /mspass; \
    /srv/conda/envs/notebook/bin/python -m pip install --no-cache-dir --force-reinstall --no-deps \
        dask==2026.3.0 \
        distributed==2026.3.0 \
        dask-gateway==2026.3.0; \
    /srv/conda/envs/notebook/bin/python -c "import importlib.metadata as md; expected={'dask':'2026.3.0','distributed':'2026.3.0','dask-gateway':'2026.3.0'}; actual={name: md.version(name) for name in expected}; print(actual); assert actual == expected, actual; import mspasspy; print(mspasspy.__file__)"; \
    rm -rf /mspass/build /mspass/.git /root/.cache /tmp/geolab-dask-constraints.txt

ADD scripts/start-mspass-geolab-entrypoint.sh /usr/sbin/start-mspass-geolab-entrypoint.sh
ADD scripts/start-mspass-geolab.sh /usr/sbin/start-mspass-geolab.sh
RUN chmod +x /usr/sbin/start-mspass-geolab-entrypoint.sh /usr/sbin/start-mspass-geolab.sh && \
    mkdir -p "${NB_HOME}" && \
    chown -R "${NB_UID}:${NB_GID}" "${NB_HOME}" /mspass

USER ${NB_UID}:${NB_GID}
WORKDIR ${NB_HOME}
ENTRYPOINT ["/usr/sbin/start-mspass-geolab-entrypoint.sh"]
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser"]

FROM dev-package AS dev

ARG DASK_LABEXTENSION_VERSION

# Install jedi
RUN pip3 --no-cache-dir install jedi==0.17.2 && docker-clean

# Tini operates as a process subreaper for jupyter.
ARG TARGETARCH
ARG TINI_VERSION=v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH} /usr/sbin/tini
RUN chmod +x /usr/sbin/tini

# Add startup script
ADD scripts/start-mspass.sh /usr/sbin/start-mspass.sh
RUN chmod +x /usr/sbin/start-mspass.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
RUN sed -i '/set -- mongod "$@"/i [[ -d data ]] || mkdir data' /usr/local/bin/docker-entrypoint.sh

# replace localhost to 127.0.0.1 in pymongo to run on HPC
RUN python -c "import site; print(site.getsitepackages()[0])" > site_packages_path.txt && \
	PYTHON_SITE_PACKAGES_PATH=$(cat site_packages_path.txt) && \
	[ -f "${PYTHON_SITE_PACKAGES_PATH}/pymongo/encryption_options.py" ] && sed -i "s/localhost:27020,/127.0.0.1:27020,/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/encryption_options.py" || true && \
	[ -f "${PYTHON_SITE_PACKAGES_PATH}/pymongo/mongo_client.py" ] && sed -i 's/HOST = "localhost"/HOST = "127.0.0.1"/g' "${PYTHON_SITE_PACKAGES_PATH}/pymongo/mongo_client.py" || true && \
	[ -f "${PYTHON_SITE_PACKAGES_PATH}/pymongo/pool.py" ] && sed -i "s/'localhost'/'127.0.0.1'/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/pool.py" || true && \
	rm site_packages_path.txt

# install Dask JupyterLab extension
RUN pip3 install dask-labextension==${DASK_LABEXTENSION_VERSION} && docker-clean

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Set the default behavior of this container
ENV SPARK_MASTER_PORT=7077
ENV DASK_SCHEDULER_PORT=8786
ENV MONGODB_PORT=27017
ENV JUPYTER_PORT=8888
ENV MSPASS_ROLE=all
# ENV MSPASS_SCHEDULER dask
ENTRYPOINT ["/usr/sbin/tini", "-s", "-g", "--", "/usr/sbin/start-mspass.sh"]

FROM runtime AS tacc

ENV MSPASS_TACC_MODE=true

FROM runtime AS final
