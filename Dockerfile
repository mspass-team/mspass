#Image: mspass/mspass
#Version: 0.0.1

FROM ghcr.io/seisscoped/container-base:ubuntu22.04_jupyterlab

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
ENV JSYAML_VERSION 3.13.1

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

ENV MONGO_MAJOR 6.0
RUN echo "deb [ signed-by=/etc/apt/keyrings/mongodb.gpg ] http://$MONGO_REPO/apt/ubuntu jammy/${MONGO_PACKAGE%-unstable}/$MONGO_MAJOR multiverse" | tee "/etc/apt/sources.list.d/${MONGO_PACKAGE%-unstable}.list"

# https://docs.mongodb.org/master/release-notes/6.0/
ENV MONGO_VERSION 6.0.5
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
ENV HOME /data/db

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
ARG SPARK_VERSION=3.0.0

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-${TARGETARCH}
ENV SPARK_HOME /usr/local/spark
ENV PYSPARK_PYTHON python3

ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG SPARK_URL=${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

# Download & install Spark
RUN wget -qO - ${SPARK_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 spark \
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

# Install Python dependencies through pip
ADD requirements.txt requirements.txt
RUN pip3 --no-cache-dir install --upgrade pip \
	&& docker-clean
RUN if [ "$TARGETARCH" == "arm64" ]; then export CFLAGS="-O3" \
	&& export DISABLE_NUMCODECS_SSE2=true && export DISABLE_NUMCODECS_AVX2=true; fi\
	&& pip3 --no-cache-dir install numpy \
    && pip3 --no-cache-dir install -r requirements.txt \
    && rm -f requirements.txt \
	&& docker-clean

# Download & install pybind11
ARG PYBIND11_VERSION=2.6.0
ARG PYBIND11_URL=https://github.com/pybind/pybind11/archive/v${PYBIND11_VERSION}.tar.gz
RUN wget -qO - ${PYBIND11_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local/pybind11-${PYBIND11_VERSION} \
    && mkdir build && cd build && cmake .. -DPYBIND11_TEST=OFF && make install && docker-clean
RUN rm -r /usr/local/pybind11-${PYBIND11_VERSION}

# Upgrade setuptools to enable namespace package
RUN pip3 --no-cache-dir install --upgrade setuptools \
	&& docker-clean

# Add cxx library
ADD cxx /mspass/cxx
RUN ln -s /opt/conda/include/yaml-cpp /usr/include/yaml-cpp && cd /mspass/cxx \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && make install \ 
    && rm -rf ../build \
	&& docker-clean

# Add data and env variable for the MetadataDefinition class
ADD data /mspass/data
ENV MSPASS_HOME /mspass

# Add setup.py to install python components
ADD setup.py /mspass/setup.py
ADD pyproject.toml /mspass/pyproject.toml
ADD python /mspass/python
RUN pip3 install /mspass -v \
	&& docker-clean

# Install jedi
RUN pip3 --no-cache-dir install jedi==0.17.2 && docker-clean

# Tini operates as a process subreaper for jupyter.
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
	sed -i "s/localhost:27020,/127.0.0.1:27020,/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/encryption_options.py" && \
	sed -i 's/HOST = "localhost"/HOST = "127.0.0.1"/g' "${PYTHON_SITE_PACKAGES_PATH}/pymongo/mongo_client.py" && \
	sed -i "s/'localhost'/'127.0.0.1'/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/settings.py" && \
	sed -i "s/'localhost'/'127.0.0.1'/g" "${PYTHON_SITE_PACKAGES_PATH}/pymongo/pool.py" && \
	rm site_packages_path.txt

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Set the default behavior of this container
ENV SPARK_MASTER_PORT 7077
ENV DASK_SCHEDULER_PORT 8786
ENV MONGODB_PORT 27017
ENV JUPYTER_PORT 8888
ENV MSPASS_ROLE all
# ENV MSPASS_SCHEDULER dask
ENTRYPOINT ["/usr/sbin/tini", "-s", "-g", "--", "/usr/sbin/start-mspass.sh"]
