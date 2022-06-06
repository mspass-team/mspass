#Image: mspass/mspass
#Version: 0.0.1

FROM mongo:4.4.0

LABEL maintainer="Ian Wang <yinzhi.wang.cug@gmail.com>"

RUN apt-get update \
    && apt-get install -y wget ssh rsync vim-tiny less \
       build-essential python3-setuptools \
       python3-dev python3-pip \
       openjdk-8-jdk \
       git cmake gfortran gdb \
       liblapack-dev libboost-dev libboost-serialization-dev libyaml-dev \
       zip unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

# Prepare the environment
ENV SPARK_VERSION 3.0.0

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME /usr/local/spark
ENV PYSPARK_PYTHON python3

ENV DOCKERIZE_VERSION v0.6.1
ENV DOCKERIZE_URL https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz

ENV APACHE_MIRROR https://archive.apache.org/dist
ENV SPARK_URL ${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz


# Download & install Dockerize
RUN wget -qO - ${DOCKERIZE_URL} | tar -xz -C /usr/local/bin 

# Download & install Spark
RUN wget -qO - ${SPARK_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 spark
RUN ln -s /usr/local/spark/bin/pyspark /usr/bin/pyspark
RUN ln -s /usr/local/spark/python/pyspark /usr/local/lib/python3.6/dist-packages/pyspark
RUN unzip /usr/local/spark/python/lib/py4j-0.10.9-src.zip -d /usr/local/lib/python3.6/dist-packages/

# Patch pyspark for machines don't have localhost defined in /etc/hosts
RUN sed -i 's/localhost/127.0.0.1/' /usr/local/spark/python/pyspark/accumulators.py
RUN unzip /usr/local/spark/python/lib/pyspark.zip \
    && sed -i 's/localhost/127.0.0.1/' ./pyspark/accumulators.py \
    && zip /usr/local/spark/python/lib/pyspark.zip pyspark/accumulators.py \
    && rm -r ./pyspark

# Install Python dependencies through pip
ADD requirements.txt requirements.txt
RUN pip3 --no-cache-dir install --upgrade pip
RUN pip3 --no-cache-dir install numpy \
    && pip3 --no-cache-dir install -r requirements.txt \
    && rm -f requirements.txt

# Download & install pybind11
ENV PYBIND11_VERSION 2.6.0
ENV PYBIND11_URL https://github.com/pybind/pybind11/archive/v${PYBIND11_VERSION}.tar.gz
RUN wget -qO - ${PYBIND11_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local/pybind11-${PYBIND11_VERSION} \
    && mkdir build && cd build && cmake .. -DPYBIND11_TEST=OFF && make install
RUN rm -r /usr/local/pybind11-${PYBIND11_VERSION}

# Upgrade setuptools to enable namespace package
RUN pip3 --no-cache-dir install --upgrade setuptools

# Add cxx library
ADD cxx /mspass/cxx
RUN cd /mspass/cxx \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && make install \ 
    && rm -rf ../build

# Add data and env variable for the MetadataDefinition class
ADD data /mspass/data
ENV MSPASS_HOME /mspass

# Add setup.py to install python components
ADD setup.py /mspass/setup.py
ADD python /mspass/python
RUN pip3 install /mspass -v

# Install Jupyter notebook
RUN pip3 --no-cache-dir install jedi==0.17.2 notebook==6.2.0

# Tini operates as a process subreaper for jupyter.
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/sbin/tini
RUN chmod +x /usr/sbin/tini

# Add startup script
ADD scripts/start-mspass.sh /usr/sbin/start-mspass.sh
RUN chmod +x /usr/sbin/start-mspass.sh
RUN sed -i '/set -- mongod "$@"/i [[ -d data ]] || mkdir data' /usr/local/bin/docker-entrypoint.sh

# Set the default behavior of this container
ENV SPARK_MASTER_PORT 7077
ENV DASK_SCHEDULER_PORT 8786
ENV MONGODB_PORT 27017
ENV JUPYTER_PORT 8888
ENV MSPASS_ROLE all
ENV MSPASS_SCHEDULER dask

ENTRYPOINT ["/usr/sbin/tini", "-g", "--", "/usr/sbin/start-mspass.sh"]
