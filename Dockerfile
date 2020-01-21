#Image: wangyinz/mspass
#Version: 0.0.1

FROM mongo:4.2.0

MAINTAINER Ian Wang <yinzhi.wang.cug@gmail.com>

RUN apt-get update \
    && apt-get install -y wget ssh rsync vim-tiny less \
       build-essential python3-setuptools \
       python3-dev python3-pip \
       openjdk-8-jdk \
       git cmake gfortran gdb \
       liblapack-dev libboost-dev libyaml-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

RUN pip3 --no-cache-dir install pymongo

RUN mkdir /home/data

# Prepare the environment
ENV SPARK_VERSION 2.4.4
ENV SPARK_MASTER_PORT 7077

ENV MSPASS_ROLE master

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME /usr/local/spark
ENV PYSPARK_PYTHON python3

ENV DOCKERIZE_VERSION v0.6.1
ENV DOCKERIZE_URL https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz

ENV APACHE_MIRROR http://ftp.ps.pl/pub/apache
ENV SPARK_URL ${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz


# Download & install Dockerize
RUN wget -qO - ${DOCKERIZE_URL} | tar -xz -C /usr/local/bin 

# Download & install Spark
RUN wget -qO - ${SPARK_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 spark
RUN ln -s /usr/local/spark/bin/pyspark /usr/bin/pyspark

# Patch pyspark for machines don't have localhost defined in /etc/hosts
RUN sed -i 's/localhost/127.0.0.1/' /usr/local/spark/python/pyspark/accumulators.py

# Install obspy through pip
RUN pip3 --no-cache-dir install numpy \
    && pip3 --no-cache-dir install obspy

# Install pytest through pip
RUN pip3 --no-cache-dir install pytest

# Download & install pybind11
ENV PYBIND11_VERSION 2.4.3
ENV PYBIND11_URL https://github.com/pybind/pybind11/archive/v${PYBIND11_VERSION}.tar.gz
RUN wget -qO - ${PYBIND11_URL} | tar -xz -C /usr/local/ \
    && cd /usr/local/pybind11-${PYBIND11_VERSION} \
    && mkdir build && cd build && cmake .. && make install
RUN rm -r /usr/local/pybind11-${PYBIND11_VERSION}

# Upgrade setuptools to enable namespace package
RUN pip3 --no-cache-dir install --upgrade setuptools

# Add cxx library
ADD cxx /mspass/cxx
RUN cd /mspass/cxx \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && make install 

# Add setup.py to install python components
ADD setup.py /mspass/setup.py
ADD python /mspass/python
RUN pip3 install /mspass -vvv

# Add startup script
ADD scripts/start-mspass.sh /usr/sbin/start-mspass.sh
RUN chmod +x /usr/sbin/start-mspass.sh
RUN sed -i '/set -- mongod "$@"/i [[ -d data ]] || mkdir data' /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/sbin/start-mspass.sh"]
