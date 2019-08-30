# Massive Parallel Analysis System for Seismologists 

MsPASS is made available through a [dockerhub repo](https://hub.docker.com/r/wangyinz/mspass) that automatically builds with the Dockerfile here. 

We have the MongoDB and Spark components working for now.

## Using MsPASS with Docker

To install Docker on machines that you have root access, please refer to the guide [here](https://docs.docker.com/v17.12/docker-for-mac/install/). For HPC systems, please refer to the following section and use Singularity instead.

Once you have docker setup properly, use the following command in a terminal to pull the docker image to your local machine:

    docker pull wangyinz/mspass

### Getting MongoDB Running with Docker

After pulling the docker image, `cd` to the directory that you want to hold the database related files stored, and create a `data` directory with `mkdir data` if it does not already exist. Use this command to start the MongoDB server: 

    docker run --name MsPASS -d -p 27017:27017 --mount src="$(pwd)",target=/home,type=bind wangyinz/mspass mongod --dbpath /home/data --logpath /home/log

* The `--name` option will give the launched container instance a name `MsPASS`. 
* The `-d` will let the container run as a daemon so that the process will be kept in the background. 
* The `-p` is used to map the host port to the container port. `27017` is the default for MongoDB. It is not necessary if all MongoDB communications will be within the container.
* The `--mount` option will bind current directory to the `/home/` in the container, so the database files will be kept outside of the container. This directory is later used in the `mongod` options to specify the database files and logs with the `--dbpath` and `--logpath` options.

You may have to wait for a couple seconds for the MongoDB server to initialize. Then, you can launch the MongoDB client with:

    docker exec -it MsPASS mongo
    
It will launch the mongo shell within the `MsPASS` container created from previous command. The `-i` and `-t` specifies an interactive pseudo-TTY session. 

To stop the mongoDB server, type the following commands in the mongo shell:

    use admin
    db.shutdownServer()
    
and then remove the container with:

    docker rm MsPASS

### Getting Spark and MongoDB Running with Docker

We will use the `docker-compose` command to launch two container instances that compose a Spark standalone cluster. One is called `mspass-master` that runs the MongoDB server and Spark master, and the other is called `mspass-worker` that runs a Spark worker. Both containers will be running on the same machine in this setup.

Fisrt, pull the docker image. Then, create a `data` directory to hold the MongoDB database files if it does not already exist. Assume you are working in the root directory of this repository, run the following command to bring up the two container instances:

    docker-compose up -d

* The `-d` will let the containers run as daemons so that the processes will be kept in the background.

To launch the containers in a different directory, `cd` to that directory and create a `data` directory there. Then, you need to explicitly point the command to the `docker-compose.yml` file:

    docker-compose -f path_to_MsPASS/docker-compose.yml up -d

Once the containers are running, you will see several log files from MongoDB and Spark created in the current directory. Since we have the port mapping feature of Docker enabled, you can also open `localhost:8080` in your browser to check the status of Spark through the master’s web UI, where you should see the worker is listed a ALIVE. Note that the links to the worker will not work due to the container's network setup.

First, we want to make sure the Spark cluster is setup and running correctly. This can be done running the pi calculation example within the Spark distribution. To submit the example from `mspass-master`, use:

    docker exec mspass-master /usr/local/spark/bin/run-example --master spark://mspass-master:7077 SparkPi 10

to submit it from `mspass-worker`, use:

    docker exec mspass-worker /usr/local/spark/bin/run-example --master spark://mspass-master:7077 SparkPi 10

* The `docker exec` will run the command within the `mspass-master` or `mspass-worker` container. 
* The `--master` option specifies the Spark master, which is `mspass-master` in our case. The `7077` is the default port of Spark master.

The output of this example is very verbose, but you should see a line of `Pi is roughly 3.141...` near the end of the stdout, which is the result of the calculation. You should also see the jobs in the Running Applications or Completed Applications session at `localhost:8080`.

To launch an interactive mongo shell within `mspass-master`, use: 

    docker exec -it mspass-master mongo

To access the MongoDB server from `mspass-worker`, use:

    docker exec -it mspass-worker mongo --host mspass-master

* The `-it` option opens an interactive pseudo-TTY session
* The `--host` option will direct the client to the server running on `mspass-master`.

To launch an interactive Python session to run Spark jobs, use the pyspark command through `mspass-master`:

    docker exec -it mspass-master pyspark \
      --conf "spark.mongodb.input.uri=mongodb://mspass-master/test.myCollection?readPreference=primaryPreferred" \
      --conf "spark.mongodb.output.uri=mongodb://mspass-master/test.myCollection" \
      --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1
      
or through `mspass-worker`:

    docker exec -it mspass-worker pyspark \
      --conf "spark.mongodb.input.uri=mongodb://mspass-master/test.myCollection?readPreference=primaryPreferred" \
      --conf "spark.mongodb.output.uri=mongodb://mspass-master/test.myCollection" \
      --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1

* The two `--conf` options specify the input and output database collections. The MongoDB server is running on `mspass-master`, so the url should point to that on both cases. Please substitute `test` and `myCollection` with the database name or collections name desired. 
* The `--packages` option will setup the MongoDB Spark connector environment in this Python session.

Please refer to [this documentation](https://docs.mongodb.com/spark-connector/master/python-api/) for more details about the MongoDB Spark connector.

To bring down the containers, run:

    docker-compose down
    
or

    docker-compose -f path_to_MsPASS/docker-compose.yml down

## Using MsPASS with Singularity (on HPC)

### Getting MongoDB Running with Singularity on a Single Node

On machines that have Singularity setup. Use the following command to build the image as `mspass.simg` in current directory:

    singularity build mspass.simg docker://wangyinz/mspass

Before starting the MongoDB server, please make sure you have a dedicated directory created for the database files. Here we assume that to be `./data`. The command to start the mongoDB server for localhost only is:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork

Then, launch the client locally with:

    singularity exec mspass.simg mongo

To stop the mongoDB server, type the following command in the mongo shell:

    use admin
    db.shutdownServer()

### Getting MongoDB Running with Singularity on Multiple Nodes

First, request a interactive session with more than one node. Below we assume the hostname (output of the `hostname` command) of the two nodes requested are `node-1` and `node-2`. Please make sure to change the names according to your system setup.

Assuming we want to have the MongoDB server running on `node-1`, for a remote client to connect, start the server with:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork --bind_ip localhost,node-1

There should be two 27017 ports opened for TCP (one on localhost, one on current IP). 27017 is the default of MongoDB. Use the `netstat -tulpn | grep LISTEN` command to check that.

To launch the client from `node-2`, simply `ssh node-2` to get to that node and then:

    singularity exec mspass.simg mongo --host node-1

It will connect to `node-1` on port 27017 by default.

To stop the mongoDB server, type the following command in mongo shell on `node-1`:

    use admin
    db.shutdownServer()
