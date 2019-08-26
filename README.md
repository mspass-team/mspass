# Massive Parallel Analysis System for Seismologists 

MsPASS is made available through a [dockerhub repo](https://hub.docker.com/r/wangyinz/mspass) that automatically builds with the Dockerfile here. 

We only have the MongoDB component working right now.

## Getting MongoDB Running with Docker

To install Docker on machines that you have root access, please refer to the guide [here](https://docs.docker.com/v17.12/docker-for-mac/install/). For HPC systems, please refer to the next section and use Singularity instead.

Once you have docker setup properly, use the following command in a terminal to pull the docker image to your local machine:

    docker pull wangyinz/mspass
    
Then, `cd` to the directory that you want to hold the database related files stored, and create a `data` directory if it does not already exist. Use this command to start the MongoDB server: 

    docker run --name MsPASS -d -p 27017:27017 --mount src="$(pwd)",target=/home,type=bind wangyinz/mspass mongod --dbpath /home/data --logpath /home/log

* The `--name` option will give the launched container instance a name `MsPASS`. 
* The `-d` will let the container run as a daemon so that the process will be kept in the background. 
* The `-p` is used to map the host port to the container port. `27017` is the default for MongoDB. It is not necessary if all MongoDB communications will be within the container.
* The `--mount` option will bind current directory to the `/home/` in the container, so the database files will be kept outside of the container. This directory is later used in the `mongod` options to specify the database files and logs with the `--dbpath` and `--logpath` options.

You may have to wait for a couple seconds for the MongoDB server to initialize. Then, you can launch the MongoDB client with:

    docker exec -it MsPASS mongo
    
It will launch the mongo shell within the `MsPASS` container created from previous command. The `-i` and `-t` specifies a interactive pseudo-TTY session. 

To stop the mongoDB server, type the following commands in the mongo shell:

    use admin
    db.shutdownServer()
    
and then remove the container with:

    docker rm MsPASS

## Getting MongoDB Running with Singularity on a Single Node

On machines that have Singularity setup. Use the following command to build the image as `mspass.simg` in current directory:

    singularity build mspass.simg docker://wangyinz/mspass

Before staring the MongoDB server, please make sure you have a dedicated directory created for the database files. Here we assume that to be `./data`. The command to start the mongoDB server for localhost only is:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork

Then, launch the client locally with:

    singularity exec mspass.simg mongo

To stop the mongoDB server, type the following command in the mongo shell:

    use admin
    db.shutdownServer()

## Getting MongoDB Running with Singularity on Multiple Nodes

First, request a interactive session with more than one node. Below we assume the hostname (output of the `hostname` command) of the two nodes requested are `node-1` and `node-2`. Please make sure to change the names according to your system setup.

Assuming we want to have the MongoDB server running on `node-1`, for a remote client to connect, start the server with:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork --bind_ip localhost,node-1

There should be two 27017 ports opened for TCP (one on localhost, one on current IP). 27017 is the default of MongoDB. Use the `netstat -tulpn | grep LISTEN` command to check that.

To launch the client from `node-2`, simply `ssh node-2` to get to that node and then:

    singularity exec mspass.simg mongo --host node-1:27017

To stop the mongoDB server, type the following command in mongo shell on `node-1`:

    use admin
    db.shutdownServer()
