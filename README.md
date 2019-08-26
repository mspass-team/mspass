# Massive Parallel Analysis System for Seismologists 

MsPASS is made available through a [dockerhub repo](https://hub.docker.com/r/wangyinz/mspass) that automatically builds with the Dockerfile here. 

We only have the MongoDB component working right now.

## Getting MongoDB Running with Docker

To install Docker on machines that you have root access, please refer to the guide [here](https://docs.docker.com/v17.12/docker-for-mac/install/). For HPC systems, please refer to the next section and use Singularity instead.

Once you have docker setup properly, use the following command in a terminal to pull the docker image to your local machine:

    docker pull wangyinz/mspass
    
Then, `cd` to the dictory that you want to hold the database related files stored, and create a `data` directory if it does not already exist. Use this command to start the MongoDB server: 

    docker run --name MsPASS -d -p 27017:27017 --mount src="$(pwd)",target=/home,type=bind wangyinz/mspass mongod --dbpath /home/data --logpath /home/log

* The `--name` option will give the launched container instance a name `MsPASS`. 
* The `-d` will let the container run as a daemon so that the process will be kept in the background. 
* The `-p` is used to map the host port to the container port. `27017` is the default for MongoDB. It is not necessary if all MongoDB communications will be within the container.
* The `--mount` option will bind current directory to the `/home/` in the container, so the database files will be kept outside of the container. This directory is later used in the `mongod` options to specify the database files and logs with the `--dbpath` and `--logpath` options.

You may have to wait for a couple seconds for the MongoDB server to initialize. Then, you can launch the MongoDB client with:

    docker exec -it MsPASS mongo
    
It will launch the mongo shell within the `MsPASS` container created from previous command. The `-i` and `-t` specifies a interactive pseudo-TTY session. 

To stop the mongoDB server, type the following command in the mongo shell:

    use admin
    db.shutdownServer()
    
and then remove the container with:

    docker rm MsPASS

## Getting MongoDB Running with Singularity (on HPC)

On machines that have Singularity setup. Use the following command to build the image as `mspass.simg` in current directory:

    singularity build mspass.simg docker://wangyinz/mspass

Before staring the MongoDB server, please make sure you have a dedicated directory created for the database files. Here we assume that to be `./data`. The command to start the mongoDB server for localhost only is:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork


Assuming current hostname is `node-1`, for a remote client to connect, start the server with:
FIX THIS:  what 'node-1' means is not at all clear.   This command failed at IU

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork --bind_ip localhost,node-1

There should be two ports opened for TCP (one on localhost, one on current IP) with that on the default 27017 port. Use the `netstat -tulpn | grep LISTEN` command to check that.

Then, launch the client locally with:

    singularity exec mspass.simg mongo


To launch the client from a different node, simply `ssh` to the node and then:

    singularity exec mspass.simg mongo --host node-1:27017

To stop the mongoDB server, type the following command in a local mongo shell:

    use admin
    db.shutdownServer()
