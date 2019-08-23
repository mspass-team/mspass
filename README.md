MsPASS is made available through a [dockerhub repo](https://hub.docker.com/r/wangyinz/mspass) that automatically builds with the Dockerfile here. 

# Getting MsPASS Running with Docker

# Getting MsPASS Running with Singularity (on HPC)

On machines that have Singularity setup. Use the following command to build the image as `mspass.simg` in current directory:

    singularity build mspass.simg docker://wangyinz/mspass

Before staring the MongoDB server, please make sure you have a dedicated directory created for the database files. Here we assume that to be `./data`. The command to start the mongoDB server for localhost only is:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork


Assuming current hostname is `node-1`, for a remote client to connect, start the server with:

    singularity exec mspass.simg mongod --dbpath ./data --logpath ./log --fork --bind_ip localhost,node-1

There should be two ports opened for TCP (one on localhost, one on current IP) with that on the default 27017 port. Use the `netstat -tulpn | grep LISTEN` command to check that.

Then, launch the client locally with:

    singularity exec mspass.simg mongo


To launch the client from a different node, simply `ssh` to the node and then:

    singularity exec mspass.simg mongo --host node-1:27017

To stop the mongoDB server, type the following command in a local mongo shell:

    use admin
    db.shutdownServer()
