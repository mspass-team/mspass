Start the mongoDB server for localhost only:

```
./mongodb-linux-x86_64-rhel70-4.2.0/bin/mongod --dbpath /work/06058/iwang/stampede2/learn/mongodb/data --logpath /work/06058/iwang/stampede2/learn/mongodb/log --fork
```

Then, launch the client with:

```
./mongodb-linux-x86_64-rhel70-4.2.0/bin/mongo
```

Assuming current hostname is c455-012, for a remote client to connect, start the server with:

```
./mongodb-linux-x86_64-rhel70-4.2.0/bin/mongod --dbpath /work/06058/iwang/stampede2/learn/mongodb/data --logpath /work/06058/iwang/stampede2/learn/mongodb/log --fork --bind_ip localhost,c455-012
```

There should be two ports opened for TCP (one on localhost, one on current IP) with that on the default 27017 port. Use the `netstat -tulpn | grep LISTEN` command to check that.

To launch the client from a different node, simply `ssh` to the node and then:

```
./mongodb-linux-x86_64-rhel70-4.2.0/bin/mongo --host c455-012:27017
```

To stop the mongoDB server, type the following command in the mongo Shell:

```
use admin
db.shutdownServer()
```
