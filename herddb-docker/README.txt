Running single instance HerdDB

(foreground mode)
docker run --rm -it  -e server.port=7000 -p 7000:7000 -e server.advertised.host=$(hostname)  --name local-herddb herd:latest

(daemon mode)
docker run -d -e server.port=7000 -p 7000:7000 -e server.advertised.host=$(hostname)  --name local-herddb herd:latest

# in order to access local cli (inside the container)
docker exec -it local-herddb /bin/bash
bin/herddb-cli.sh -x jdbc:herddb:server:0.0.0.0:7000 -q "select * from sysnodes"

# in order to access the cli from outside the container

bin/herddb-cli.sh -x jdbc:herddb:server:$(hostname):7000 -q "select * from sysnodes"


Running multiple HerdDB using docker

# start ZooKeeper
docker run -p 2181:2181 --name zookeeper --restart always -d zookeeper

# start as many 
# HerdDB inside the container will by default bind to port 7000, you have to pass server.port=0 in order to let the server choose a random port
docker run -d  --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest

Remember that the 'default'tablespace will be served by the first instance, so you should keep it running. If the leader of 'default' tablespace is lost you can recover by issuing ALTER TABLESPACE commands directly to other nodes and change the leader

# unzip a herddb-service package and hust run
bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "select * from sysnodes"

# scale up the 'default' tablespace
bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "ALTER TABLESPACE 'default','expectedreplicacount:2'"
