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
# HerdDB inside the container will by default bind to port 7000, it cannot known the real coordinates outside the container
# so you have to override server.advertised.host and server.advertised.port configuration parameters
docker run -d  -p 8000:7000 -e server.mode=cluster -e server.bookkeeper.start=true -e server.advertised.host=$(hostname) -e server.advertised.port=8000  -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  -p 9000:7000 -e server.mode=cluster -e server.bookkeeper.start=true -e server.advertised.host=$(hostname) -e server.advertised.port=9000  -e server.zookeeper.address=$(hostname):2181 herd:latest


# unzip a herddb-service package and hust run
bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "select * from sysnodes"

# scale up the 'default' tablespace
bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "ALTER TABLESPACE 'default','expectedreplicacount:2'"
