# How to build  this image


```
git clone git@github.com:diennea/herddb.git
cd herddb-docker
docker build .
```



# How to use this image

## Running single instance HerdDB

(foreground mode)
```
docker run --rm -it  -e server.port=7000 -p 7000:7000 -e server.advertised.host=$(hostname)  --name herddb herd:latest
```
(daemon mode)
```
docker run --restart=always -d -e server.port=7000 -p 7000:7000 -e server.advertised.host=$(hostname)  --name herddb herd:latest
```

## in order to access local cli (inside the container)
```
docker exec -it herddb /bin/bash bin/herddb-cli.sh -x jdbc:herddb:server:0.0.0.0:7000 -q "select * from sysnodes"
```

## in order to access the cli from outside the container using herddb-cli.sh from a HerdDB downloaded package
```
bin/herddb-cli.sh -x jdbc:herddb:server:$(hostname):7000 -q "select * from sysnodes"
```

## use the cli with a temporary container
```
docker run --network host -it --entrypoint /bin/bash herd /herddb/bin/herddb-cli.sh -x jdbc:herddb:server:$(hostname):7000 -q "select * from sysnodes"
```

Running multiple HerdDB using docker

## start ZooKeeper
```
docker run -p 2181:2181 --name zookeeper --restart always -d zookeeper
```

## start as many
*HerdDB inside the container will by default bind to port 7000, you have to pass server.port=0 in order to let the server choose a random port*
```
docker run -d  --restart always --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --restart always --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --restart always --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --restart always --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --restart always --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
docker run -d  --restart always --network host -e server.mode=cluster -e server.bookkeeper.start=true -e server.port=0 -e server.zookeeper.address=$(hostname):2181 herd:latest
```

Remember that the 'default' tablespace will be served by the first instance, so you should keep it running. If the leader of 'default' tablespace is lost you can recover by issuing ALTER TABLESPACE commands directly to other nodes and change the leader

## in order to see your hosts
```
docker run --network host -it --entrypoint /bin/bash herd /herddb/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "select * from sysnodes"
```

## scale up the 'default' tablespace
```
docker run --network host -it --entrypoint /bin/bash herd /herddb/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "ALTER TABLESPACE 'default','expectedreplicacount:2','maxleaderinactivitytime:10000"
docker run --network host -it --entrypoint /bin/bash herd /herddb/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "select tablespace_name, leader, replica from systablespaces"
docker run --network host -it --entrypoint /bin/bash herd /herddb/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:$(hostname):2181 -q "select * from systablespacereplicastate"
```
