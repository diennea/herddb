Running multiple HerdDB using docker

# start ZooKeeper
docker run -p 2181:2181 --name some-zookeeper --restart always -d zookeeper

docker run --rm -it  -e server.port=8000 -p 8000:8000 -e server.mode=cluster  -e server.bookkeeper.start=true -e server.advertised.host=$(hostname)  -e server.zookeeper.address=$(hostname):2181 herd:latest

docker run --rm -it  -e server.port=9000 -p 9000:9000 -e server.mode=cluster  -e server.bookkeeper.start=true -e server.advertised.host=$(hostname)  -e server.zookeeper.address=$(hostname):2181 herd:latest

