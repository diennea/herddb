#/bin/bash
# Licensed to Diennea S.r.l. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Diennea S.r.l. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
set -x xtrace

ZKDIR=$(realpath target/zookeeper)
SERVER1DIR=$(realpath target/server1)
SERVER2DIR=$(realpath target/server2)
SERVER3DIR=$(realpath target/server3)
ZIP=$(ls target/herddb-service*zip)

echo "Installing $ZIP"
rm -Rf $ZKDIR
rm -Rf $SERVER1DIR
rm -Rf $SERVER2DIR
rm -Rf $SERVER3DIR
mkdir $ZKDIR
mkdir $SERVER1DIR
mkdir $SERVER2DIR
mkdir $SERVER3DIR

echo "Unzipping ZK in $ZKDIR"
unzip -q $ZIP -d $ZKDIR
unzip -q $ZIP -d $SERVER1DIR
unzip -q $ZIP -d $SERVER2DIR
unzip -q $ZIP -d $SERVER3DIR


cd $ZKDIR/herddb*
bin/service zookeeper start
cd ../..

sleep 1

cd $SERVER1DIR/herddb*
CONFIGFILE=conf/server.properties
sed -i 's/server.mode=standalone/server.mode=cluster/g' $CONFIGFILE
sed -i 's/server.port=7000/server.port=0/g' $CONFIGFILE
sed -i  's/#http.enable=true/http.enable=false/g' $CONFIGFILE
sed -i  's/server.bookkeeper.ensemble.size=1/server.bookkeeper.ensemble.size=2/g' $CONFIGFILE
sed -i  's/server.bookkeeper.write.quorum.size=1/server.bookkeeper.write.quorum.size=2/g' $CONFIGFILE
sed -i  's/server.bookkeeper.ack.quorum.size=1/server.bookkeeper.ack.quorum.size=2/g' $CONFIGFILE
sed -i  's/server.halt.on.tablespace.boot.error=true/server.halt.on.tablespace.boot.error=false/g' $CONFIGFILE
cd $SERVER1DIR/herddb*
bin/service server start
cd ../..

sleep 1
 
cd $SERVER2DIR/herddb*
CONFIGFILE=conf/server.properties
sed -i 's/server.mode=standalone/server.mode=cluster/g' $CONFIGFILE
sed -i 's/server.port=7000/server.port=0/g' $CONFIGFILE
sed -i  's/#http.enable=true/http.enable=false/g' $CONFIGFILE
sed -i  's/server.bookkeeper.ensemble.size=1/server.bookkeeper.ensemble.size=2/g' $CONFIGFILE
sed -i  's/server.bookkeeper.write.quorum.size=1/server.bookkeeper.write.quorum.size=2/g' $CONFIGFILE
sed -i  's/server.bookkeeper.ack.quorum.size=1/server.bookkeeper.ack.quorum.size=2/g' $CONFIGFILE
sed -i  's/server.halt.on.tablespace.boot.error=true/server.halt.on.tablespace.boot.error=false/g' $CONFIGFILE
cd $SERVER2DIR/herddb*
bin/service server start
cd ../..

sleep 1

cd $SERVER3DIR/herddb*
CONFIGFILE=conf/server.properties
sed -i 's/server.mode=standalone/server.mode=cluster/g' $CONFIGFILE
sed -i 's/server.port=7000/server.port=0/g' $CONFIGFILE
sed -i  's/#http.enable=true/http.enable=false/g' $CONFIGFILE
sed -i  's/server.bookkeeper.ensemble.size=1/server.bookkeeper.ensemble.size=2/g' $CONFIGFILE
sed -i  's/server.bookkeeper.write.quorum.size=1/server.bookkeeper.write.quorum.size=2/g' $CONFIGFILE
sed -i  's/server.bookkeeper.ack.quorum.size=1/server.bookkeeper.ack.quorum.size=2/g' $CONFIGFILE
sed -i  's/server.halt.on.tablespace.boot.error=true/server.halt.on.tablespace.boot.error=false/g' $CONFIGFILE
cd $SERVER3DIR/herddb*
bin/service server start
cd ../..

sleep 1

# test query
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:localhost:2181/herd -q 'select * from sysnodes'
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:localhost:2181/herd -q 'select * from systablespaces'
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:localhost:2181/herd -q "alter tablespace 'herd','expectedreplicacount:2'"
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:localhost:2181/herd -q "alter tablespace 'herd','maxleaderinactivitytime:15000'"
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:localhost:2181/herd -q 'select * from systablespaces'
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:zookeeper:localhost:2181/herd -q 'select * from systablespacereplicastate'




