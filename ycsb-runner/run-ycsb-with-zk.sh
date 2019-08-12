#/usr/bin/env bash

HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
HERDDB_PATH=$(realpath $2)
WORKLOAD=$3
JDBC_DRIVER=$(ls $HERDDB_PATH/*jdbc*.jar)
LOAD_PHASE="load"
RUN_PHASE="run"

echo "Running YCSB workload $WORKLOAD from $YCSB_PATH"
echo "Using HerdDB instance at $HERDDB_PATH"
echo "Using JDBC Driver $JDBC_DRIVER"

$HERDDB_PATH/bin/service zookeeper kill
$HERDDB_PATH/bin/service server kill

rm -Rf $HERDDB_PATH/dbdata

$HERDDB_PATH/bin/service zookeeper start

sleep 10

$HERDDB_PATH/bin/service server start

sleep 5

$HERDDB_PATH/bin/herddb-cli.sh -q "DROP TABLE usertable"
$HERDDB_PATH/bin/herddb-cli.sh -q "CREATE TABLE usertable ( YCSB_KEY VARCHAR(191) NOT NULL, FIELD0 STRING, FIELD1 STRING, FIELD2 STRING, FIELD3 STRING, FIELD4 STRING, FIELD5 STRING, FIELD6 STRING, FIELD7 STRING, FIELD8 STRING, FIELD9 STRING, PRIMARY KEY (YCSB_KEY));"

$YCSB_PATH/bin/ycsb load jdbc -P $YCSB_PATH/workloads/$WORKLOAD -P $HERE/cluster.properties -cp $HERDDB_PATH/herddb-jdbc* -threads 200 -s
./metrics.sh $WORKLOAD $LOAD_PHASE
echo "END_LOAD"
$YCSB_PATH/bin/ycsb run jdbc -P $YCSB_PATH/workloads/$WORKLOAD -P $HERE/cluster.properties -cp $HERDDB_PATH/herddb-jdbc* -threads 200 -s
./metrics.sh $WORKLOAD $RUN_PHASE
echo "END_FILE"
$HERDDB_PATH/bin/service server kill
$HERDDB_PATH/bin/service zookeeper kill
