#/bin/bash

HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
HERDDB_PATH=$(realpath $2)
WORKLOAD=$3
JDBC_DRIVER=$(ls $HERDDB_PATH/*jdbc*.jar)
JDBC_URL=$4
LOAD_PHASE="load"
RUN_PHASE="run"

echo "Running YCSB workload $WORKLOAD from $YCSB_PATH"
echo "Using HerdDB instance at $HERDDB_PATH"
echo "Using JDBC Driver $JDBC_DRIVER"
echo "Using JDBC URL $JDBC_URL"

if [[ ! -d "$HERDDB_PATH" ]]; then
   echo "Directory $HERDDB_PATH is not a valid directory"
   exit 1
fi
if [[ ! -d "$YCSB_PATH" ]]; then
   echo "Directory $YCSB_PATH is not a valid directory"
   exit 1
fi




$HERDDB_PATH/bin/herddb-cli.sh -q "DROP TABLE  usertable" -x $JDBC_URL
$HERDDB_PATH/bin/herddb-cli.sh -q "CREATE TABLE usertable ( YCSB_KEY VARCHAR(191) NOT NULL, FIELD0 STRING, FIELD1 STRING, FIELD2 STRING, FIELD3 STRING, FIELD4 STRING, FIELD5 STRING, FIELD6 STRING, FIELD7 STRING, FIELD8 STRING, FIELD9 STRING, PRIMARY KEY (YCSB_KEY));" -x $JDBC_URL

CONFFILE=$HERE/run.properties
echo "db.driver=herddb.jdbc.Driver" > $CONFFILE
echo "db.user=sa" >> $CONFFILE
echo "db.driver=herddb.jdbc.Driver" >> $CONFFILE
echo "db.url=$JDBC_URL" >> $CONFFILE
echo "db.passwd=hdb" >> $CONFFILE

$YCSB_PATH/bin/ycsb load jdbc -P $YCSB_PATH/workloads/$WORKLOAD -P $CONFFILE -cp $HERDDB_PATH/herddb-jdbc* -threads 200 -s
$YCSB_PATH/bin/ycsb run jdbc -P $YCSB_PATH/workloads/$WORKLOAD -P $CONFFILE -cp $HERDDB_PATH/herddb-jdbc* -threads 200 -s
