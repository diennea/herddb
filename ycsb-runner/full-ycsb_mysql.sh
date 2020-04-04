#/usr/bin/env bash
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

HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
MYSQL_PROPERTIES=$2
MYSQL_PATH=$3
JDBC_PATH=$4
WORKLOAD=$5
JDBC_DRIVER=$(ls $JDBC_PATH/*connector*.jar)
dbuser=$(grep "db.user" $MYSQL_PROPERTIES | cut -d "=" -f2)
dbpasswd=$(grep "db.passwd" $MYSQL_PROPERTIES | cut -d "=" -f2)
echo "Running YCSB workload $WORKLOAD from $YCSB_PATH"
echo "Using JDBC Driver $JDBC_DRIVER"

if [[ ! -d "$YCSB_PATH" ]]; then
   echo "Directory $YCSB_PATH is not a valid directory"
   exit 1
fi


database="UsertableData"
createdatabase="CREATE DATABASE $database;"
dropdatabase="DROP DATABASE IF EXISTS  $database;"
#droptable= "DROP TABLE usertable;"
usedatabase="use $database;"
#create table; use utf8 because some version of mysql have max length=767 bytes
createtable="CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 TEXT, FIELD1 TEXT,FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT,FIELD8 TEXT, FIELD9 TEXT) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;"

$MYSQL_PATH/bin/mysql --socket $MYSQL_PATH/data/mysqld.sock -u $dbuser -p$dbpasswd << EOF
$dropdatabase
$createdatabase
EOF

$MYSQL_PATH/bin/mysql --socket $MYSQL_PATH/data/mysqld.sock -u $dbuser -p$dbpasswd  << EOF
$usedatabase
$createtable
EOF

$YCSB_PATH/bin/ycsb load jdbc -P $YCSB_PATH/workloads/$WORKLOAD -P $MYSQL_PROPERTIES  -cp $JDBC_PATH/mysql-connector*  -threads 200 -s
echo "END_LOAD"
$YCSB_PATH/bin/ycsb run jdbc -P $YCSB_PATH/workloads/$WORKLOAD -P $MYSQL_PROPERTIES -cp $JDBC_PATH/mysql-connector*  -threads 200 -s
echo "END_FILE"
