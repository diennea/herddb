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

DATE=$(date '+%Y-%m-%d-%H:%M:%S')
VAR=$5
HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
MYSQL_PATH=$(realpath $3)
MYSQL_PROPERTIES=$2
WORKLOAD=$@
I=5
NARG=0 
WORK=
REPORT="Benchmark-"
L=0
LOG="work_files_mysql-$DATE/"
FILE_TEMP="target/$LOG"
FINAL="target/"
FINAL_REPORT="target/report_files_mysql-$DATE/"
JDBC_PATH=$4
MYSQL="MYSQL_"
FORMAT=".log" 
mkdir $FINAL
mkdir $FILE_TEMP
mkdir $FINAL_REPORT
argv=("$@");
NARG=$#
while [ $L -lt $VAR ]; do 
	while [ $I -lt $NARG ]; do
 		WORK=${argv[$I]}
		./full-ycsb_mysql.sh  $YCSB_PATH $MYSQL_PROPERTIES  $MYSQL_PATH $JDBC_PATH $WORK > $FILE_TEMP$WORK.txt
		./parse_report.sh $WORK.txt $WORK$I.txt $WORK $VAR $FILE_TEMP $FINAL_REPORT $MYSQL_PATH
		let I=I+1
	done
I=5
let L=L+1
done
cat  $FINAL_REPORT* > $FINAL$MYSQL$REPORT$DATE$FORMAT
rm -rf ".txt"

  



