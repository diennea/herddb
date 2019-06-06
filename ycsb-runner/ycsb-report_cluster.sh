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
DATE=$(date '+%Y-%m-%d-%H-%M-%S')
VAR=$3
HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
HERDDB_PATH=$(realpath $2)
WORKLOAD=$@
JDBC_DRIVER=$(ls $HERDDB_PATH/*jdbc*.jar)
I=3
NARG=0 
WORK=
REPORT="Benchmark-"
L=0
FINAL="target/"
LOG="work_files_herd-$DATE/"
FILE_TEMP="target/$LOG"
FINAL_REPORT="target/report_files_herd-$DATE/"
HERDDB="HERDDB_"
FORMAT=".log" 


 mkdir -p  $FINAL #if not exist create a target folder


rm -rf $FILE_TEMP
rm -rf $FINAL_REPORT
mkdir $FILE_TEMP
mkdir $FINAL_REPORT
argv=("$@");
NARG=$#
while [ $L -lt $VAR ]; do 
	while [ $I -lt $NARG ]; do
 		WORK=${argv[$I]}
		./run-ycsb-with-zk.sh  $YCSB_PATH  $HERDDB_PATH  $WORK > $FILE_TEMP$WORK.txt
		./parse_report.sh $WORK.txt $WORK$I.txt $WORK $VAR $FILE_TEMP $FINAL_REPORT $HERDDB_PATH
		let I=I+1
	done
I=3
let L=L+1
done
cat  $FINAL_REPORT* >$FINAL$HERDDB$REPORT$DATE$FORMAT
rm -rf ".txt"
  



