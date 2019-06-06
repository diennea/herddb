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

WORKLOAD=$1
NAME="_final_report.txt"
SYSTEM=$(uname -s -r)
RAM=$(dmidecode -t 16 | grep "Maximum Capacity" | cut -d ':'  -f2)
DATE=$(date)

PROCESSOR=$(grep -m 1 'cpu cores' /proc/cpuinfo| cut -d ":" -f2)
THROUGHPUT=$2
ATTEMPTS=$3
REPORT=$4
FILE_TEMP=$5
FINAL_REPORT=$6
DATABASE_PATH=$7
MEDIA_LOAD=$8


$JAVA_HOME/bin/java -version 2> $FILE_TEMPjavaversion.txt

if [[ ! -e  $FILE_TEMP$WORKLOAD$NAME ]]; then
        touch  $FILE_TEMP$WORKLOAD$NAME
else
	rm -rf  $FILE_TEMP$WORKLOAD$NAME
	touch $FILE_TEMP$WORKLOAD$NAME
fi

echo -e " " >> $FILE_TEMP$WORKLOAD$NAME
echo -e "------------------------------------------------------------------- " >> $FILE_TEMP$WORKLOAD$NAME
echo "$(grep "java version" $FILE_TEMPjavaversion.txt)" >> $FILE_TEMP$WORKLOAD$NAME

echo "Database Folder=$DATABASE_PATH"  >> $FILE_TEMP$WORKLOAD$NAME
echo "Workload=$WORKLOAD" >> $FILE_TEMP$WORKLOAD$NAME
echo "System=$SYSTEM" >> $FILE_TEMP$WORKLOAD$NAME
echo "Ram=$RAM" >> $FILE_TEMP$WORKLOAD$NAME
echo "Number of processor=$PROCESSOR" >> $FILE_TEMP$WORKLOAD$NAME
echo "Date=$DATE" >> $FILE_TEMP$WORKLOAD$NAME
echo "Throughput=$THROUGHPUT" >> $FILE_TEMP$WORKLOAD$NAME
echo "Load phase=$MEDIA_LOAD" >> $FILE_TEMP$WORKLOAD$NAME
echo "Attempts=$ATTEMPTS"  >> $FILE_TEMP$WORKLOAD$NAME
if [[ -e  $DATABASE_PATH/bin/setenv.sh  ]]; then 
        JAVA_OPTS=$(cat $DATABASE_PATH/bin/setenv.sh | grep JAVA_OPTS)	
	echo "Conf JAVA_OPTS= $JAVA_OPTS" >> $FILE_TEMP$WORKLOAD$NAME
fi

mv $FILE_TEMP$WORKLOAD$NAME $FINAL_REPORT
 
 
