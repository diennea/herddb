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
FILE_NAME=$1
SAVE_FILE_NAME=$2
NAME="Throughput"
TEMP="temp.txt"
NUMERO_TENTATIVI=0
TEMP2="temp2.txt"
TEMP3="temp3.txt"
SUM=0
MEDIA=0
WORKLOAD=$3
JAVA="java.txt"
REPORT="FULL_TOTAL_REPORT.txt"
FILE_TEMP=$5
FINAL_REPORT=$6
DATABASE_PATH=$7
LOAD_FILE="loadfile"
LOAD=0
MEDIA_LOAD=0
grep -B50 END_FILE $FILE_TEMP$FILE_NAME > $FILE_TEMP$SAVE_FILE_NAME 
grep -B50 "END_LOAD" $FILE_TEMP$FILE_NAME | grep " Throughput(ops/sec)" | cut -f 3 -d , |  awk '{$1=$1;print}' | awk -F"." '{print $1}' >> $FILE_TEMP$LOAD_FILE$SAVE_FILE_NAME

if [[ ! -e $FILE_TEMP$NAME$FILE_NAME ]]; then
        grep "Throughput(ops/sec)" $FILE_TEMP$SAVE_FILE_NAME > $FILE_TEMP$TEMP2
        cut -f 3 -d, $FILE_TEMP$TEMP2   > $FILE_TEMP$TEMP3
        cat $FILE_TEMP$TEMP3 | awk '{$1=$1;print}' | awk -F"." '{print $1}' > $FILE_TEMP$NAME$FILE_NAME  
else
         grep "Throughput(ops/sec)" $FILE_TEMP$SAVE_FILE_NAME > $FILE_TEMP$TEMP
        mv $FILE_TEMP$NAME$FILE_NAME $FILE_TEMP$NAME.txt
        cat $FILE_TEMP$TEMP $FILE_TEMP$NAME.txt > $FILE_TEMP$TEMP2
        cut -f 3 -d, $FILE_TEMP$TEMP2  > $FILE_TEMP$TEMP3
        cat $FILE_TEMP$TEMP3 | awk '{$1=$1;print}' | awk -F"." '{print $1}' > $FILE_TEMP$NAME$FILE_NAME     
fi

rm -rf $FILE_TEMP$SAVE_FILE_NAME
rm -rf $FILE_TEMP$TEMP
rm -rf $FILE_TEMP$NAME.txt
rm -rf $FILE_TEMP$TEMP2
rm -rf $FILE_TEMP$TEMP3

NUMERO_TENTATIVI=$4
while read -r line   
do
 let SUM=$SUM+$line  
done < $FILE_TEMP$NAME$FILE_NAME
while read -r line
do 
let LOAD=$LOAD+$line
done < $FILE_TEMP$LOAD_FILE$SAVE_FILE_NAME 
MEDIA=$(($SUM/$NUMERO_TENTATIVI)) 
MEDIA_LOAD=$(($LOAD/$NUMERO_TENTATIVI))

./final_report.sh $WORKLOAD $MEDIA $NUMERO_TENTATIVI $REPORT $FILE_TEMP $FINAL_REPORT $DATABASE_PATH $MEDIA_LOAD 


