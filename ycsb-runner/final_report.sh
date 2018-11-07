#/bin/bash

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
DATABASE_PATH=$9


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
        JAVA_OPTS=$(cat $DATABASE_PATH/bin/setenv.sh | grep Xm)	
	echo "Conf= $JAVA_OPTS"
fi

$FILE_TEMP$WORKLOAD$NAME $FINAL_REPORT
 
#cat $FILE_TEMP$WORKLOAD$NAME > $REPORT
 
