#/bin/bash

WORKLOAD=$1
NAME="_final_report.txt"
JAVA_VERSION="java 10"
SYSTEM=$(uname -s -r)
DATE=$(date)
THROUGHPUT=$2
ATTEMPTS=$3
REPORT="FULL_TOTAL_REPORT.txt"

if [[ ! -e $WORKLOAD$NAME ]]; then
        touch $WORKLOAD$NAME
else
	rm -rf  $WORKLOAD$NAME
	touch  $WORKLOAD$NAME
fi

echo -e " " >> $WORKLOAD$NAME
echo -e "------------------------------------------------------------------- " >> $WORKLOAD$NAME
echo "WORKLOAD = $WORKLOAD" >>  $WORKLOAD$NAME
echo "SYSTEM= $SYSTEM" >> $WORKLOAD$NAME
echo "JAVA_VERSION= $JAVA_VERSION" >> $WORKLOAD$NAME
echo "DATE= $DATE" >> $WORKLOAD$NAME
echo "THROUGHPUT = $THROUGHPUT" >> $WORKLOAD$NAME
echo "ATTEMPTS = $ATTEMPTS"  >> $WORKLOAD$NAME


cat $WORKLOAD$NAME >> $REPORT 


