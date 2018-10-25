#/bin/bash

WORKLOAD=$1
NAME="_final_report.txt"
SYSTEM=$(uname -s -r)
RAM=$(dmidecode -t 16 | grep "Maximum Capacity" | cut -d ':'  -f2)
DATE=$(date)
PROCESSOR=
THROUGHPUT=$2
ATTEMPTS=$3
REPORT="FULL_TOTAL_REPORT.txt"


cat /proc/cpuinfo | grep "model name" > processor.txt
PROCESSOR=$(tail -1 processor.txt | cut -d ':' -f2)
echo $PROCESSOR

if [[ ! -e $WORKLOAD$NAME ]]; then
        touch $WORKLOAD$NAME
else
	rm -rf  $WORKLOAD$NAME
	touch $WORKLOAD$NAME
fi

echo -e " " >> $WORKLOAD$NAME
echo -e "------------------------------------------------------------------- " >> $WORKLOAD$NAME
echo "WORKLOAD = $WORKLOAD" >> $WORKLOAD$NAME
echo "SYSTEM= $SYSTEM" >> $WORKLOAD$NAME
echo "RAM= $RAM" >> $WORKLOAD$NAME
echo "PROCESSOR = $PROCESSOR" >> $WORKLOAD$NAME
echo "JAVA_VERSION= $JAVA_HOME" >> $WORKLOAD$NAME
echo "DATE= $DATE" >> $WORKLOAD$NAME
echo "THROUGHPUT = $THROUGHPUT" >>$WORKLOAD$NAME
echo "ATTEMPTS = $ATTEMPTS"  >> $WORKLOAD$NAME


cat $WORKLOAD$NAME >> $REPORT 
rm -rf processor.txt
rm -rf $WORKLOAD$NAME
 
