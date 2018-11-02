#/bin/bash

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
FORMAT=".txt" 
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

  



