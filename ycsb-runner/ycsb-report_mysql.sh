#/bin/bash
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
LOG="work_files/"
FILE_TEMP="target/$LOG"
FINAL="target/"
FINAL_REPORT="target/report_files/"
JDBC_PATH=$4
MYSQL="MYSQL_"
DATE=$(date '+%Y-%m-%d-%H:%M:%S')
FORMAT=".txt"


if [[ -e $MYSQL$REPORT ]]; then 
rm -rf $MYSQL$REPORT
fi 
rm -rf $FILE_TEMP
rm -rf $FINAL_REPORT
mkdir $FINAL
mkdir $FILE_TEMP
mkdir $FINAL_REPORT
argv=("$@");
NARG=$#
while [ $L -lt $VAR ]; do 
	while [ $I -lt $NARG ]; do
 		WORK=${argv[$I]}
		echo $WORK
		rm -rf $REPORT
		./full-ycsb_mysql.sh  $YCSB_PATH $MYSQL_PROPERTIES  $MYSQL_PATH $JDBC_PATH $WORK > $FILE_TEMP$WORK.txt
		./parse_report.sh $WORK.txt $WORK$I.txt $WORK $VAR $FILE_TEMP $FINAL_REPORT $MYSQL_PATH
		let I=I+1
	done
I=5
let L=L+1
done


cat  $FINAL_REPORT* > $FINAL$MYSQL$REPORT$DATE$FORMAT


  



