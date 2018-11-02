#/bin/bash


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
LOG="work_files_herd/"
FILE_TEMP="target/$LOG"
FINAL_REPORT="target/report_files_herd/"
HERDDB="HERDDB_"
DATE=$(date '+%Y-%m-%d-%H:%M:%S')
FORMAT=".txt"
if [[ -e $REPORT ]]; then 
rm -rf $REPORT
fi 
mkdir $FINAL
rm -rf $FILE_TEMP
rm -rf $FINAL_REPORT
mkdir $FILE_TEMP
mkdir $FINAL_REPORT
argv=("$@");
NARG=$#
while [ $L -lt $VAR ]; do 
	while [ $I -lt $NARG ]; do
 		WORK=${argv[$I]}
		echo $WORK
		rm -rf $REPORT
		./full-ycsb.sh  $YCSB_PATH  $HERDDB_PATH  $WORK > $FILE_TEMP$WORK.txt
		./parse_report.sh $WORK.txt $WORK$I.txt $WORK $VAR $FILE_TEMP $FINAL_REPORT $HERDDB_PATH
		let I=I+1
	done
I=3
let L=L+1
done
cat  $FINAL_REPORT* >$FINAL$HERDDB$REPORT$DATE$FORMAT

  



