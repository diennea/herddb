#/bin/bash

echo -n "INSERT NUMBER OF ATTEMPTS[ENTER]: "
read var


HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
HERDDB_PATH=$(realpath $2)
WORKLOAD=$@
JDBC_DRIVER=$(ls $HERDDB_PATH/*jdbc*.jar)
I=2
NARG=0 
WORK=
REPORT="FULL_TOTAL_REPORT.txt"
L=0


if [[ ! -d "$HERDDB_PATH" ]]; then
   echo "Directory $HERDDB_PATH is not a valid directory"
   exit 1
fi
if [[ ! -d "$YCSB_PATH" ]]; then
   echo "Directory $YCSB_PATH is not a valid directory"
   exit 1
fi

if [[ -e $REPORT ]]; then 
rm -rf $REPORT
fi 
rm -rf file_temp
mkdir file_temp
argv=("$@");
NARG=$#
while [ $L -le $var ]; do 
	while [ $I -lt $NARG ]; do
 		WORK=${argv[$I]}
		rm -rf $REPORT
		./full-ycsb.sh  $YCSB_PATH  $HERDDB_PATH $WORK > file_temp/$WORK.txt
		./parse_report.sh $WORK.txt $WORK$I.txt $WORK $var
		let I=I+1
	done
I=2
let L=L+1
done


rm -rf file_temp

  



