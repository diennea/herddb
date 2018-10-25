#/bin/bash

HERE=$(dirname $0)
HERE=$(realpath $HERE)
YCSB_PATH=$1
HERDDB_PATH=$(realpath $2)
WORKLOAD=$@
JDBC_DRIVER=$(ls $HERDDB_PATH/*jdbc*.jar)
I=2
NARG=0 
WORK=



if [[ ! -d "$HERDDB_PATH" ]]; then
   echo "Directory $HERDDB_PATH is not a valid directory"
   exit 1
fi
if [[ ! -d "$YCSB_PATH" ]]; then
   echo "Directory $YCSB_PATH is not a valid directory"
   exit 1
fi

argv=("$@");
NARG=$#
while [ $I -lt $NARG ]; do
	WORK=${argv[$I]}
	./full-ycsb.sh  $YCSB_PATH  $HERDDB_PATH $WORK > $WORK.txt
	./parse_report.sh $WORK.txt $WORK$I.txt $WORK
	let I=I+1
done



  



