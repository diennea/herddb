#/usr/bin/env bash
DATE=$(date '+%Y-%m-%d-%H-%M-%S')
workload=$1
separator="_"
type=$2
metrics="metrics"
host=$(hostname) #hostname 
url="http://$host:9845/metrics"
folder="target/metrics/"
filename="metrics"
name="$folder$workload$separator$type$separator$metrics"
extension=".log"


mkdir -p $folder


wget $url  -P $folder  #dowload metrics from url and save it in target/metrics

if [[ -e $name$extension ]]; then
	i=1
while [[ -e $name.$i$extension ]]; do
	let i++
done
name=$name.$i
fi 

mv $folder$filename $name$extension #change file name 
