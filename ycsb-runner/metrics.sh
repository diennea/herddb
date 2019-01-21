#/bin/bash
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
target="target/"


if [ ! -d $target ]; #check if folder target/ does not exits
then  
	mkdir $target #if not exist create a target folder
fi 


if [ ! -d $folder ];  #check if folder target/metric does not exists 
then
	mkdir $folder #if not exist create a target/metrics folder  
fi



wget $url  -P $folder  #dowload metrics from url and save it in target/metrics

if [[ -e $name$extension ]]; then
	i=1
while [[ -e $name.$i$extension ]]; do
	let i++
done
name=$name.$i
fi 

mv $folder$filename $name$extension #change file name 
