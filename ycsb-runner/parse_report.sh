#/bin/bash
FILE_NAME=$1
SAVE_FILE_NAME=$2
NAME="Throughput"
TEMP="temp.txt"
NUMERO_TENTATIVI=0
TEMP2="temp2.txt"
TEMP3="temp3.txt"
SUM=0
MEDIA=0
WORKLOAD=$3
JAVA="java.txt"
REPORT="FULL_TOTAL_REPORT.txt"
grep -B50 END_FILE $FILE_NAME > $SAVE_FILE_NAME
JAVA_VERSION=`$JAVA_HOME/bin/java -version`

if [[ ! -e $NAME$FILE_NAME ]]; then
        grep "Throughput(ops/sec)" $SAVE_FILE_NAME > $TEMP2
        cut -f 3 -d, $TEMP2   >$TEMP3
        cat $TEMP3 | awk '{$1=$1;print}' | awk -F"." '{print $1}' > $NAME$FILE_NAME  #prende il risultato, ci toglie lo spazio iniziale e prende solamente la parte prima del punto
else
         grep "Throughput(ops/sec)" $SAVE_FILE_NAME > $TEMP
        mv $NAME$FILE_NAME $NAME.txt
        cat $TEMP $NAME.txt > $TEMP2
        cut -f 3 -d, $TEMP2  >$TEMP3
        cat $TEMP3 | awk '{$1=$1;print}' | awk -F"." '{print $1}' > $NAME$FILE_NAME     #prende il risultato, ci toglie lo spazio iniziale e prende solamente la parte prima del punto
fi
rm -rf $SAVE_FILE_NAME
rm -rf $TEMP
rm -rf $NAME.txt
rm -rf $TEMP2
rm -rf $TEMP3

NUMERO_TENTATIVI=$(cat $NAME$FILE_NAME | wc -l)
while read -r line   #scorro tutte le righe del file
do
 let SUM=$SUM+$line  #calcolo della somma di tutte le righe presenti nel file
done < $NAME$FILE_NAME
#echo $SUM
MEDIA=$(($SUM/$NUMERO_TENTATIVI)) #calcolo della media
#echo $MEDIA

#if [[ ! -e $REPORT ]]; then
#touch $REPORT
#fi


./final_report.sh $WORKLOAD $MEDIA $NUMERO_TENTATIVI $REPORT

