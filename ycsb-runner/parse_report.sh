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
grep -B50 END_FILE file_temp/$FILE_NAME > file_temp/$SAVE_FILE_NAME 

if [[ ! -e file_temp/$NAME$FILE_NAME ]]; then
        grep "Throughput(ops/sec)" file_temp/$SAVE_FILE_NAME > file_temp/$TEMP2
        cut -f 3 -d, file_temp/$TEMP2   > file_temp/$TEMP3
        cat file_temp/$TEMP3 | awk '{$1=$1;print}' | awk -F"." '{print $1}' > file_temp/$NAME$FILE_NAME  #prende il risultato, ci toglie lo spazio iniziale e prende solamente la parte prima del punto
else
         grep "Throughput(ops/sec)" file_temp/$SAVE_FILE_NAME > file_temp/$TEMP
        mv file_temp/$NAME$FILE_NAME file_temp/$NAME.txt
        cat file_temp/$TEMP file_temp/$NAME.txt > file_temp/$TEMP2
        cut -f 3 -d, file_temp/$TEMP2  > file_temp/$TEMP3
        cat file_temp/$TEMP3 | awk '{$1=$1;print}' | awk -F"." '{print $1}' > file_temp/$NAME$FILE_NAME     #prende il risultato, ci toglie lo spazio iniziale e prende solamente la parte prima del punto
fi
rm -rf file_temp/$SAVE_FILE_NAME
rm -rf file_temp/$TEMP
rm -rf file_temp/$NAME.txt
rm -rf file_temp/$TEMP2
rm -rf file_temp/$TEMP3

NUMERO_TENTATIVI=$(cat file_temp/$NAME$FILE_NAME | wc -l)
while read -r line   #scorro tutte le righe del file
do
 let SUM=$SUM+$line  #calcolo della somma di tutte le righe presenti nel file
done < file_temp/$NAME$FILE_NAME
#echo $SUM
MEDIA=$(($SUM/$NUMERO_TENTATIVI)) #calcolo della media
#echo $MEDIA

#if [[ ! -e $REPORT ]]; then
#touch $REPORT
#fi


./final_report.sh $WORKLOAD $MEDIA $NUMERO_TENTATIVI $REPORT 

