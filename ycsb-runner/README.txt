#### HERDDB BENCHMARK ####

#edit herdb.properties if you need to change the configuration for the connection
#herd.properties MUST BE IN THE EXECUTION FOLDER
#example of herd.properties/*
        db.driver=herddb.jdbc.Driver
        db.url=jdbc:herddb:server:127.0.0.1:7000
        db.user="User name"
        db.passwd= "yourUserPass"
        */
#execute this command
#JAVA_HOME=/your/java/path ./ycsb-report.sh  YCSB_PATH    HERDDB_PATH  NUMBER_OF_ATTEMPTS   LIST_OF_WORKLOAD
#the result is saved in HERDDB_Benchmark.txt





#### MYSQL BENCHMARK ####

prerequisites: 
 *MYSQL must be running
 *DB USER have all privileges 

#create mysql.properties
#example of mysql.properties/*
	db.driver=com.mysql.jdbc.Driver
	db.url=jdbc:mysql://127.0.0.1:3307/usertabledata
	db.user="Db User name"
	db.passwd="Your user Passwd"
	*/
#execute this command
#JAVA_HOME=/your/java/path ./ycsb-report_mysql.sh YCSB_PATH PROPERTIES_PATH MYSQL_PATH CONNECTOR NUMER_OF_ATTEMPTS LIST_OF_WORLOAD 
#the result is saved in MYSQL_Benchmark.txt 



#### DETAILS OUTPUT FILE #### 

1 ---> java version
2 ---> database folder 
3 ---> workload name 
4 ---> operating system used 
5 ---> memory Ram capacity 
6 ---> numer of processor 
7 ---> date 
8 ---> average throughput 
9 ---> average load phase 
10 --> Number of attempts




