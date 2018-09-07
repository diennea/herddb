#!/bin/bash
cd ..
JAVA_HOME=/usr/java/current10 mvn clean install -DskipTests
cd herddb-services/target
unzip herddb-services*zip
cd ../../ycsb-runner
cp setsenv_bench_jdk10.sh ../herddb-services/target/herddb-service*-SNAPSHOT/bin/setenv.sh
