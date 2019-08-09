#/usr/bin/env bash
cd ..
JAVA_HOME=~/dev/jdk-12 mvn clean install -DskipTests
cd herddb-services/target
unzip herddb-services*zip
cd ../../ycsb-runner
cp setsenv_bench_jdk11.sh ../herddb-services/target/herddb-service*-SNAPSHOT/bin/setenv.sh
