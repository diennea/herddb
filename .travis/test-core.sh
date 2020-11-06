#bin/bash
set -x -e
mvn -v
# run tests in herddb-core
mvn install -Pjenkins -Dmaven.test.redirectTestOutputToFile=true -DforkCount=2 -Dherddb.file.requirefsync=false -f herddb-core/pom.xml
