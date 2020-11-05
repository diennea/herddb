#bin/bash
set -x -e
mvn -v
# build and validate
mvn checkstyle:check install spotbugs:check apache-rat:check -Pjenkins -DskipTests
# run tests
#mvn verify -Pjenkins -Dmaven.test.redirectTestOutputToFile=false -DforkCount=1 -Dherddb.file.requirefsync=false jacoco:report coveralls:report -Dtest=UseVirtualTableSpaceIdWithZookKeeperTest
mvn verify -Pjenkins -Dmaven.test.redirectTestOutputToFile=false -DforkCount=1 -Dherddb.file.requirefsync=false jacoco:report -Dtest=UseVirtualTableSpaceIdWithZookKeeperTest
