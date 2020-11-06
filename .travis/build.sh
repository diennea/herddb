#bin/bash
set -x -e
mvn -v
# build and validate
mvn checkstyle:check install spotbugs:check apache-rat:check -Pjenkins -DskipTests
