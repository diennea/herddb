#/bin/bash
### BASE_DIR discovery
if [ -z "$BASE_DIR" ]; then
  BASE_DIR="`dirname \"$0\"`"
  BASE_DIR="`( cd \"$BASE_DIR\" && pwd )`"
fi
BASE_DIR=$BASE_DIR/..
BASE_DIR="`( cd \"$BASE_DIR\" && pwd )`"

. $BASE_DIR/bin/setenv.sh

JAVA="$JAVA_HOME/bin/java"
$JAVA -jar $BASE_DIR/herddb-cli.jar "$@"