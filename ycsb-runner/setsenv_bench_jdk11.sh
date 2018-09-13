# Basic Environment and Java variables

JAVA_HOME=~/dev/jdk-11
JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED"
JAVA_OPTS="-XX:+UseG1GC -Duser.language=en -Xmx4g -Xms4g -XX:+AggressiveOpts -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g  -Djava.util.logging.config.file=conf/logging.properties -Dherddb.file.requirefsync=false -XX:+FlightRecorder"

if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=`which java 2>/dev/null`
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_BIN=`dirname $JAVA_PATH 2>/dev/null`
    JAVA_HOME=`dirname $JAVA_BIN 2>/dev/null`
  fi
  if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME environment variable is not defined and is needed to run this program"
    exit 1
  fi
fi
