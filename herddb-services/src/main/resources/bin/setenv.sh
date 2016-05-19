# Basic Environment and Java variables

#JAVA_HOME=
JAVA_OPTS="-Xmx1g -Xms1g -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g "


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
