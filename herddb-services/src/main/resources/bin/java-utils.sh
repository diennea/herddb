#!/bin/bash

# Licensed to Diennea S.r.l. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Diennea S.r.l. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] SERVICETYPE [jvmargs]"
  exit 1
fi


if [ -z "$BASE_DIR" ];
then
  BASE_DIR="`dirname \"$0\"`"
  BASE_DIR="`( cd \"$BASE_DIR/..\" && pwd )`"
fi

cd $BASE_DIR
. $BASE_DIR/bin/setenv.sh


CLASSPATH=

for file in $BASE_DIR/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $BASE_DIR/extra/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done


if [ -z "$JAVA_OPTS" ]; then
  JAVA_OPTS=""
fi

JAVA="$JAVA_HOME/bin/java"

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

SERVICE=$1
shift

if [ "x$DAEMON_MODE" = "xtrue" ]; then
  CONSOLE_OUTPUT_FILE=$SERVICE.service.log  
  nohup $JAVA -cp $CLASSPATH $JAVA_OPTS "$@" >> "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
  RETVAL=$?
else    
  exec $JAVA -cp $CLASSPATH $JAVA_OPTS "$@"
  RETVAL=$?
fi
