#!/bin/bash

function usage() {
  echo "Usage: $0 <jar> <server-port>"
}

# Print usage if incorrect number of args
[[ $# -ne 2 ]] && usage

MAIN_JAR=$1
SERVER_PORT=$2
SERVER_CLASS_NAME="com.mozilla.bagheera.rest.Bagheera"
HADOOP_CONF_PATH=/etc/hadoop/conf
HBASE_CONF_PATH=/etc/hbase/conf
ZK_CONF_PATH=/etc/hbase/conf
CLASSPATH=$MAIN_JAR:$HADOOP_CONF_PATH:$HBASE_CONF_PATH:$ZK_CONF_PATH

for lib in `ls lib/*.jar`;
do
    CLASSPATH=$CLASSPATH:$lib
done

echo $CLASSPATH

java -Dserver.port=$SERVER_PORT -cp $CLASSPATH $SERVER_CLASS_NAME
