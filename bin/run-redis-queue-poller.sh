#!/bin/bash

function usage() {
  echo "Usage: $0 <jar>"
}

# Print usage if incorrect number of args
[[ $# -ne 1 ]] && usage

MAIN_JAR=$1
SERVER_PORT=$2
SERVER_CLASS_NAME="com.mozilla.bagheera.redis.RedisListPoller"
HADOOP_CONF_PATH=/Users/xstevens/CDH3/hadoop-0.20.2-cdh3u0
HBASE_CONF_PATH=/Users/xstevens/CDH3/hbase-0.90.1-cdh3u0

CLASSPATH=$MAIN_JAR:$HADOOP_CONF_PATH:$HBASE_CONF_PATH

for lib in `ls lib/*.jar`;
do
    CLASSPATH=$CLASSPATH:$lib
done

echo $CLASSPATH

java -cp $CLASSPATH $SERVER_CLASS_NAME
