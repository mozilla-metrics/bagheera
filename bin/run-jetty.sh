#!/bin/bash

function usage() {
  echo "Usage: $0 <jar> <server-port> <hazelcast-config-path>"
  exit 1
}

# Print usage if incorrect number of args
[[ $# -ne 3 ]] && usage

MAIN_JAR=$1
SERVER_PORT=$2
HAZELCAST_CONFIG=$3
SERVER_CLASS_NAME="com.mozilla.bagheera.rest.Bagheera"
HADOOP_CONF_PATH=/etc/hadoop/conf
HBASE_CONF_PATH=/etc/hbase/conf

CLASSPATH=$MAIN_JAR:$HADOOP_CONF_PATH:$HBASE_CONF_PATH
JAVA_OPTS="-Xmx1000m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
for lib in `ls lib/*.jar`;
do
    CLASSPATH=$CLASSPATH:$lib
done

for lib in `ls lib/elasticsearch/*.jar`;
do
    CLASSPATH=$CLASSPATH:$lib
done
for lib in `ls lib/elasticsearch/sigar/*.jar`;
do
    CLASSPATH=$CLASSPATH:$lib
done

echo $CLASSPATH

java -Dhazelcast.config=$HAZELCAST_CONFIG -Dserver.port=$SERVER_PORT $JAVA_OPTS -cp $CLASSPATH $SERVER_CLASS_NAME
