#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`

LIB_DIR=$DEPLOY_DIR/lib

CLS_DIR=$DEPLOY_DIR/classes

nohup java -server -Xmx5g -Xms1g -Xmn256m -XX:PermSize=128m -Xss256k -XX:+UseNUMA -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:LargePageSizeInBytes=128m -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 -classpath $CLS_DIR:$LIB_DIR/* cn.situation.KafkaProducerTest 5 > /dev/null 2>&1 &
