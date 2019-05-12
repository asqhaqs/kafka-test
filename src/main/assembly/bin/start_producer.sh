#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`

LIB_DIR=$DEPLOY_DIR/lib

CLS_DIR=$DEPLOY_DIR/classes

java -classpath $CLS_DIR:$LIB_DIR/* cn.situation.KafkaProducerTest 1
