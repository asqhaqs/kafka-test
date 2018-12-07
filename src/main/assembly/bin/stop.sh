#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`

# SERVER_NAME=collector-log

SERVER_NAME=`sed '/application.name/!d;s/.*=//' classes/app.properties | tr -d '\r'`

LOGS_DIR=$DEPLOY_DIR/logs

if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi

if [ -z "$SERVER_NAME" ]; then
    SERVER_NAME=`hostname`
fi

PID_FILE=$LOGS_DIR/${SERVER_NAME}.pid

# PIDS=`ps -ef | grep java | grep "$SERVER_NAME" |awk '{print $2}'`

if [ ! -f "$PID_FILE" ]; then
    PIDS=`cat $PID_FILE`
    echo "ERROR: The $SERVER_NAME does not started!"
    exit 1
fi

if [ "$1" != "skip" ]; then
    $BIN_DIR/dump.sh
fi

echo -e "Stopping the $SERVER_NAME ...\c"

PIDS=`ps -ef | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
for PID in $PIDS ; do
    kill $PID > /dev/null 2>&1
done

COUNT=0
while [ $COUNT -lt 1 ]; do    
    echo -e ".\c"
    sleep 1
    COUNT=1
    for PID in $PIDS ; do
        PID_EXIST=`ps -f -p $PID | grep java`
        if [ -n "$PID_EXIST" ]; then
            COUNT=0
            break
        fi
    done
done

echo "OK!"
echo "PID: $PIDS"
rm -rf $PID_FILE
