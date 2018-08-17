#!/bin/bash

MASTER=ubuntu
MOUNT=/tmp/hdfsmnt
HDFS_PATH=`pwd`

function start {
	PID=`ps ax | grep HDFS.py | grep -v grep | awk '{print $1}'`
	if [ $PID ]; then
		echo "Datanode was already running!"
	else
		echo -n "Starting Datanode... "
		echo "Start "`date` >> /tmp/hdfs-datanode.log
		python $HDFS_PATH/HDFS.py $MASTER $MOUNT >> /tmp/hdfs-datanode.log &
		echo "done!"
	fi
}

function stop {
	echo -n "Stopping Datanode..."
 	/usr/bin/fusermount -uz $MOUNT 2> /dev/null
	fusermount -uz $MOUNT 2> /dev/null
        ps ax | grep HDFS.py | grep -v grep | awk '{print $1}' | xargs kill -9 2> /dev/null
	echo "done!"
}

case "$1" in
start)
	start
        ;;
stop)
	stop
        ;;
restart)
	stop
	start
        ;;
status)
	PID=`ps ax | grep HDFS.py | grep -v grep | awk '{print $1}'`
	if [ $PID ]; then
		echo "Datanode is running!"
	else
		echo "Datanode not running..."
	fi
	;;
*)
        echo "Usage: namenode {start|stop|restart}"
        exit 1
        ;;
esac
