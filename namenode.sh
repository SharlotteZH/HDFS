#!/bin/bash

HDFS_PATH=`pwd`

function start {
	PID=`ps ax | grep NamenodeServer.py | grep -v grep | awk '{print $1}'`
	if [ $PID ]; then
		echo "Namenode was already running..."
	else
		echo -n "Starting Namenode..."
		echo "Start "`date` >> /tmp/hdfs-namenode.log
		python $HDFS_PATH/NamenodeServer.py >> /tmp/hdfs-namenode.log &
		echo "done!"
	fi
}

function stop {
	echo -n "Stopping Namenode..."
	ps ax | grep NamenodeServer.py | grep -v grep | awk '{print $1}' | xargs kill -9 2> /dev/null
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
	PID=`ps ax | grep NamenodeServer.py | grep -v grep | awk '{print $1}'`
	if [ $PID ]; then
		echo "Namenode is running!"
	else
		echo "Namenode not running..."
	fi
	;;
*)
	echo "Usage: namenode {start|stop|restart}"
	exit 1
	;;
esac
