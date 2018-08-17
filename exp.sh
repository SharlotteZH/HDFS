#!/bin/bash
MOUNT=/tmp/hdfsmnt

function stop {
	bash namenode.sh stop
	bash datanode.sh stop
}

function restart {
	bash namenode.sh restart 
	bash datanode.sh restart 
}

function cleanfs {
	rm -fr /tmp/hdfs*
	rm -fr $MOUNT
	rm -fr /tmp/namenode* 
	rm -fr /tmp/*.log
	rm -fr *.pyc
	mkdir -p $MOUNT
}

function run_simple {
	stop
	cleanfs
	echo "clean fs"
	sleep 1
	restart
	echo "restart"
	sleep 1
	ls $MOUNT
	echo "hello" > $MOUNT/file0
	cat $MOUNT/file0
	#dd if=/dev/zero of=$MOUNT/file1 bs=64M count=10
	#dd if=/dev/zero of=/tmp/hdfsmnt/1 bs=1M count=1024
}

case "$1" in
restart)
        restart $2
        ;;
stop)
        stop
        ;;
cleanfs)
        cleanfs
        ;;
simple)
        run_simple 
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
                echo "Namenode not running...."
        fi
        ;;
*)
        echo "Usage: namenode {start|stop|restart}"
        ;;
esac
