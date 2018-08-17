#!/usr/bin/env python

BLOCK_SIZE = 64*1024*1024

TIMEOUT = 5000 # ms

HEARTBEAT_PERIOD = 5 # seconds
HEARTBEAT_LOST = 30 # seconds

REPLICATION_MIN = 2

DATANODE_PORT = 8081

NAMENODE_HOST = "ubuntu"
NAMENODE_PORT = 8082

DATANODE_ROOT = "/tmp/hdfs/datanode"
NAMENODE_ROOT = "/tmp/hdfs/namenode"

BASE_PATH = "."
