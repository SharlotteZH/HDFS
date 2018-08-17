# HDFS
HDFS style filesystem based on FUSE
Compile:
        make gen-py

Execution:
        bash exp.sh simple


Roles:
        Namenode:
                Directory with the location of the locks (and some replicas)
                Contain files
        Datanode:
                Contain the data and the locks
        FUSE:
                Interface between FS and GDFS
Components:
        File:
                List of blocks
        Block:
                Data
                64 MB
        Metainfo:
                Block md5
                Block location
                Block lock location
                File (or files it belong)
        Lock:
                Per block
                Contains the location of the other replicas
FS operations:
        Write:
                Get lock from previous lock
                Invalidate other replicas
                Write
                Update md5
        Read:
                Get location from namenode if unknown
                Subscribe to the owner of the lock (+md5)
                Get block, if more than a few bytes
                Subscribe to the namenode when it reads the block
Remote operations:
        Namenode:
                Get file location: return list of blocks
                Get block location: return owner lock + replicas, md5
        Datanode:
                Get lock
                        If the owner is missing, namenode
                Get block (pull)
                Read block
                Put block (push)
                Invalidate block
                Create block (append to file)
High level operations:
        Read file:
                Get block location from namenode ()
