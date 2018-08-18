gen-py: namenode.thrift datanode.thrift 
	thrift --gen py namenode.thrift
	thrift --gen py datanode.thrift
tgz: gen-py
	tar cvfz HDFS.tgz README Makefile HDFSCommons.py HDFS.py BlockManager.py NamenodeServer.py NamenodeClient.py DatanodeServer.py DatanodeClient.py namenode.thrift datanode.thrift namenode.sh datanode.sh fusepy thrift gen-py  --exclude .svn*

clean:
	rm -f *~
	rm -f *.pyc
	rm -Rf gen-py
