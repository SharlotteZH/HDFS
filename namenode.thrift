exception NamenodeOSError {
	1: i32 what,
	2: string why
}

service Namenode {
	void ping(1: string host),
	
	string addBlock(1:string host, 2:string filename, 3:i32 index),
	list<string> getBlocks(1:string filename),
	list<string> getBlockLocation(1:string filename, 2:string blockId, 3:i32 index) throws (1:NamenodeOSError e), // locations, md5
	
	string getLock(1:string host, 2:string filename, 3:string blockId, 4:i32 index)
	bool delLock(1:string filename, 2:string blockId, 3:i32 index)
	
	bool registerBlock(1:string host, 2:string blockId, 3:string md5)

	void chmod(1:string path, 2:i64 mode) throws (1:NamenodeOSError e),
	void chown(1:string path, 2:i32 uid, 3:i32 gid) throws (1:NamenodeOSError e),
	void create(1:string  path, 2:i64 mode),
	map<string,string> getattr(1:string path) throws (1:NamenodeOSError e),
	void mkdir(1:string path, 2:i64 mode),
	list<string> readdir(1:string path),
	string readlink(1:string path)
	void rename(1:string oldpath, 2:string newpath) throws (1:NamenodeOSError e),
	void rmdir(1:string path),
	void symlink(1:string source, 2:string link_name),
	void link(1:string source, 2:string link_name),
	void truncate(1:string path, 2:i64 length)
	void unlink(1:string path),
	void utimens(1:string path, 2:i32 atime, 3:i32 mtime),
	
	void copyOnWrite(1:string oldpath, 2:string newpath),
}
