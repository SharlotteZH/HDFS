// Datanode definition

struct Block {
	1: string md5,
	2: string data,
}

service Datanode {
	void ping(),

	Block getBlock(1:string blockId),
	i32 getBlockSize(1:string blockId),
	string getBlockMD5(1:string blockId),
	void putBlock(1:string blockId, 2:Block block, 3:bool lock),
	void replicateBlock(1:string blockId, 2:string dst),
	
	string readBlock(1:string blockId, 2:i64 offset, 3:i32 length),
	void writeBlock(1:string blockId, 2:string data, 3:i64 offset),
	void truncateBlock(1:string blockId, 2:i32 length),
	void deleteBlock(1: string blockId),
	
	string getLock(1:string blockId),
	bool invalidateFile(1:string file),
}
