package org.apache.hadoop.hdfs.server.statestore;

public interface StateStore {
  long tso();
  long[] tso(int size);

  INodeFileMeta createFile(long parent, long id, String name, long permission, long modificationTime, long accessTime, long header);

  INodeDirectoryMeta mkdir(long parent, long id, String name, long permission, long modificationTime, long accessTime);

  INodeMeta getDirectoryChild(long directoryId, String name);

  INodeMeta[] getDirectoryChildren(long directoryId);

  INodeFileMeta getFile(long fileId);

  INodeDirectoryMeta getDirectory(long directoryId);

  BlockMeta getFileBlock(long fileId, int index);

  BlockMeta addBlock(long fileId, long blockId, long generationTimestamp);
}
