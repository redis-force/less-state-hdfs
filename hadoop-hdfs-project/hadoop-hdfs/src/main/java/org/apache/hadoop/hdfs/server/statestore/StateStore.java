package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.blockmanagement.*;

public abstract class StateStore {
  public abstract long tso();
  public abstract long[] tso(int size);

  public abstract INodeFileMeta createFile(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime, long header);

  public abstract INodeDirectoryMeta mkdir(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime);

  public abstract INodeMeta getDirectoryChild(long directoryId, byte[] name);

  public abstract INodeMeta[] getDirectoryChildren(long directoryId);

  public abstract INodeFileMeta getFile(long fileId);

  public abstract INodeDirectoryMeta getDirectory(long directoryId);

  public abstract BlockMeta getFileBlock(long fileId, int index);

  public abstract BlockMeta addBlock(long fileId, long blockId, long generationTimestamp);

  public abstract BlockMeta[] updateBlocks(long fileId, BlockInfo[] blocks);

  public abstract void truncateBlocks(long fileId, int size);

  public abstract BlockMeta updateBlock(long fileId, int atIndex, BlockInfo block);

  public abstract void setParent(long newParentId, long oldParentId, long id);

  public abstract void removeDirectoryChild(long directoryId, byte[] name);

  public abstract void removeDirectory(long directoryId);

  public abstract void removeFile(long fileId);

  private static StateStore STORE = new MockStateStore();

  public static StateStore get() {
    return STORE;
  }
}
