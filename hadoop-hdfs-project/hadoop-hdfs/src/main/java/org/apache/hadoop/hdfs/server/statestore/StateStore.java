package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.blockmanagement.*;

public abstract class StateStore {
  public abstract long tso();
  public abstract long[] tso(int size);

  public abstract INodeFileMeta createFile(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime, long header, String clientName, String clientMachine);

  public abstract INodeFileMeta createFile(INodeFileMeta meta);

  public abstract INodeDirectoryMeta mkdir(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime);

  public abstract INodeDirectoryMeta mkdir(INodeDirectoryMeta meta);

  public abstract INodeMeta getDirectoryChild(long directoryId, byte[] name);

  public abstract INodeMeta[] getDirectoryChildren(long directoryId);

  public abstract INodeFileMeta getFile(long fileId);

  public abstract INodeDirectoryMeta getDirectory(long directoryId);

  public abstract BlockMeta getFileBlock(long fileId, int index);

  public abstract BlockMeta addBlock(long fileId, long blockId, long generationTimestamp);

  public abstract BlockMeta[] updateBlocks(long fileId, BlockMeta[] blocks);

  public abstract void truncateBlocks(long fileId, int size);

  public abstract BlockMeta updateBlock(long fileId, BlockMeta block);

  public abstract void rename(long oldParentId, INodeMeta inode);

  public abstract void removeDirectoryChild(long directoryId, byte[] name);

  public abstract void removeDirectory(long directoryId);

  public abstract void removeFile(long fileId);

  public abstract void update(INodeFileMeta meta);
  public abstract void update(INodeDirectoryMeta meta);

  private static volatile StateStore STORE;

  public static void init(String endpoint) {
    STORE = new KVStatStore(endpoint);
  }

  public void addBlockStorage(String id, String dataNodeId, String StorageId) {}

  public static StateStore get() {
    return STORE;
  }


}
