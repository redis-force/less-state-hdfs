package org.apache.hadoop.hdfs.server.statestore;

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

  private static StateStore STORE = new MockStateStore();

  public static StateStore get() {
    return STORE;
  }
}
