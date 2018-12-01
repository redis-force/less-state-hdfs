package org.apache.hadoop.hdfs.server.statestore;

import java.util.concurrent.atomic.*;

public class MockStateStore implements StateStore {
  private static AtomicLong current = new AtomicLong(1024*1024); /* in case conflict with some default magic number */
  public long tso() {
    return current.incrementAndGet();
  }

  public long[] tso(int size) {
    long[] result = new long[size];
    for (int idx = 0; idx < size; ++idx) {
      result[idx] = current.incrementAndGet();
    }
    return result;
  }

  public INodeFileMeta createFile(long parent, long id, String name, long permission, long modificationTime, long accessTime, long header) {
    return new INodeFileMeta(parent, id, name, permission, modificationTime, accessTime, header);
  }

  public INodeDirectoryMeta mkdir(long parent, long id, String name, long permission, long modificationTime, long accessTime) {
    return new INodeDirectoryMeta(parent, id, name, permission, modificationTime, accessTime);
  }

  public INodeMeta getDirectoryChild(long directoryId, String name) {
    return null;
  }

  public INodeMeta[] getDirectoryChildren(long directoryId) {
    return null;
  }

  public INodeFileMeta getFile(long fileId) {
    return null;
  }

  public INodeDirectoryMeta getDirectory(long directoryId) {
    return null;
  }

  public BlockMeta getFileBlock(long fileId, int index) {
    return null;
  }

  public BlockMeta addBlock(long fileId, long blockId, long generationTimestamp) {
    return null;
  }
}
