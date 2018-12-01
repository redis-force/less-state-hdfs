package org.apache.hadoop.hdfs.server.statestore;

public class INodeDirectoryMeta extends INodeMeta {
  public static short TYPE = 1;
  public static INodeMeta[] EMPTY = new INodeMeta[0];
  public INodeMeta[] children;

  public INodeDirectoryMeta(long parentId, long id, String name, long permission, long modificationTime, long accessTime) {
    super(parentId, id, name, permission, modificationTime, accessTime, 0, TYPE);
    this.children = EMPTY;
  }
}
