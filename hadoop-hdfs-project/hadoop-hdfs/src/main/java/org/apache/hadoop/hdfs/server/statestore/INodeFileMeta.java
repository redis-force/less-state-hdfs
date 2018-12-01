package org.apache.hadoop.hdfs.server.statestore;

public class INodeFileMeta extends INodeMeta {
  public static short TYPE = 0;
  public static BlockMeta[] EMPTY = new BlockMeta[0];
  public BlockMeta[] blocks;

  public INodeFileMeta(long parentId, long id, String name, long permission, long modificationTime, long accessTime, long header) {
    super(parentId, id, name, permission, modificationTime, accessTime, header, TYPE);
    this.blocks = EMPTY;
  }
}
