package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.blockmanagement.*;

public class BlockMeta {
  public long id;
  public long generation;
  public long numberOfBytes;
  public short replication;
  public long collectionId;
  public BlockStorageMeta[] storages;

  public static BlockInfo convert(BlockMeta meta) {
    return null;
  }

  public static BlockInfo[] convert(BlockMeta[] meta) {
    BlockInfo[] result = new BlockInfo[meta.length];
    for (int idx = 0, count = meta.length; idx < count; ++idx) {
      result[idx] = convert(meta[idx]);
    }
    return result;
  }
}
