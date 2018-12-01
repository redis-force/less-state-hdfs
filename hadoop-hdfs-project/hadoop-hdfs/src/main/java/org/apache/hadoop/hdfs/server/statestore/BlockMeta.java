package org.apache.hadoop.hdfs.server.statestore;

public class BlockMeta {
  public long id;
  public long generation;
  public long numberOfBytes;
  public short replication;
  public long collectionId;
  public BlockStorageMeta[] storages;
}
