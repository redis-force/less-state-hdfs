package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.blockmanagement.*;

public class BlockMeta {
  public long id;
  public long generation;
  public long numberOfBytes;
  public short replication;
  public long collectionId;
  public String blockPoolId;
  public BlockStorageMeta[] storages;

  public BlockMeta() {
  }

  public BlockMeta(BlockInfo info) {
    this.id = info.getBlockId();
    this.generation = info.getGenerationStamp();
    this.numberOfBytes = info.getNumBytes();
    this.replication = info.getReplication();
    this.collectionId = info.getBlockCollectionId();
    this.blockPoolId = info.getBlockPoolId();
  }

  public static BlockInfo convert(BlockMeta meta) {
    BlockInfo info = new BlockInfoContiguous();
    info.setBlockId(meta.id);
    info.setGenerationStamp(meta.generation);
    info.setNumBytes(meta.numberOfBytes);
    info.setReplication(meta.replication);
    info.setBlockCollectionId(meta.collectionId);
    String[] nodeId = new String[meta.storages.length];
    String[] storageId = new String[meta.storages.length];
    for (int idx = 0; idx < meta.storages.length; ++idx) {
      BlockStorageMeta storage = meta.storages[idx];
      nodeId[idx] = storage.dataNodeId;
      storageId[idx] = storage.storageId;
    }
    info.resetStorages(nodeId, storageId);
    return info;
  }

  public static BlockInfo[] convert(BlockMeta[] meta) {
    BlockInfo[] result = new BlockInfo[meta.length];
    for (int idx = 0, count = meta.length; idx < count; ++idx) {
      result[idx] = convert(meta[idx]);
    }
    return result;
  }
}
