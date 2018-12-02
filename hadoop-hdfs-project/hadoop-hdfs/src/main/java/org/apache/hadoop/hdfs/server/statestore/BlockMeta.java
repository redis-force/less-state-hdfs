package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.protocol.*;

public class BlockMeta {
  public long id;
  public long generation;
  public long numberBytes;
  public short replication;
  public long collectionId;
  public String blockPoolId;
  public BlockStorageMeta[] storage;

  public BlockMeta() {
  }

  public BlockMeta(BlockInfo info) {
    this(info, info.getReplication(), info.getBlockCollectionId(), info.getBlockPoolId());
  }

  public BlockMeta(Block info, short replication, long collectionId, String blockPoolId) {
    this(info, replication, collectionId, blockPoolId, null);
  }

  public BlockMeta(Block info, short replication, long collectionId, String blockPoolId, DatanodeStorageInfo[] storages) {
    this.id = info.getBlockId();
    this.generation = info.getGenerationStamp();
    this.numberBytes = info.getNumBytes();
    this.replication = replication;
    this.collectionId = collectionId;
    this.blockPoolId = blockPoolId;
  }

  public static BlockInfo convert(BlockMeta meta) {
    BlockInfo info = new BlockInfoContiguous();
    info.setBlockId(meta.id);
    info.setGenerationStamp(meta.generation);
    info.setNumBytes(meta.numberBytes);
    info.setReplication(meta.replication);
    info.setBlockCollectionId(meta.collectionId);
    String[] nodeId = new String[meta.storage.length];
    String[] storageId = new String[meta.storage.length];
    for (int idx = 0; idx < meta.storage.length; ++idx) {
      BlockStorageMeta storage = meta.storage[idx];
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
