package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

public class SwappableBlock extends Block {
  /* todo make fields swappable */
  private long blockId;
  private long numBytes;
  private long generationStamp;

  public SwappableBlock() {
  }

  public SwappableBlock(Block block) {
    super(block);
  }

  public SwappableBlock(final long blkid) {
    super(blkid);
  }

  public SwappableBlock(final long blkid, final long len, final long generationStamp) {
    super(blkid, len, generationStamp);
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStamp(long stamp) {
    generationStamp = stamp;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public void setNumBytes(long len) {
    this.numBytes = len;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long bid) {
    blockId = bid;
  }
}
