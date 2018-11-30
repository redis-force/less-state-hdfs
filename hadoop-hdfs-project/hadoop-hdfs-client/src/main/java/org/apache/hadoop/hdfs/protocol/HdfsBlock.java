package org.apache.hadoop.hdfs.protocol;

public class HdfsBlock extends Block {
  private long blockId;
  private long numBytes;
  private long generationStamp;

  public HdfsBlock() {
  }

  public HdfsBlock(final long blkid) {
    super(blkid);
  }

  public HdfsBlock(Block block) {
    super(block);
  }

  public HdfsBlock(final long blkid, final long len, final long generationStamp) {
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
