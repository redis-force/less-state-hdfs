package org.apache.hadoop.hdfs.server.statestore;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.*;

import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.fs.permission.*;

public class INodeFileMeta extends INodeMeta {
  public static final short TYPE = 0;
  public static BlockMeta[] EMPTY = new BlockMeta[0];
  public BlockMeta[] blocks;
  public String clientName;
  public String clientMachine;

  private int dirtyFlags;

  public static final int DIRTY_BLOCKS = 1 << 1;
  public static final int DIRTY_UNDER_CONSTRUCTION = 1 << 2;

  public INodeFileMeta() {
  }

  public INodeFileMeta(long parentId, long id, byte[] name, long permission, long modificationTime, long accessTime, long header) {
    super(parentId, id, name, permission, modificationTime, accessTime, header, TYPE);
    this.blocks = EMPTY;
  }

  public INodeFileMeta(long parentId, long id, byte[] name, long permission, long modificationTime, long accessTime, long header, String clientName, String clientMachine) {
    this(parentId, id, name, permission, modificationTime, accessTime, header);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
  }

  public INodeFileMeta(INodeFile file, int flags) {
    super(file, file.getHeaderLong(), TYPE);
    this.dirtyFlags = flags;
  }

  public static INodeFile convert(INodeMeta meta, PermissionStatus ps) {
    Optional<BlockInfo[]> blocks = (meta instanceof INodeFileMeta && ((INodeFileMeta) meta).blocks != EMPTY) ?
      Optional.of(BlockMeta.convert(((INodeFileMeta) meta).blocks)) : Optional.empty();
    return new INodeFile(meta.id,
        DFSUtil.string2Bytes(meta.name),
        ps,
        meta.modificationTime,
        meta.accessTime,
        blocks,
        INodeFile.getReplication(meta.header),
        INodeFile.getPreferredBlockSize(meta.header));
  }
}
