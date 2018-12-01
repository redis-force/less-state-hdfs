package org.apache.hadoop.hdfs.server.statestore;

import java.util.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class INodeMeta {
  public long id;
  public String name;
  public long permission;
  public long modificationTime;
  public long accessTime;
  public long header;
  public short type;
  public long parentId;

  public INodeMeta(long parentId, long id, byte[] name, long permission, long modificationTime, long accessTime, long header, short type) {
    this.parentId = parentId;
    this.id = id;
    this.name = new String(name, UTF_8);
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.header = header;
    this.type = type;
  }

  public Optional<INodeFileMeta> toFile() {
    if (this.type == INodeFileMeta.TYPE) {
      return Optional.of((INodeFileMeta) this);
    } else {
      return Optional.empty();
    }
  }

  public Optional<INodeDirectoryMeta> toDirectory() {
    if (this.type == INodeDirectoryMeta.TYPE) {
      return Optional.of((INodeDirectoryMeta) this);
    } else {
      return Optional.empty();
    }
  }
}
