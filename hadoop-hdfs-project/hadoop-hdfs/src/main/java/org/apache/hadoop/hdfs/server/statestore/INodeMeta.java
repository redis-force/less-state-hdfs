package org.apache.hadoop.hdfs.server.statestore;

import java.util.*;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.permission.*;

public class INodeMeta {
  public long id;
  public String name;
  public long permission;
  public long modificationTime;
  public long accessTime;
  public long header;
  public short type;
  public long parentId;

  public static final int DIRTY_META = 1 << 0;

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

  public INodeMeta() {
  }

  public INodeMeta(INodeWithAdditionalFields inode, long header, short type) {
    this.type = type;
    this.header = header;
    this.id = inode.getId();
    this.name = inode.getLocalName();
    this.permission = inode.getPermissionLong();
    this.modificationTime = inode.getModificationTime();
    this.accessTime = inode.getAccessTime();
    this.parentId = inode.getParent().getId();
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

  public INodeWithAdditionalFields convert() {
    PermissionStatus ps = FSNamesystem.get().createFsOwnerPermissions(new FsPermission(INodeWithAdditionalFields.getFsPermissionShort(permission)));
    if (this.getClass() == INodeMeta.class) {
      switch (this.type) {
        case INodeFileMeta.TYPE:
          return StateStore.get().getFile(id).convert();
        case INodeDirectoryMeta.TYPE:
          return StateStore.get().getDirectory(id).convert();
        default:
          throw new IllegalStateException("Unknown inode type:" + type);
      }
    } else {
      switch (type) {
        case INodeFileMeta.TYPE:
          return INodeFileMeta.convert(this, ps);
        case INodeDirectoryMeta.TYPE:
          return INodeDirectoryMeta.convert(this, ps);
        default:
          throw new IllegalStateException("Unknown inode type:" + type);
      }
    }
  }

  public static List<INode> convert(INodeMeta[] meta) {
    ArrayList<INode> result = new ArrayList(meta.length);
    for (INodeMeta m : meta) {
      result.add(m.convert());
    }
    return result;
  }
}
