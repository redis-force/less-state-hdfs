package proxy

import (
	"bytes"
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/redis-force/less-state-hdfs/pkg/model"
	pb "github.com/redis-force/less-state-hdfs/pkg/proto"
	"go.uber.org/zap"
)

func (s *Proxy) set(ctx context.Context, key []byte, m proto.Message) error {
	tx, err := s.store.Begin()
	err = s.transSet(ctx, tx, key, m)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) transSet(ctx context.Context, tx kv.Transaction, key []byte, m proto.Message) error {
	val, err := proto.Marshal(m)
	if err != nil {
		return err

	}
	err = tx.Set(key, val)
	if err != nil {
		return err
	}
	return nil
}

func (s *Proxy) get(ctx context.Context, key []byte, m proto.Message) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	return s.transGet(ctx, tx, key, m)
}

func (s *Proxy) transGet(ctx context.Context, tx kv.Transaction, key []byte, m proto.Message) error {
	val, err := tx.Get(key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, m)
}

func (s *Proxy) transDel(ctx context.Context, tx kv.Transaction, keys ...[]byte) error {
	var err error
	for _, key := range keys {
		err = tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Proxy) del(ctx context.Context, keys ...[]byte) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.transDel(ctx, tx, keys...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) GetBlock(ctx context.Context, id int64) (*model.Block, error) {
	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	bm := new(pb.BlockMeta)
	if err = s.transGet(ctx, tx, generateBlockMetaKey(id), bm); err != nil {
		return nil, err
	}
	bs := new(pb.BlockStorage)
	if err = s.transGet(ctx, tx, generateBlockStorageKey(id), bs); err != nil {
		return nil, err
	}
	return pbBlockMetaToBlock(bm, bs), nil
}

func (s *Proxy) PutBlock(ctx context.Context, block *pb.BlockMeta) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.transSet(ctx, tx, generateBlockMetaKey(block.GetId()), block); err != nil {
		return err
	}
	// add block to file
	return tx.Commit(ctx)
}

func (s *Proxy) DeleteBlock(ctx context.Context, id int64) error {
	return s.del(ctx, generateBlockMetaKey(id))
}

func (s *Proxy) GetBlockStorage(ctx context.Context, id int64) (*pb.BlockStorage, error) {
	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	bs := new(pb.BlockStorage)
	if err = s.transGet(ctx, tx, generateBlockStorageKey(id), bs); err != nil {
		return nil, err
	}
	return bs, nil
}
func (s *Proxy) AddBlockStorage(ctx context.Context, id int64, nodeID, storageID string) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	bs := new(pb.BlockStorage)
	if err = s.transGet(ctx, tx, generateBlockStorageKey(id), bs); err != nil && !kv.ErrNotExist.Equal(err) {
		return err
	}
	bs.Id = proto.Int64(id)
	if len(bs.Nodes) == 0 {
		bs.Nodes = make([]*pb.BlockStorageNode, 0)
	}
	for _, b := range bs.Nodes {
		if b.GetDataNodeId() == nodeID && b.GetStorageId() == storageID {
			return nil
		}
	}
	bs.Nodes = append(bs.Nodes, &pb.BlockStorageNode{
		StorageId:  proto.String(storageID),
		DataNodeId: proto.String(nodeID),
	})
	if err = s.transSet(ctx, tx, generateBlockStorageKey(id), bs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Proxy) DeleteBlockStorage(ctx context.Context, id int64) *pb.BlockStorage {
	return nil
}

func (s *Proxy) GetINodeFile(ctx context.Context, id int64, simple bool) (*pb.INodeMeta, []*model.Block, error) {
	m := new(pb.INodeMeta)
	tx, err := s.store.Begin()
	if err != nil {
		return nil, nil, err
	}
	if err = s.transGet(ctx, tx, generateINodeKey(id), m); err != nil {
		s.logger.Error("GetINodeFile error", zap.Int64("id", id), zap.Error(err))
		return nil, nil, err
	}
	if simple {
		return m, nil, nil
	}
	ret, err := s.scanINodeBlocks(ctx, tx, id)
	if err != nil {
		s.logger.Error("scanINodeBlocks error", zap.Int64("id", id), zap.Error(err))
	}
	return m, ret, err
}

func (s *Proxy) PutINodeFile(ctx context.Context, m *pb.INodeMeta) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.transSet(ctx, tx, generateINodeKey(m.GetId()), m); err != nil {
		tx.Rollback()
		return err
	}
	// if m.GetParentId() > 0 {
	if err = s.linkNode(ctx, tx, m.GetParentId(), m); err != nil {
		tx.Rollback()
		return err
	}
	// }
	return tx.Commit(ctx)
}

func (s *Proxy) deleteINodeFile(ctx context.Context, tx kv.Transaction, id int64) error {
	blocks, err := s.scanINodeBlocks(ctx, tx, id)
	if err != nil {
		return err
	}
	// 删除之前的block
	for i, b := range blocks {
		if err = s.transDel(ctx, tx, generateINodeFileBlockKey(id, int64(i)), generateBlockMetaKey(b.ID), generateBlockStorageKey(b.ID)); err != nil {
			return err
		}
	}
	return s.transDel(ctx, tx, generateINodeFileKey(id))
}

func (s *Proxy) DeleteINodeFile(ctx context.Context, id int64) error {
	//TODO delete inode block
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.deleteINodeFile(ctx, tx, id); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) GetINodeDirectory(ctx context.Context, id int64) (*pb.INodeMeta, error) {
	m := new(pb.INodeMeta)
	err := s.get(ctx, generateINodeKey(id), m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Proxy) PutINodeDirectory(ctx context.Context, m *pb.INodeMeta) error {
	return s.PutINodeFile(ctx, m)
}

func (s *Proxy) deleteDirectory(ctx context.Context, tx kv.Transaction, id int64, deleted map[int64]bool) error {
	if _, ok := deleted[id]; ok {
		return nil
	}
	deleted[id] = true
	children, err := s.listINodeDirectory(ctx, tx, id, false)
	if err != nil {
		return err
	}
	for _, child := range children {
		if child.Type == 0 {
			//DELETE FILE
			if err = s.deleteINodeFile(ctx, tx, id); err != nil {
				return err
			}
		} else {
			if err = s.deleteDirectory(ctx, tx, child.ID, deleted); err != nil {
				return err
			}
		}
		if err = s.transDel(ctx, tx, generateINodeDirectoryChildKey(id, child.Name)); err != nil {
			return err
		}
	}
	if err = s.transDel(ctx, tx, generateINodeFileKey(id)); err != nil {
		return err
	}
	return nil
}

func (s *Proxy) DeleteINodeDirectory(ctx context.Context, id int64) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	deleteMap := make(map[int64]bool)
	if err = s.deleteDirectory(ctx, tx, id, deleteMap); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

//GetINodeDirectoryChild get inode directory child by name
func (s *Proxy) GetINodeDirectoryChild(ctx context.Context, id int64, name string, needMore bool) (*pb.INodeMeta, error) {
	m := new(pb.INodeID)
	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	if err = s.transGet(ctx, tx, generateINodeDirectoryChildKey(id, name), m); err != nil {
		return nil, err
	}
	nm := new(pb.INodeMeta)
	nm.Id = m.Id
	if needMore {
		if err = s.transGet(ctx, tx, generateINodeKey(m.GetId()), nm); err != nil {
			return nil, err
		}
	}
	return nm, nil
}
func (s *Proxy) linkNode(ctx context.Context, tx kv.Transaction, parentID int64, node *pb.INodeMeta) error {
	var err error
	id := new(pb.INodeID)
	id.Id = node.Id
	s.logger.Info("linkNode", zap.Int64("id", id.GetId()), zap.Int64("parent", parentID))
	if err != nil {
		return err
	}
	if err = s.transSet(ctx, tx, generateINodeDirectoryChildKey(parentID, node.GetName()), id); err != nil {
		return err
	}
	return nil
}

//PutINodeDirectoryChild put inode directory child by name
func (s *Proxy) PutINodeDirectoryChild(ctx context.Context, directoryID int64, node *pb.INodeMeta) error {
	tx, err := s.store.Begin()
	if err = s.transSet(ctx, tx, generateINodeKey(node.GetId()), node); err != nil {
		return err
	}
	if err = s.linkNode(ctx, tx, directoryID, node); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) DeleteINodeDirectoryChild(ctx context.Context, id int64, name string) error {
	// return nil
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.transDel(ctx, tx, generateINodeDirectoryChildKey(id, name)); err != nil {
		return err
	}
	// m := new(pb.INodeMeta)
	// if err = s.transGet(ctx, tx, generateINodeDirectoryChildKey(id, name), m); err != nil {
	// 	fmt.Printf("get inode meta error %s\n", err)
	// 	tx.Rollback()
	// 	return err
	// }
	// nm := new(pb.INodeMeta)
	// nm.Id = m.Id
	// if err = s.transGet(ctx, tx, generateINodeKey(m.GetId()), nm); err != nil {
	// 	tx.Rollback()
	// 	return err
	// }
	// switch nm.GetType() {
	// case inodeDirectoryType:
	// 	if err = s.deleteDirectory(ctx, tx, id, make(map[int64]bool)); err != nil {
	// 		tx.Rollback()
	// 		return err

	// 	}
	// case inodeFileType:
	// 	if err = s.deleteINodeFile(ctx, tx, id); err != nil {
	// 		tx.Rollback()
	// 		return err
	// 	}

	// }
	return tx.Commit(ctx)
}

func (s *Proxy) GetINodeFileBlock(ctx context.Context, id, blockID int64) (*model.Block, error) {
	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	blocks, err := s.scanINodeBlocks(ctx, tx, id)
	for _, b := range blocks {
		if b.ID == blockID {
			return b, nil
		}

	}
	return nil, kv.ErrNotExist
}

func (s *Proxy) DeleteINodeFileBlock(ctx context.Context, id, blockID int64) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	index, err := s.getFileBlockIndex(ctx, tx, id, blockID)
	if err = s.transDel(ctx, tx, generateINodeFileBlockKey(id, index), generateBlockMetaKey(blockID), generateBlockStorageKey(blockID)); err != nil {
		tx.Rollback()
		return err

	}
	return tx.Commit(ctx)
}

func (s *Proxy) getFileBlockIndex(ctx context.Context, tx kv.Transaction, id, blockID int64) (int64, error) {
	var index int64 = -1
	blocks, err := s.scanINodeBlocks(ctx, tx, id)
	if err != nil {
		return index, err
	}
	for i, block := range blocks {
		if block.ID == blockID {
			index = int64(i)
			break
		}
	}
	if index == -1 {
		index = int64(len(blocks))
	}
	return index, err
}

func (s *Proxy) UpdateINodeFileBlock(ctx context.Context, id, blockID int64, block *model.Block) error {
	ib, sb, ifb := modelBlockToINode([]*model.Block{block})
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.updateINodeFileBlock(ctx, tx, id, blockID, ifb[0], ib[0], sb[0]); err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit(ctx)
	return nil
}

func (s *Proxy) updateINodeFileBlock(ctx context.Context, tx kv.Transaction, id, blockID int64, m *pb.INodeFileBlock, bm *pb.BlockMeta, bs *pb.BlockStorage) error {
	index, err := s.getFileBlockIndex(ctx, tx, id, blockID)
	if err != nil {
		return err
	}
	if index >= 1 {
		pre := new(pb.INodeFileBlock)
		preBlockKey := generateINodeFileBlockKey(id, index-1)
		if err = s.transGet(ctx, tx, preBlockKey, pre); err != nil {
			return err
		}
		pre.NextBlockId = proto.Int64(blockID)
		if err = s.transSet(ctx, tx, preBlockKey, pre); err != nil {
			return err
		}
	}
	if err = s.transSet(ctx, tx, generateINodeFileBlockKey(id, index), m); err != nil {
		return err
	}
	if err := s.transSet(ctx, tx, generateBlockMetaKey(blockID), bm); err != nil {
		return err
	}

	oldBs := new(pb.BlockStorage)
	blockStorageKey := generateBlockStorageKey(blockID)
	if err = s.transGet(ctx, tx, blockStorageKey, bs); err != nil && !kv.ErrNotExist.Equal(err) {
		return err
	}
	oldBs.Id = proto.Int64(id)
	if len(oldBs.Nodes) == 0 {
		oldBs.Nodes = make([]*pb.BlockStorageNode, 0)
	}
	nodesLen := len(bs.Nodes)
	for i := 0; i < nodesLen; i++ {
		found := false
		for _, bn := range bs.Nodes {
			if bs.Nodes[i].GetDataNodeId() == bn.GetDataNodeId() && bs.Nodes[i].GetStorageId() == bn.GetStorageId() {
				found = true
			}
		}
		if !found {
			oldBs.Nodes = append(oldBs.Nodes, &pb.BlockStorageNode{
				StorageId:  proto.String(bs.Nodes[i].GetStorageId()),
				DataNodeId: proto.String(bs.Nodes[i].GetDataNodeId()),
			})
		}
	}
	if err := s.transSet(ctx, tx, blockStorageKey, oldBs); err != nil {
		return err
	}
	return nil
}

func (s *Proxy) PutINodeFileBlock(ctx context.Context, id, blockID, generationTime int64) error {
	m := new(pb.INodeFileBlock)
	m.Id = proto.Int64(blockID)

	bm := new(pb.BlockMeta)
	bm.Id = proto.Int64(blockID)
	bm.Generation = proto.Int64(generationTime)

	bs := new(pb.BlockStorage)
	bs.Id = proto.Int64(blockID)

	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.updateINodeFileBlock(ctx, tx, id, blockID, m, bm, bs); err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit(ctx)
	return nil
}

func (s *Proxy) listINodeDirectory(ctx context.Context, tx kv.Transaction, id int64, simple bool) ([]*model.INode, error) {
	prefixKey := generateINodeDirectoryChildScanKey(id)
	it, err := tx.Iter(prefixKey, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Close()
	ret := make([]*model.INode, 0)
	for it.Valid() {
		key, val := it.Key(), it.Value()
		if !bytes.HasPrefix(key, prefixKey) {
			break
		}
		m := new(pb.INodeID)
		if err = proto.Unmarshal(val, m); err != nil {
			return nil, err
		}
		keySplited := bytes.SplitN(key, prefixKey, 2)
		if len(keySplited) != 2 {
			it.Next()
			continue
		}
		ret = append(ret, &model.INode{
			ID:   m.GetId(),
			Name: string(keySplited[1]),
		})
		it.Next()
	}
	if !simple {
		for _, n := range ret {
			m := new(pb.INodeMeta)
			err := s.transGet(ctx, tx, generateINodeKey(n.ID), m)
			if err != nil {
				return nil, err
			}
			n.ID = m.GetId()
			n.Permission = m.GetPermission()
			n.ModificationTime = m.GetModificationTime()
			n.AccessTime = m.GetAccessTime()
			n.Header = m.GetHeader()
			n.Type = int16(m.GetType())
			n.ParentID = m.GetParentId()
		}
	}
	return ret, nil
}

func (s *Proxy) GetINodeDirectoryChildren(ctx context.Context, id int64, simple bool) ([]*model.INode, error) {
	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	return s.listINodeDirectory(ctx, tx, id, simple)
}

func (s *Proxy) UpdateINodeParent(ctx context.Context, id, newParent, old int64) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	inodeKey := generateINodeKey(id)
	m := new(pb.INodeMeta)
	err = s.transGet(ctx, tx, inodeKey, m)
	if err != nil {
		return err
	}
	om := new(pb.INodeMeta)
	err = s.transGet(ctx, tx, generateINodeKey(old), om)
	if err != nil {
		return err
	}
	nm := new(pb.INodeMeta)
	err = s.transGet(ctx, tx, generateINodeKey(newParent), nm)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	//TODO get id and old inode, modify parent and children
	m.ParentId = proto.Int64(newParent)
	err = s.transSet(ctx, tx, inodeKey, m)
	if err != nil {
		return err
	}
	if err = s.transDel(ctx, tx, generateINodeDirectoryChildKey(old, om.GetName())); err != nil {
		return err
	}
	if err = s.transSet(ctx, tx, generateINodeDirectoryChildKey(newParent, om.GetName()), &pb.INodeID{Id: proto.Int64(id)}); err != nil {
		return err
	}
	tx.Commit(ctx)
	return nil
}

func (s *Proxy) scanINodeBlocks(ctx context.Context, tx kv.Transaction, id int64) ([]*model.Block, error) {
	prefix := generateINodeFileBlockScanKey(id)
	it, err := tx.Iter(prefix, nil)
	if err != nil {
		s.logger.Error("scanINodeBlocks iter prefix error", zap.Int64("id", id), zap.Error(err))
		return nil, err
	}
	ret := make([]*model.Block, 0)
	defer it.Close()
	for it.Valid() {
		key, val := it.Key(), it.Value()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		keySplited := bytes.SplitN(key, prefix, 2)
		if len(keySplited) != 2 || len(keySplited[1]) == 0 {
			it.Next()
			continue
		}
		m := new(pb.INodeFileBlock)
		if err = proto.Unmarshal(val, m); err != nil {
			return nil, err
		}
		bm := new(pb.BlockMeta)
		if err = s.transGet(ctx, tx, generateBlockMetaKey(m.GetId()), bm); err != nil {
			return nil, err
		}
		bs := new(pb.BlockStorage)
		if err = s.transGet(ctx, tx, generateBlockStorageKey(m.GetId()), bs); err != nil {
			return nil, err
		}
		ret = append(ret, pbBlockToBlock(bm, bs))
		it.Next()
	}
	return ret, nil
}

func (s *Proxy) TruncateINodeFile(ctx context.Context, id, size int64) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	blocks, err := s.scanINodeBlocks(ctx, tx, id)
	if err != nil {
		tx.Rollback()
		return err
	}
	var totalSize, endBlockSizeNew int64
	endBlockIndex := -1
	for i, b := range blocks {
		totalSize += b.NumberBytes
		if totalSize == size {
			endBlockIndex = i - 1
			endBlockSizeNew = -1 //not update
		} else if totalSize > size {
			endBlockSizeNew = b.NumberBytes - (totalSize - size)
			endBlockIndex = i
		} else {
			if endBlockIndex != -1 {
				// 删除多余的block
				if err = s.transDel(ctx, tx, generateBlockMetaKey(b.ID), generateINodeFileBlockKey(id, int64(i)), generateBlockStorageKey(b.ID)); err != nil {
					tx.Rollback()
					return err
				}
			}
		}
	}
	if endBlockIndex >= 0 {
		block := blocks[endBlockIndex]
		m := new(pb.INodeFileBlock)
		m.Id = proto.Int64(block.ID)
		if endBlockSizeNew == -1 {
			m.NumberBytes = proto.Int64(block.NumberBytes)
		} else {
			m.NumberBytes = proto.Int64(endBlockSizeNew)
		}
		m.NextBlockId = proto.Int64(0)
		if err = s.transSet(ctx, tx, generateINodeFileBlockKey(id, int64(endBlockIndex)), m); err != nil {
			tx.Rollback()
			return err
		}
		bm := &pb.BlockMeta{
			Id:           proto.Int64(block.ID),
			Generation:   proto.Int64(block.Generation),
			NumberBytes:  proto.Int64(endBlockSizeNew),
			Replication:  proto.Int32(int32(block.Replication)),
			CollectionId: proto.Int64(block.CollectionID),
			BlockPoolId:  proto.String(block.BlockPoolID),
		}
		if err = s.transSet(ctx, tx, generateBlockMetaKey(block.ID), bm); err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit(ctx)
	return nil
}

func (s *Proxy) UpdateINodeFile(ctx context.Context, node *model.INodeFile) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	blocks, err := s.scanINodeBlocks(ctx, tx, node.ID)
	// 删除之前的block
	for i, b := range blocks {
		if err = s.transDel(ctx, tx, generateINodeFileBlockKey(node.ID, int64(i)), generateBlockMetaKey(b.ID), generateBlockStorageKey(b.ID)); err != nil {
			tx.Rollback()
			return err
		}
	}
	im, bm, bs, ifb := modelINodeFileToPbINode(node)
	if err = s.transSet(ctx, tx, generateINodeFileKey(node.ID), im); err != nil {
		tx.Rollback()
		return err
	}
	for i, b := range bm {
		if err = s.transSet(ctx, tx, generateINodeFileBlockKey(node.ID, int64(i)), ifb[i]); err != nil {
			return err
		}
		if err = s.transSet(ctx, tx, generateBlockMetaKey(node.ID), b); err != nil {
			return err
		}
		if err = s.transSet(ctx, tx, generateBlockStorageKey(b.GetId()), bs[i]); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Proxy) UpdateINodeDirectory(ctx context.Context, dir *model.INodeDirectory) error {
	// tx, err := s.store.Begin()
	// if err != nil {
	// 	return err
	// }
	// if err = s.deleteDirectory(ctx, tx, dir.ID, make(map[int64]bool)); err != nil {
	// 	return err
	// }
	return nil
}
