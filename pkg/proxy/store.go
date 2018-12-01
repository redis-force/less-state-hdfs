package proxy

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/kv"
	pb "github.com/redis-force/less-state-hdfs/pkg/proto"
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
	fmt.Printf("set key %s, value %v\n", key, val)
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
	fmt.Printf("get key %s, value %v\n", key, val)
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, m)
}

func (s *Proxy) del(ctx context.Context, keys ...[]byte) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	for _, key := range keys {
		err = tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Proxy) GetBlockMeta(ctx context.Context, id int64) (*pb.BlockMeta, error) {
	m := new(pb.BlockMeta)
	if err := s.get(ctx, generateBlockMetaKey(id), m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Proxy) PutBlockMeta(ctx context.Context, block *pb.BlockMeta) error {
	return s.set(ctx, generateBlockMetaKey(block.GetId()), block)
}

func (s *Proxy) DeleteBlockMeta(ctx context.Context, id int64) error {
	return nil
}

func (s *Proxy) GetBlockStorage(ctx context.Context, id int64) *pb.BlockStorage {
	return nil
}

func (s *Proxy) GetINodeFile(ctx context.Context, id int64) (*pb.INodeMeta, error) {
	m := new(pb.INodeMeta)
	if err := s.get(ctx, generateINodeKey(id), m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Proxy) PutINodeFile(ctx context.Context, m *pb.INodeMeta) error {
	return s.set(ctx, generateINodeKey(m.GetId()), m)
}

func (s *Proxy) DeleteINodeFile(ctx context.Context, id int64) error {
	return s.del(ctx, generateINodeKey(id))
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
	return s.set(ctx, generateINodeKey(m.GetId()), m)
}

func (s *Proxy) DeleteINodeDirectory(ctx context.Context, id int64) error {
	return s.del(ctx, generateINodeKey(id))
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
	nm.Id = m.GetId()
	if needMore {
		if err = s.transGet(ctx, tx, generateINodeKey(id), nm); err != nil {
			return nil, err
		}
	}
	return nm, nil
}

//PutINodeDirectoryChild put inode directory child by name
func (s *Proxy) PutINodeDirectoryChild(ctx context.Context, node *pb.INodeMeta) error {
	id := new(pb.INodeID)
	id.Id = node.GetId()
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	if err = s.transSet(ctx, tx, generateINodeDirectoryChildKey(node.GetId(), node.GetName()), id); err != nil {
		tx.Rollback()
		return err
	}
	if err = s.transSet(ctx, tx, generateINodeKey(node.GetId()), node); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) DeleteINodeDirectoryChild(ctx context.Context, id int64, name string) error {
	return s.del(ctx, generateINodeDirectoryChildKey(id, name), generateINodeKey(id))
}
