package proxy

import (
	"context"

	"github.com/golang/protobuf/proto"
	pb "github.com/redis-force/less-state-hdfs/pkg/proto"
)

func (s *Proxy) set(ctx context.Context, key []byte, m proto.Message) error {
	val, err := proto.Marshal(m)
	if err != nil {
		return err

	}
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	err = tx.Set(key, val)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) del(ctx context.Context, key []byte) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	err = tx.Delete(key)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Proxy) get(ctx context.Context, key []byte, m proto.Message) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}
	val, err := tx.Get(key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, m)
}

func (s *Proxy) GetBlockMeta(ctx context.Context, id int64) (*pb.BlockMeta, error) {
	m := new(pb.BlockMeta)
	err := s.get(ctx, generateBlockMetaKey(id), m)
	if err != nil {
		return nil, err
	}
	return m, err
}

func (s *Proxy) PutBlockMeta(ctx context.Context, block *pb.BlockMeta) error {
	return s.set(ctx, generateBlockMetaKey(block.GetId()), block)
}

func (s *Proxy) DeleteBlockMeta(ctx context.Context, id int64) *pb.BlockMeta {
	return nil
}

func (s *Proxy) GetBlockStorage(ctx context.Context, id int64) *pb.BlockStorage {
	return nil
}

func (s *Proxy) GetINodeFile(ctx context.Context, id int64) (*pb.INodeMeta, error) {
	m := new(pb.INodeMeta)
	err := s.get(ctx, generateINodeFileKey(m.GetId()), m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Proxy) PutINodeFile(ctx context.Context, m *pb.INodeMeta) error {
	return s.set(ctx, generateINodeFileKey(m.GetId()), m)
}

func (s *Proxy) DeleteINodeFile(ctx context.Context, id int64) error {
	return s.del(ctx, generateINodeFileKey(id))
}

func (s *Proxy) GetINodeDirectory(ctx context.Context, id int64) (*pb.INodeMeta, error) {
	m := new(pb.INodeMeta)
	err := s.get(ctx, generateINodeDirectoryKey(m.GetId()), m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Proxy) PutINodeDirectory(ctx context.Context, m *pb.INodeMeta) error {
	return s.set(ctx, generateINodeDirectoryKey(m.GetId()), m)
}

func (s *Proxy) DeleteINodeDirectory(ctx context.Context, id int64) error {
	return s.del(ctx, generateINodeDirectoryKey(id))
}
