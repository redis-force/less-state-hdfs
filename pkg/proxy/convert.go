package proxy

import (
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/redis-force/less-state-hdfs/pkg/model"
	pb "github.com/redis-force/less-state-hdfs/pkg/proto"
)

func modelBlockToINode(blocks []*model.Block) ([]*pb.BlockMeta, []*pb.BlockStorage, []*pb.INodeFileBlock) {
	ib := make([]*pb.BlockMeta, len(blocks))
	sb := make([]*pb.BlockStorage, len(blocks))
	ifb := make([]*pb.INodeFileBlock, len(blocks))
	for i, bs := range blocks {
		ib[i] = &pb.BlockMeta{
			Id:           proto.Int64(bs.ID),
			Generation:   proto.Int64(bs.Generation),
			NumberBytes:  proto.Int64(bs.NumberBytes),
			Replication:  proto.Int32(int32(bs.Replication)),
			CollectionId: proto.Int64(bs.CollectionID),
			BlockPoolId:  proto.String(bs.BlockPoolID),
		}
		sb[i] = &pb.BlockStorage{
			Id:    proto.Int64(bs.ID),
			Nodes: make([]*pb.BlockStorageNode, len(bs.Storage)),
		}
		for j, s := range bs.Storage {
			sb[i].Nodes[j] = &pb.BlockStorageNode{
				DataNodeId: proto.String(s.DataNodeID),
				StorageId:  proto.String(s.StorageID),
			}
		}
		ifb[i] = &pb.INodeFileBlock{
			Id:          proto.Int64(bs.ID),
			NumberBytes: proto.Int64(bs.NumberBytes),
		}
		if i < len(blocks)-2 {
			ifb[i].NextBlockId = proto.Int64(blocks[i+1].ID)
		}
	}
	return ib, sb, ifb
}

func modelINodeFileToPbINode(node *model.INodeFile) (*pb.INodeMeta, []*pb.BlockMeta, []*pb.BlockStorage, []*pb.INodeFileBlock) {
	im := &pb.INodeMeta{
		Id:               proto.Int64(node.ID),
		Name:             proto.String(node.Name),
		Permission:       proto.Int64(node.Permission),
		ModificationTime: proto.Int64(node.ModificationTime),
		AccessTime:       proto.Int64(node.AccessTime),
		Header:           proto.Int64(node.Header),
		Type:             proto.Int32(int32(node.Type)),
		ParentId:         proto.Int64(node.ParentID),
		ClientName:       proto.String(node.ClientName),
		ClientMachine:    proto.String(node.ClientMachine),
	}
	ib, sb, ifb := modelBlockToINode(node.Blocks)
	return im, ib, sb, ifb
}

func modelINodeDirectoryToPbINodeDirectory(dir *model.INodeDirectory) (*pb.INodeMeta, []*pb.INodeMeta) {
	im := &pb.INodeMeta{
		Id:               proto.Int64(dir.ID),
		Name:             proto.String(dir.Name),
		Permission:       proto.Int64(dir.Permission),
		ModificationTime: proto.Int64(dir.ModificationTime),
		AccessTime:       proto.Int64(dir.AccessTime),
		Header:           proto.Int64(dir.Header),
		Type:             proto.Int32(int32(dir.Type)),
		ParentId:         proto.Int64(dir.ParentID),
	}
	children := make([]*pb.INodeMeta, len(dir.Children))
	for i, child := range dir.Children {
		_ = i
		_ = child
	}
	return im, children
}

func pbBlockMetaToBlock(m *pb.BlockMeta, s *pb.BlockStorage) *model.Block {
	bl := &model.Block{
		ID:           m.GetId(),
		Generation:   m.GetGeneration(),
		Replication:  int16(m.GetReplication()),
		CollectionID: m.GetCollectionId(),
		BlockPoolID:  m.GetBlockPoolId(),
	}
	bl.Storage = make([]model.BlockStorage, len(s.GetNodes()))
	for i, node := range s.GetNodes() {
		bl.Storage[i] = model.BlockStorage{
			DataNodeID: node.GetDataNodeId(),
			StorageID:  node.GetStorageId(),
		}
	}
	return bl
}

func pbINodeMetaToINode(m *pb.INodeMeta, n *model.INode) {
	n.ID = m.GetId()
	n.Name = m.GetName()
	n.Permission = m.GetPermission()
	n.ModificationTime = m.GetModificationTime()
	n.AccessTime = m.GetAccessTime()
	n.Header = m.GetHeader()
	n.Type = int16(m.GetType())
	n.ParentID = m.GetParentId()
}

func pbINodeMetaToSimpleINode(m *pb.INodeMeta) *model.INode {
	return &model.INode{

		ID:               m.GetId(),
		Name:             m.GetName(),
		Permission:       m.GetPermission(),
		ModificationTime: m.GetModificationTime(),
		AccessTime:       m.GetAccessTime(),
		Header:           m.GetHeader(),
		Type:             int16(m.GetType()),
		ParentID:         m.GetParentId(),
	}

}

func pbINodeFileToINodeFile(m *pb.INodeMeta, bs []*model.Block) *model.INodeFile {
	f := &model.INodeFile{
		Blocks:           bs,
		ID:               m.GetId(),
		Name:             m.GetName(),
		Permission:       m.GetPermission(),
		ModificationTime: m.GetModificationTime(),
		AccessTime:       m.GetAccessTime(),
		Header:           m.GetHeader(),
		Type:             int16(m.GetType()),
		ParentID:         m.GetParentId(),
		ClientName:       m.GetClientName(),
		ClientMachine:    m.GetClientMachine(),
	}
	return f

}

func pbBlockToBlock(bm *pb.BlockMeta, s *pb.BlockStorage) *model.Block {
	b := &model.Block{
		ID:           bm.GetId(),
		Generation:   bm.GetGeneration(),
		NumberBytes:  bm.GetNumberBytes(),
		Replication:  int16(bm.GetReplication()),
		CollectionID: bm.GetCollectionId(),
		BlockPoolID:  bm.GetBlockPoolId(),
		Storage:      make([]model.BlockStorage, len(s.GetNodes())),
	}
	for i, node := range s.GetNodes() {
		b.Storage[i] = model.BlockStorage{
			DataNodeID: node.GetDataNodeId(),
			StorageID:  node.GetStorageId(),
		}
	}
	return b
}
func bytesToInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// Converts a uint to a byte slice
func int64ToBytes(u int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(u))
	return buf
}
