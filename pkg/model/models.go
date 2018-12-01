package model

//APIResponse rpc接口通用的响应格式
type APIResponse struct {
	Code     int         `json:"code"`
	Error    string      `json:"error"`
	Response interface{} `json:"response,omitempty"`
}

//TS timestamp
type TS struct {
	Timestamp []uint64 `json:"timestamp"`
	Count     int      `jons:"count"`
}

//Block hdfs file block
type Block struct {
	ID   int64 `json:"id"`
	Meta struct {
		Generation   int64 `json:"generation"`
		NumberBytes  int64 `json:"number_bytes"`
		Replication  int16 `json:"replication"`
		CollectionID int64 `json:"collection_id"`
	} `json:"meta"`
	Sotrage []BlockStorage `json:"storage"`
}

//BlockStorage hdfs file block storage
type BlockStorage struct {
	DataNodeID string `json:"data_node_id"`
	StorageID  string `json:"storage_id"`
}

//INode hdfs inode meta
type INode struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	Permission       int64  `json:"permission"`
	ModificationTime int64  `json:"modifycation_time"`
	AccessTime       int64  `json:"access_time"`
	Header           int64  `json:"header"`
	Type             int16  `json:"type"`
	ParentID         int64  `json:"parent_id"`
}

//INodeDirectory hdfs directory
type INodeDirectory struct {
	INode    INode   `json:"inode"`
	Children []INode `json:"children"`
}

//InodeFile hdfs file
type InodeFile struct {
	INode  INode   `json:"inode"`
	Blocks []Block `json:"blocks"`
}
