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
	Count     int      `json:"count"`
}

//Block hdfs file block
type Block struct {
	ID           int64          `json:"id"`
	Generation   int64          `json:"generation"`
	NumberBytes  int64          `json:"number_bytes"`
	Replication  int16          `json:"replication"`
	CollectionID int64          `json:"collection_id"`
	BlockPoolID  string         `json:"block_pool_id"`
	Storage      []BlockStorage `json:"storage"`
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
	ModificationTime int64  `json:"modification_time"`
	AccessTime       int64  `json:"access_time"`
	Header           int64  `json:"header"`
	Type             int16  `json:"type"`
	ParentID         int64  `json:"parent_id"`
}

//INodeDirectory hdfs directory
type INodeDirectory struct {
	ID               int64   `json:"id"`
	Name             string  `json:"name"`
	Permission       int64   `json:"permission"`
	ModificationTime int64   `json:"modification_time"`
	AccessTime       int64   `json:"access_time"`
	Header           int64   `json:"header"`
	Type             int16   `json:"type"`
	ParentID         int64   `json:"parent_id"`
	Children         []INode `json:"children"`
}

//InodeFile hdfs file
type INodeFile struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	Permission       int64  `json:"permission"`
	ModificationTime int64  `json:"modification_time"`
	AccessTime       int64  `json:"access_time"`
	Header           int64  `json:"header"`
	Type             int16  `json:"type"`
	ParentID         int64  `json:"parent_id"`

	ClientName    string `json:"client_name"`
	ClientMachine string `json:"client_machine"`

	Blocks []*Block `json:"blocks"`
}
