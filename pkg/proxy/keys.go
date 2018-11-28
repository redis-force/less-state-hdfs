package proxy

import "strconv"

func generateBlockMetaKey(id int64) []byte {
	return strconv.AppendInt([]byte(`{bm}_`), id, 10)
}

func generateINodeFileKey(id int64) []byte {
	return strconv.AppendInt([]byte(`{im}_`), id, 10)
}

func generateINodeDirectoryKey(id int64) []byte {
	return strconv.AppendInt([]byte(`{id}_`), id, 10)
}
