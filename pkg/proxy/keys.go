package proxy

import (
	"fmt"
	"strconv"
)

func generateBlockMetaKey(id int64) []byte {
	return strconv.AppendInt([]byte(`{bm}_`), id, 10)
}

func generateINodeFileKey(id int64) []byte {
	return strconv.AppendInt([]byte(`{in}_`), id, 10)
}

func generateINodeKey(id int64) []byte {
	return strconv.AppendInt([]byte(`{in}_`), id, 10)
}

func generateINodeDirectoryChildKey(id int64, name string) []byte {
	return []byte(fmt.Sprintf("{id}_%d_%s", id, name))
}
