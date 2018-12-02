package proxy

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

	"runtime"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/redis-force/less-state-hdfs/pkg/model"
	pb "github.com/redis-force/less-state-hdfs/pkg/proto"
	"go.uber.org/zap"
)

const (
	inodeFileType = iota
	inodeDirectoryType
)

type bodyLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w bodyLogWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

func ginBodyLogMiddleware(c *gin.Context) {
	blw := &bodyLogWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
	c.Writer = blw
	c.Next()
	statusCode := c.Writer.Status()
	fmt.Printf("request %s, response code %d body: %s\n", c.Request.URL.String(), statusCode, blw.body.String())
}

type apiServer struct {
	proxy *Proxy
}

func newAPIServer(proxy *Proxy) *gin.Engine {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery(), ginBodyLogMiddleware)
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	})
	router.Any("/debug/*path", func(c *gin.Context) {
		http.DefaultServeMux.ServeHTTP(c.Writer, c.Request)
	})
	preCheck := func(c *gin.Context) {
		if proxy.IsClosed() {
			apiResponseError(c, http.StatusServiceUnavailable, ErrServerClosed)
			return
		}
		c.Next()
	}
	intCheck := func(params ...string) gin.HandlerFunc {
		return func(c *gin.Context) {
			for _, p := range params {
				id, err := strconv.ParseInt(c.Param(p), 10, 64)
				if err != nil {
					apiResponseError(c, http.StatusBadRequest, fmt.Errorf("%q param format error %s", p, err))
					return
				}
				if id < 0 {
					apiResponseError(c, http.StatusBadRequest, fmt.Errorf("%s format error", p))
					return
				}
				c.Set(p, id)
			}
			c.Next()
		}
	}
	server := &apiServer{proxy: proxy}
	//TODO auth middleware
	api := router.Group("/api")
	api.Use(preCheck)
	{
		api.GET("/tso", server.ts)
		blockMeta := api.Group("/block/meta")
		blockMeta.Use(intCheck("id"))
		{
			blockMeta.GET("/:id", server.getBlock)
			blockMeta.PUT("/:id", server.putBlock)
			blockMeta.DELETE("/:id", server.deleteBlock)
		}
		blockStorage := api.Group("/block/storage")
		blockStorage.Use(intCheck("id"))
		{
			blockStorage.GET("/:id", server.getBlockStorage)
			blockStorage.PUT("/:id/:data_node_id/:storage_id", server.putBlockStorage)
			blockStorage.DELETE("/:id/:data_node_id/:storage_id", server.deleteBlockStorage)
		}
		file := api.Group("/file")
		file.Use(intCheck("id"))
		{
			file.GET("/:id", server.getINodeFile)
			file.PUT("/:id", server.putINodeFile)

			file.POST("/:id", server.updateINodeFile)
			file.DELETE("/:id", server.deleteINodeFile)

			file.GET("/:id/:block_id", intCheck("block_id"), server.getINodeFileBlock)
			file.PUT("/:id/:block_id", intCheck("block_id"), server.putINodeFileBlock)
			file.POST("/:id/:block_id", intCheck("block_id"), server.updateINodeFileBlock)
			file.DELETE("/:id/:block_id", intCheck("block_id"), server.deleteINodeFileBlock)
		}
		api.PUT("/file-truncate/:id", intCheck("id", "size"), server.truncateINodeFile)

		direcotry := api.Group("/directory")
		direcotry.Use(intCheck("id"))
		{
			direcotry.GET("/:id", server.getINodeDirectory)
			direcotry.PUT("/:id", server.putINodeDirectory)
			direcotry.POST("/:id", server.updateINodeDirectory)
			direcotry.DELETE("/:id", server.deleteINodeDirectory)

			direcotry.GET("/:id/:name", server.getINodeDirectoryChild)
			direcotry.PUT("/:id/:name", server.putINodeDirectoryChild)
			direcotry.DELETE("/:id/:name", server.deleteINodeDirectoryChild)
		}
		api.GET("/directory-children/:id", intCheck("id"), server.getINodeDirectoryChildren)
		inode := api.Group("/inode")
		{
			inode.PUT("/:id/:old_id/:new_id", intCheck("id", "old_id", "new_id"), server.updateINodeParent)

		}
	}
	rt := router.Group("/runtime")
	{
		rt.PUT("/force-gc", func(c *gin.Context) {
			runtime.GC()
			apiResponseSuccess(c, nil)
		})
		rt.PUT("/force-free", func(c *gin.Context) {
			debug.FreeOSMemory()
			apiResponseSuccess(c, nil)
		})

	}
	return router
}

func apiResponseError(c *gin.Context, code int, err error) {
	c.AbortWithStatusJSON(code, model.APIResponse{Code: code, Error: err.Error()})
}

func apiResponseSuccess(c *gin.Context, resp interface{}) {
	if resp == nil {
		c.AbortWithStatusJSON(http.StatusAccepted, model.APIResponse{Code: 0, Error: "success"})
		return
	}
	c.AbortWithStatusJSON(http.StatusOK, resp)
}

func (s *apiServer) ts(c *gin.Context) {
	count, err := strconv.Atoi(c.DefaultQuery("count", "1"))
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, err)
		return
	}
	if count <= 0 {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("count should not less than 1"))
		return
	}
	ts := model.TS{
		Timestamp: make([]uint64, count),
		Count:     count,
	}
	fs := make([]oracle.Future, count)
	for i := 0; i < count; i++ {
		fs[i] = s.proxy.oracle.GetTimestampAsync(c.Request.Context())
	}
	for i := 0; i < count; i++ {
		ts.Timestamp[i], err = fs[i].Wait()
		if err != nil {
			apiResponseError(c, http.StatusInternalServerError, err)
			return
		}
	}
	apiResponseSuccess(c, ts)
}

func (s *apiServer) getBlock(c *gin.Context) {
	id := c.GetInt64("id")
	bm, err := s.proxy.GetBlock(c.Request.Context(), id)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("block id=%d not found", id))
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	//TODO get block meta
	apiResponseSuccess(c, bm)
}

func (s *apiServer) putBlock(c *gin.Context) {
	id := c.GetInt64("id")
	bm := new(pb.BlockMeta)
	err := c.ShouldBindJSON(bm)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error %s", err))
		return
	}
	if bm.GetGeneration() < 0 || bm.GetReplication() < 0 || bm.GetCollectionId() < 0 || bm.GetNumberBytes() < 0 {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error, filed must not by none %v ", bm))
		return
	}
	bm.Id = proto.Int64(id)
	err = s.proxy.PutBlock(c.Request.Context(), bm)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

func (s *apiServer) deleteBlock(c *gin.Context) {
	id := c.GetInt64("id")
	err := s.proxy.DeleteBlock(c.Request.Context(), id)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//getBlock param id
func (s *apiServer) getBlockStorage(c *gin.Context) {
	id := c.GetInt64("id")
	st, err := s.proxy.GetBlockStorage(c.Request.Context(), id)
	if err != nil {
		if kv.ErrNotExist.Equal(err) {
			apiResponseError(c, http.StatusNotFound, err)
			return
		}
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, st)
}

//putBlock param id/data_node_id/storage_id
func (s *apiServer) putBlockStorage(c *gin.Context) {
	id := c.GetInt64("id")
	dataNodeID := c.Param(("data_node_id"))
	storageID := c.Param("storage_id")
	if len(dataNodeID) == 0 || len(storageID) == 0 {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("data/storage id param error"))
		return
	}
	if err := s.proxy.AddBlockStorage(c.Request.Context(), id, dataNodeID, storageID); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, model.APIResponse{})
}

//deleteBlock param id/data_node_id/storage_id
func (s *apiServer) deleteBlockStorage(c *gin.Context) {
	// _ =
	//TODO get block meta
	apiResponseSuccess(c, nil)
}

//getINodeFile param id
func (s *apiServer) getINodeFile(c *gin.Context) {
	id := c.GetInt64("id")
	_, simple := c.GetQuery("simple")
	m, bs, err := s.proxy.GetINodeFile(c.Request.Context(), id, simple)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("inode-file id=%d not found", id))
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, pbINodeFileToINodeFile(m, bs))

}

//putINodeFile param id
func (s *apiServer) putINodeFile(c *gin.Context) {
	id := c.GetInt64("id")
	nm := new(pb.INodeMeta)
	err := c.ShouldBindJSON(nm)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error %s", err))
		return
	}
	if nm.GetParentId() <= 0 {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put file request parent id is null"))
		return
	}
	nm.Id = proto.Int64(id)
	nm.Type = proto.Int32(inodeFileType)
	s.proxy.logger.Info("putINodeFile", zap.Int64("id", id), zap.Int64("parent_id", nm.GetParentId()), zap.String("name", nm.GetName()))
	err = s.proxy.PutINodeFile(c.Request.Context(), nm)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)

}

//deleteINodeFile param id
func (s *apiServer) deleteINodeFile(c *gin.Context) {
	id := c.GetInt64("id")
	if err := s.proxy.DeleteINodeFile(c.Request.Context(), id); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//getINodeFileBlock param id/block_id
func (s *apiServer) getINodeFileBlock(c *gin.Context) {
	id := c.GetInt64("id")
	blockID := c.GetInt64("block_id")
	// index, err := strconv.ParseInt(c.DefaultQuery("index", "0"), 10, 64)
	// if err != nil {
	// 	apiResponseError(c, http.StatusBadRequest, err)
	// 	return
	// }
	m, err := s.proxy.GetINodeFileBlock(c.Request.Context(), id, blockID)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, err)
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, m)
	return
}

//putINodeFileBlock param id/block_id/index
func (s *apiServer) putINodeFileBlock(c *gin.Context) {
	id := c.GetInt64("id")
	blockID := c.GetInt64("block_id")
	generationTime, err := strconv.ParseInt(c.DefaultQuery("generation_time", "0"), 10, 64)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, err)
		return
	}
	if err := s.proxy.PutINodeFileBlock(c.Request.Context(), id, blockID, generationTime); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)

}

//putINodeFileBlock param id/block_id/index
func (s *apiServer) updateINodeFileBlock(c *gin.Context) {
	id := c.GetInt64("id")
	blockID := c.GetInt64("block_id")
	block := new(model.Block)
	if err := c.ShouldBindJSON(block); err != nil {
		apiResponseError(c, http.StatusBadRequest, err)
		return
	}
	block.Replication = 1
	if err := s.proxy.UpdateINodeFileBlock(c.Request.Context(), id, blockID, block); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, block)

}

//deleteINodeFileBlock param id/block_id
func (s *apiServer) deleteINodeFileBlock(c *gin.Context) {
	id := c.GetInt64("id")
	blockID := c.GetInt64("block_id")
	// index, err := strconv.ParseInt(c.DefaultQuery("index", "0"), 10, 64)
	// if err != nil {
	// 	apiResponseError(c, http.StatusBadRequest, err)
	// 	return
	// }
	if err := s.proxy.DeleteINodeFileBlock(c.Request.Context(), id, blockID); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//getINodeDirectory param id
func (s *apiServer) getINodeDirectory(c *gin.Context) {
	id := c.GetInt64("id")
	m, err := s.proxy.GetINodeDirectory(c.Request.Context(), id)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("inode-directory id=%d not found", id))
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	//TODO get block meta
	apiResponseSuccess(c, m)

}

//getINodeDirectory param id
func (s *apiServer) getINodeDirectoryChildren(c *gin.Context) {
	id := c.GetInt64("id")
	_, simple := c.GetQuery("simple")
	m, err := s.proxy.GetINodeDirectoryChildren(c.Request.Context(), id, simple)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("inode-directory id=%d not found", id))
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	//TODO get block meta
	apiResponseSuccess(c, map[string]interface{}{"response": m})

}

//putINodeDirectory param id
func (s *apiServer) putINodeDirectory(c *gin.Context) {
	id := c.GetInt64("id")
	nm := new(pb.INodeMeta)
	err := c.ShouldBindJSON(nm)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error %s", err))
		return
	}
	nm.Id = proto.Int64(id)
	nm.Type = proto.Int32(inodeDirectoryType)
	err = s.proxy.PutINodeDirectory(c.Request.Context(), nm)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//deleteINodeDirectory param id
func (s *apiServer) deleteINodeDirectory(c *gin.Context) {
	id := c.GetInt64("id")
	//TODO
	if err := s.proxy.DeleteINodeDirectory(c.Request.Context(), id); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//getINodeDirectoryChild param id/name
func (s *apiServer) getINodeDirectoryChild(c *gin.Context) {
	name := c.Param("name")
	if len(name) == 0 {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("param name must not be empty"))
		return
	}
	id := c.GetInt64("id")
	// _, needMore := c.GetQuery("need_more")
	im, err := s.proxy.GetINodeDirectoryChild(c.Request.Context(), id, name, true)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, err)
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, pbINodeMetaToSimpleINode(im))
}

//putINodeDirectory param id/name
func (s *apiServer) putINodeDirectoryChild(c *gin.Context) {
	id := c.GetInt64("id")
	name := c.Param("name")
	if len(name) == 0 {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("param name must not be empty"))
		return
	}
	bm := new(pb.INodeMeta)
	err := c.ShouldBindJSON(bm)
	if err != nil {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("param post body err %s", err))
		return
	}
	// bm.Id = proto.Int64(c.GetInt64("id"))
	bm.Name = proto.String(name)
	bm.ParentId = proto.Int64(id)
	if err := s.proxy.PutINodeDirectoryChild(c.Request.Context(), id, bm); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//deleteINodeDirectory param id/name
func (s *apiServer) deleteINodeDirectoryChild(c *gin.Context) {
	name := c.Param("name")
	if len(name) == 0 {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("param name must not be empty"))
		return
	}
	id := c.GetInt64("id")
	if err := s.proxy.DeleteINodeDirectoryChild(c.Request.Context(), id, name); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

func (s *apiServer) updateINodeParent(c *gin.Context) {
	id := c.GetInt64("id")
	newParent := c.GetInt64("new_id")
	oldParent := c.GetInt64("old_id")
	if err := s.proxy.UpdateINodeParent(c.Request.Context(), id, newParent, oldParent); err != nil {
		if kv.ErrNotExist.Equal(err) {
			apiResponseError(c, http.StatusNotFound, err)
			return
		}
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
	return
}

func (s *apiServer) truncateINodeFile(c *gin.Context) {
	id := c.GetInt64("id")
	size := c.GetInt64("size")
	if err := s.proxy.TruncateINodeFile(c.Request.Context(), id, size); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

func (s *apiServer) updateINodeFile(c *gin.Context) {
	// id := c.GetInt("id")
	node := new(model.INodeFile)
	err := c.ShouldBindJSON(node)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, err)
		return
	}
	if err = s.proxy.UpdateINodeFile(c.Request.Context(), node); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

func (s *apiServer) updateINodeDirectory(c *gin.Context) {
	id := new(model.INodeDirectory)
	err := c.ShouldBindJSON(id)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, err)
		return
	}
	if err = s.proxy.UpdateINodeDirectory(c.Request.Context(), id); err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}
