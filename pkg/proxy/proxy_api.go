package proxy

import (
	"fmt"
	"net/http"
	"strconv"

	"runtime"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tidb/kv"
	"github.com/redis-force/less-state-hdfs/pkg/model"
	pb "github.com/redis-force/less-state-hdfs/pkg/proto"
)

type apiServer struct {
	proxy *Proxy
}

func newAPIServer(proxy *Proxy) *gin.Engine {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
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
		c.Keys = make(map[string]interface{})
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
				if id <= 0 {
					apiResponseError(c, http.StatusBadRequest, fmt.Errorf("%s format error", p))
					return
				}
				c.Keys[p] = id
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
		block := api.Group("/block/meta")
		block.Use(intCheck("id"))
		{
			block.GET("/:id", server.getBlock)
			block.PUT("/:id", server.putBlock)
			block.DELETE("/:id", server.deleteBlock)
		}
		blockStorage := api.Group("/block/storage")
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
			file.DELETE("/:id", server.deleteINodeFile)

			file.GET("/:id/:block_id", intCheck("block_id"), server.getINodeFileBlock)
			file.PUT("/:id/:block_id", intCheck("block_id"), server.putINodeFileBlock)
			file.DELETE("/:id/:block_id", intCheck("block_id"), server.deleteINodeFileBlock)
		}
		direcotry := api.Group("/directory")
		direcotry.Use(intCheck("id"))
		{
			direcotry.GET("/:id", server.getINodeDirectory)
			direcotry.PUT("/:id", server.putINodeDirectory)
			direcotry.DELETE("/:id", server.deleteINodeDirectory)

			direcotry.GET("/:id/:name", server.getINodeDirectoryChild)
			direcotry.PUT("/:id/:name", server.putINodeDirectoryChild)
			direcotry.DELETE("/:id/:name", server.deleteINodeDirectoryChild)
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
	c.JSON(http.StatusOK, model.APIResponse{Code: 0, Error: "success", Response: resp})

}

func (s *apiServer) ts(c *gin.Context) {
	tm, err := s.proxy.oracle.GetTimestamp(c.Request.Context())
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, model.TS{Timestamp: tm})
}

func (s *apiServer) getBlock(c *gin.Context) {
	id := c.Keys["id"].(int64)
	bm, err := s.proxy.GetBlockMeta(c.Request.Context(), id)
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
	id := c.Keys["id"].(int64)
	bm := new(pb.BlockMeta)
	err := c.ShouldBindJSON(bm)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error %s", err))
		return
	}
	if bm.Generation <= 0 || bm.Replication <= 0 || bm.CollectionId <= 0 || bm.NumberBytes < 0 {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error, filed must not by none %v ", bm))
		return
	}
	bm.Id = id
	err = s.proxy.PutBlockMeta(c.Request.Context(), bm)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

func (s *apiServer) deleteBlock(c *gin.Context) {
	id := c.Keys["id"].(int64)
	err := s.proxy.DeleteBlockMeta(c.Request.Context(), id)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//getBlock param id
func (s *apiServer) getBlockStorage(c *gin.Context) {
	//TODO get block meta
	apiResponseSuccess(c, nil)
}

//putBlock param id/data_node_id/storage_id
func (s *apiServer) putBlockStorage(c *gin.Context) {
	//TODO get block meta
	apiResponseSuccess(c, nil)
}

//deleteBlock param id/data_node_id/storage_id
func (s *apiServer) deleteBlockStorage(c *gin.Context) {
	// _ =
	//TODO get block meta
	apiResponseSuccess(c, nil)
}

//getINodeFile param id
func (s *apiServer) getINodeFile(c *gin.Context) {
	id := c.Keys["id"].(int64)
	m, err := s.proxy.GetINodeFile(c.Request.Context(), id)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("inode-file id=%d not found", id))
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	//TODO get block meta
	apiResponseSuccess(c, m)

}

//putINodeFile param id
func (s *apiServer) putINodeFile(c *gin.Context) {
	id := c.Keys["id"].(int64)
	nm := new(pb.INodeMeta)
	err := c.ShouldBindJSON(nm)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error %s", err))
		return
	}
	nm.Id = id
	err = s.proxy.PutINodeFile(c.Request.Context(), nm)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)

}

//deleteINodeFile param id
func (s *apiServer) deleteINodeFile(c *gin.Context) {

}

//getINodeFileBlock param id/block_id
func (s *apiServer) getINodeFileBlock(c *gin.Context) {

}

//putINodeFileBlock param id/block_id
func (s *apiServer) putINodeFileBlock(c *gin.Context) {

}

//deleteINodeFileBlock param id/block_id
func (s *apiServer) deleteINodeFileBlock(c *gin.Context) {

}

//getINodeDirectory param id
func (s *apiServer) getINodeDirectory(c *gin.Context) {
	id := c.Keys["id"].(int64)
	m, err := s.proxy.GetINodeDirectory(c.Request.Context(), id)
	if kv.ErrNotExist.Equal(err) {
		apiResponseError(c, http.StatusNotFound, fmt.Errorf("inode-file id=%d not found", id))
		return
	}
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	//TODO get block meta
	apiResponseSuccess(c, m)

}

//putINodeDirectory param id
func (s *apiServer) putINodeDirectory(c *gin.Context) {
	id := c.Keys["id"].(int64)
	nm := new(pb.INodeMeta)
	err := c.ShouldBindJSON(nm)
	if err != nil {
		apiResponseError(c, http.StatusBadRequest, fmt.Errorf("parse put request error %s", err))
		return
	}
	nm.Id = id
	err = s.proxy.PutINodeDirectory(c.Request.Context(), nm)
	if err != nil {
		apiResponseError(c, http.StatusInternalServerError, err)
		return
	}
	apiResponseSuccess(c, nil)
}

//deleteINodeDirectory param id
func (s *apiServer) deleteINodeDirectory(c *gin.Context) {
}

//getINodeDirectory param id/name
func (s *apiServer) getINodeDirectoryChild(c *gin.Context) {

}

//putINodeDirectory param id/name
func (s *apiServer) putINodeDirectoryChild(c *gin.Context) {

}

//deleteINodeDirectory param id/name
func (s *apiServer) deleteINodeDirectoryChild(c *gin.Context) {
}
