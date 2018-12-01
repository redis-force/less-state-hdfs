package proxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/redis-force/less-state-hdfs/pkg/proxy/config"
	"go.uber.org/zap"
)

var (
	ErrServerClosed = errors.New("Error server closed.")
)

func New(config *config.Config) (*Proxy, error) {
	s := &Proxy{}
	s.config = config
	s.exitChan = make(chan struct{})
	s.logger = config.Logger

	var err error
	driver := tikv.Driver{}
	s.store, err = driver.Open(fmt.Sprintf("tikv://%s", config.KVPDAddress))
	if err != nil {
		s.logger.Fatal("open tivk storage error", zap.Error(err))
		return nil, errors.Trace(err)
	}
	s.oracle = s.store.GetOracle()
	// s.client = s.store.GetClient()
	return s, nil
}

type Proxy struct {
	config *config.Config

	store  kv.Storage
	oracle oracle.Oracle
	// client kv.Client

	logger    *zap.Logger
	apiServer *http.Server
	mu        sync.Mutex

	closed   bool
	exitChan chan struct{}
}

func (p *Proxy) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrServerClosed
	}
	l, err := net.Listen("tcp4", p.config.HostPort)
	if err != nil {
		return errors.Trace(err)
	}
	api := newAPIServer(p)
	p.apiServer = &http.Server{Handler: api}
	go func() {
		p.logger.Info("api server start listening", zap.String("hostPort", p.config.HostPort))
		p.apiServer.Serve(l)
	}()
	return nil
}

func (p *Proxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.oracle.Close()
	p.closed = true
	close(p.exitChan)
	p.logger.Warn("api server start shutdown.")
	p.apiServer.Shutdown(context.Background())
	p.logger.Warn("api server gracefully shutdown.")
	return nil
}

func (p *Proxy) IsClosed() (closed bool) {
	p.mu.Lock()
	closed = p.closed
	p.mu.Unlock()
	return
}
