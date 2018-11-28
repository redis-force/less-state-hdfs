package app

import (
	"github.com/redis-force/less-state-hdfs/pkg/proxy"
	"github.com/redis-force/less-state-hdfs/pkg/proxy/config"
	"go.uber.org/zap"
)

const (
	defaultHTTPServerHostPort = ":8080"
	defaultKVPdaddress        = "127.0.0.1:2379"
)

type Builder struct {
	Proxy config.Config `yaml:"proxy"`
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) BuildProxy(l *zap.Logger) (*proxy.Proxy, error) {
	b.Proxy.Logger = l
	return proxy.New(&b.Proxy)
}
