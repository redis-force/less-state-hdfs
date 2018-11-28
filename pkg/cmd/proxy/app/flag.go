package app

import (
	"flag"

	"github.com/spf13/viper"
)

const (
	httpServerHostPort = "proxy.server.host-port"
	tikvPDAddress      = "proxy.tivk.pd-address"
)

func AddFlags(flag *flag.FlagSet) {
	flag.String(
		httpServerHostPort,
		defaultHTTPServerHostPort,
		"host:port of the http server")
	flag.String(
		tikvPDAddress,
		defaultKVPdaddress,
		"address of tikv pd address")

}

// InitFromViper initializes Builder with properties retrieved from Viper.
func (b *Builder) InitFromViper(v *viper.Viper) *Builder {
	b.Proxy.HostPort = v.GetString(httpServerHostPort)
	b.Proxy.KVPDAddress = v.GetString(tikvPDAddress)
	return b
}
