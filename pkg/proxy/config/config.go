package config

import "go.uber.org/zap"

type Config struct {
	KVPDAddress string `yaml:"pdAddress"`
	HostPort    string `yaml:"hostPort"`
	Logger      *zap.Logger
}
