package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis-force/less-state-hdfs/pkg/cmd/flags"
	"github.com/redis-force/less-state-hdfs/pkg/cmd/proxy/app"
	"github.com/redis-force/less-state-hdfs/pkg/config"
	"github.com/redis-force/less-state-hdfs/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	v := viper.New()
	var command = &cobra.Command{
		Use:   "hdfs-ns-proxy",
		Short: "hdfs ns proxy is a daemon program which serve as a proxy to tikv.",
		Long:  `hdfs ns proxy is a daemon program serve as a http proxy to tikv, it provides inodes api to hdfs namenode`,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := flags.TryLoadConfigFile(v)
			if err != nil {
				return err
			}
			builder := app.NewBuilder()
			sFlags := new(flags.SharedFlags).InitFromViper(v)
			logger, err := sFlags.NewLogger(zap.NewProductionConfig())
			if err != nil {
				return err
			}

			builder.InitFromViper(v)
			proxy, err := builder.BuildProxy(logger)
			if err != nil {
				logger.Fatal("Build proxy error", zap.Error(err))
				return err
			}
			stop := make(chan os.Signal, 1)
			signal.Notify(stop, os.Interrupt, os.Kill, syscall.SIGTERM)
			logger.Info("Starting server")
			err = proxy.Start()
			if err != nil {
				logger.Fatal("proxy start error", zap.Error(err))
				return err
			}
			<-stop
			proxy.Close()
			return nil
		},
	}

	command.AddCommand(version.Command())

	config.AddFlags(
		v,
		command,
		flags.AddConfigFileFlag,
		flags.AddFlags,
		app.AddFlags,
	)

	if err := command.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
