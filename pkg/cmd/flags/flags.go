package flags

import (
	"flag"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logLevel   = "log-level"
	logPath    = "log-path"
	configFile = "config-file"
)

// AddConfigFileFlag adds flags for ExternalConfFlags
func AddConfigFileFlag(flagSet *flag.FlagSet) {
	flagSet.String(configFile, "", "Configuration file in JSON, TOML, YAML, HCL, or Java properties formats (default none). See spf13/viper for precedence.")
}

// TryLoadConfigFile initializes viper with config file specified as flag
func TryLoadConfigFile(v *viper.Viper) error {
	if file := v.GetString(configFile); file != "" {
		v.SetConfigFile(file)
		err := v.ReadInConfig()
		if err != nil {
			return errors.Wrapf(err, "Error loading config file %s", file)
		}
	}
	return nil
}

// SharedFlags holds flags configuration
type SharedFlags struct {
	// Logging holds logging configuration
	Logging logging
}

type logging struct {
	Level string
	Path  []string
}

// AddFlags adds flags for SharedFlags
func AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(logLevel, "info", "Minimal allowed log Level. For more levels see https://github.com/uber-go/zap")
	flagSet.String(logPath, "stderr", "Server log path")
}

// InitFromViper initializes SharedFlags with properties from viper
func (flags *SharedFlags) InitFromViper(v *viper.Viper) *SharedFlags {
	flags.Logging.Level = v.GetString(logLevel)
	flags.Logging.Path = strings.Split(v.GetString(logPath), ",")
	return flags
}

// NewLogger returns logger based on configuration in SharedFlags
func (flags *SharedFlags) NewLogger(conf zap.Config, options ...zap.Option) (*zap.Logger, error) {
	var level zapcore.Level
	err := (&level).UnmarshalText([]byte(flags.Logging.Level))
	if err != nil {
		return nil, err
	}
	if len(flags.Logging.Path) != 0 {
		conf.OutputPaths = flags.Logging.Path
		conf.ErrorOutputPaths = flags.Logging.Path
	}

	conf.Level = zap.NewAtomicLevelAt(level)
	return conf.Build(options...)
}
