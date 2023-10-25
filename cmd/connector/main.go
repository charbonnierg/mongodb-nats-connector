package main

import (
	"log"
	"os"

	"github.com/damianiandrea/mongodb-nats-connector/internal/config"
	"github.com/damianiandrea/mongodb-nats-connector/pkg/connector"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const defaultConfigFileName = "connector.yaml"

func main() {
	configFileName := getEnvOrDefault("CONFIG_FILE", defaultConfigFileName)
	cfg, err := config.Load(configFileName)
	if err != nil {
		log.Fatalf("error while loading config: %v", err)
	}

	logLevel := getEnvOrDefault("LOG_LEVEL", cfg.Connector.Log.Level)
	logger := getLogger(logLevel)
	opts := []connector.Option{
		connector.WithLogger(logger),
		connector.WithMongoUri(getEnvOrDefault("MONGO_URI", cfg.Connector.Mongo.Uri)),
		connector.WithNatsUrl(getEnvOrDefault("NATS_URL", cfg.Connector.Nats.Url)),
		connector.WithNatsOptions(getNatsOptions(cfg.Connector.Nats)...),
		connector.WithServerAddr(getEnvOrDefault("SERVER_ADDR", cfg.Connector.Server.Addr)),
	}
	for _, coll := range cfg.Connector.Collections {
		collOpts := []connector.CollectionOption{
			connector.WithTokensDbName(coll.TokensDbName),
			connector.WithTokensCollName(coll.TokensCollName),
			connector.WithStreamName(coll.StreamName),
		}
		if coll.ChangeStreamPreAndPostImages != nil && *coll.ChangeStreamPreAndPostImages {
			collOpts = append(collOpts, connector.WithChangeStreamPreAndPostImages())
		}
		if coll.TokensCollCapped != nil && coll.TokensCollSizeInBytes != nil && *coll.TokensCollCapped {
			collOpts = append(collOpts, connector.WithTokensCollCapped(*coll.TokensCollSizeInBytes))
		}
		opt := connector.WithCollection(coll.DbName, coll.CollName, collOpts...)
		opts = append(opts, opt)
	}

	if conn, err := connector.New(opts...); err != nil {
		log.Fatalf("could not create connector: %v", err)
	} else {
		log.Fatalf("exiting: %v", conn.Run())
	}
}

func getEnvOrDefault(env, def string) string {
	if val, found := os.LookupEnv(env); found {
		return val
	}
	return def
}

func getMongoOptions(cfg config.Mongo) *options.ClientOptions {
	var opts *options.ClientOptions
	if cfg.AuthMechanism != "" {
		opts := options.Client()
		opts.Auth = &options.Credential{
			AuthMechanism: cfg.AuthMechanism,
		}
	}
	return opts
}

func getNatsOptions(cfg config.Nats) []nats.Option {
	opts := []nats.Option{}
	if cfg.Credentials != "" {
		opts = append(opts, nats.UserCredentials(cfg.Credentials))
	}
	if cfg.User != "" && cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	if cfg.Token != "" {
		opts = append(opts, nats.Token(cfg.Token))
	}
	return opts
}

func getLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zap.DebugLevel
	case "info":
		zapLevel = zap.InfoLevel
	case "warn":
		zapLevel = zap.WarnLevel
	case "error":
		zapLevel = zap.ErrorLevel
	case "fatal":
		zapLevel = zap.FatalLevel
	case "panic":
		zapLevel = zap.PanicLevel
	default:
		zapLevel = zap.InfoLevel
	}
	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zapLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := cfg.Build()
	if err != nil {
		log.Fatalf("could not create logger: %v", err)
	}
	return logger
}
