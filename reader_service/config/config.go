package config

import (
	"flag"
	"fmt"
	"os"

	"github.com/glu/video-real-time-ranking/core/pkg/constants"
	kafkaClient "github.com/glu/video-real-time-ranking/core/pkg/kafka"
	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/core/pkg/mongodb"
	"github.com/glu/video-real-time-ranking/core/pkg/probes"
	"github.com/glu/video-real-time-ranking/core/pkg/redis"
	"github.com/glu/video-real-time-ranking/core/pkg/tracing"
	"github.com/glu/video-real-time-ranking/core/pkg/utils"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "config", "", "Reader microservice config path")
}

type Config struct {
	ServiceName      string              `mapstructure:"serviceName"`
	Logger           *logger.Config      `mapstructure:"logger"`
	KafkaTopics      KafkaTopics         `mapstructure:"kafkaTopics"`
	GRPC             GRPC                `mapstructure:"grpc"`
	Http             Http                `mapstructure:"http"`
	Postgresql       *utils.Config       `mapstructure:"postgres"`
	Kafka            *kafkaClient.Config `mapstructure:"kafka"`
	Mongo            *mongodb.Config     `mapstructure:"mongo"`
	Redis            *redis.Config       `mapstructure:"redis"`
	MongoCollections MongoCollections    `mapstructure:"mongoCollections"`
	Probes           probes.Config       `mapstructure:"probes"`
	ServiceSettings  ServiceSettings     `mapstructure:"serviceSettings"`
	Jaeger           *tracing.Config     `mapstructure:"jaeger"`
}

type Http struct {
	Port                string   `mapstructure:"port"`
	Development         bool     `mapstructure:"development"`
	BasePath            string   `mapstructure:"basePath"`
	ProductsPath        string   `mapstructure:"productsPath"`
	DebugHeaders        bool     `mapstructure:"debugHeaders"`
	HttpClientDebug     bool     `mapstructure:"httpClientDebug"`
	DebugErrorsResponse bool     `mapstructure:"debugErrorsResponse"`
	IgnoreLogUrls       []string `mapstructure:"ignoreLogUrls"`
}

type GRPC struct {
	Port        string `mapstructure:"port"`
	Development bool   `mapstructure:"development"`
}

type MongoCollections struct {
	Products        string `mapstructure:"products"`
	ActivityHistory string `mapstructure:"activityHistory"`
}

type KafkaTopics struct {
	VideoCreated kafkaClient.TopicConfig `mapstructure:"videoCreated"`
	VideoUpdated kafkaClient.TopicConfig `mapstructure:"videoUpdated"`
	VideoDeleted kafkaClient.TopicConfig `mapstructure:"videoDeleted"`
}

type ServiceSettings struct {
	RedisVideoPrefixKey    string `mapstructure:"redisVideoPrefixKey"`
	RedisCommentPrefixKey  string `mapstructure:"redisCommentPrefixKey"`
	RedisObjectPrefixKey   string `mapstructure:"redisObjectPrefixKey"`
	RedisReactionPrefixKey string `mapstructure:"redisReactionPrefixKey"`
	RedisViewerPrefixKey   string `mapstructure:"redisViewerPrefixKey"`
}

func InitConfig() (*Config, error) {
	if configPath == "" {
		configPathFromEnv := os.Getenv(constants.ConfigPath)
		if configPathFromEnv != "" {
			configPath = configPathFromEnv
		} else {
			getwd, err := os.Getwd()
			if err != nil {
				return nil, errors.Wrap(err, "os.Getwd")
			}
			configPath = fmt.Sprintf("%s/reader_service/config/config.yaml", getwd)
		}
	}

	cfg := &Config{}

	viper.SetConfigType(constants.Yaml)
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "viper.ReadInConfig")
	}

	if err := viper.Unmarshal(cfg); err != nil {
		return nil, errors.Wrap(err, "viper.Unmarshal")
	}

	grpcPort := os.Getenv(constants.GrpcPort)
	if grpcPort != "" {
		cfg.GRPC.Port = grpcPort
	}
	postgresHost := os.Getenv(constants.PostgresqlHost)
	if postgresHost != "" {
		cfg.Postgresql.Host = postgresHost
	}
	postgresPort := os.Getenv(constants.PostgresqlPort)
	if postgresPort != "" {
		cfg.Postgresql.Port = postgresPort
	}
	mongoURI := os.Getenv(constants.MongoDbURI)
	if mongoURI != "" {
		//cfg.Mongo.URI = "mongodb://host.docker.internal:27017"
		cfg.Mongo.URI = mongoURI
	}
	redisAddr := os.Getenv(constants.RedisAddr)
	if redisAddr != "" {
		cfg.Redis.Addr = redisAddr
	}
	//jaegerAddr := os.Getenv("JAEGER_HOST")
	//if jaegerAddr != "" {
	//	cfg.Jaeger.HostPort = jaegerAddr
	//}
	//kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	//if kafkaBrokers != "" {
	//	cfg.Kafka.Brokers = []string{"host.docker.internal:9092"}
	//}
	kafkaBrokers := os.Getenv(constants.KafkaBrokers)
	if kafkaBrokers != "" {
		cfg.Kafka.Brokers = []string{kafkaBrokers}
	}
	jaegerAddr := os.Getenv(constants.JaegerHostPort)
	if jaegerAddr != "" {
		cfg.Jaeger.HostPort = jaegerAddr
	}

	return cfg, nil
}
