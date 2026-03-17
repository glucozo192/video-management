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
	"github.com/glu/video-real-time-ranking/core/pkg/tracing"
	"github.com/glu/video-real-time-ranking/core/pkg/utils"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "config", "", "Writer microservice microservice config path")
}

type Config struct {
	App              App                 `mapstructure:"app"`
	ServiceName      string              `mapstructure:"serviceName"`
	Logger           *logger.Config      `mapstructure:"logger"`
	KafkaTopics      KafkaTopics         `mapstructure:"kafkaTopics"`
	GRPC             GRPC                `mapstructure:"grpc"`
	Http             Http                `mapstructure:"http"`
	Postgresql       *utils.Config       `mapstructure:"postgres"`
	Kafka            *kafkaClient.Config `mapstructure:"kafka"`
	Mongo            *mongodb.Config     `mapstructure:"mongo"`
	MongoCollections MongoCollections    `mapstructure:"mongoCollections"`
	ServiceSettings  ServiceSettings     `mapstructure:"serviceSettings"`
	Probes           probes.Config       `mapstructure:"probes"`
	Jaeger           *tracing.Config     `mapstructure:"jaeger"`
	ExternalService  ExternalService     `mapstructure:"externalService"`
	Media            mediaStruct         `mapstructure:"media"`
	Storage          storageStruct       `mapstructure:"storage"`
}

type IConfigService interface {
	GetMediaCdn() string
	GetMediaS3() string
	GetMediaDisplay() string
	GetPathUpload() string
	GetPlatformService() string
	GetTokenToServer() string
}

type conf struct {
	App          App
	MediaCdn     string
	MediaS3      string
	MediaDisplay string

	ExternalService ExternalService
}

var cfg_inst conf

func GetInstance() conf {
	return cfg_inst
}

type mediaStruct struct {
	Cdn     string `mapstructure:"cdn"`
	S3      string `mapstructure:"s3"`
	Display string `mapstructure:"display"`
}

type storageStruct struct {
	Disk string `mapstructure:"disk"`
}

type ExternalService struct {
	Develop string `mapstructure:"develop"`
	Media   string `mapstructure:"media"`
	Product string `mapstructure:"product"`

	App      string `mapstructure:"app"`
	User     string `mapstructure:"user"`
	Award    string `mapstructure:"award"`
	Lesson   string `mapstructure:"lesson"`
	Story    string `mapstructure:"story"`
	Platform string `mapstructure:"platform"`

	Report    string `mapstructure:"report"`
	K5Path    string `mapstructure:"k5Path"`
	LocalPath string `mapstructure:"localPath"`

	Sentry string `mapstructure:"sentry"`
}

type App struct {
	Env           string `mapstructure:"env"`
	SecretKey     string `mapstructure:"secretKey"`
	TokenToServer string `mapstructure:"tokenToServer"`
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

type MongoCollections struct {
	Products        string `mapstructure:"products"`
	ActivityHistory string `mapstructure:"activityHistory"`
}

type GRPC struct {
	Port              string `mapstructure:"port"`
	ReaderServicePort string `mapstructure:"readerServicePort"`
	Development       bool   `mapstructure:"development"`
}

type ServiceSettings struct {
	RedisVideoPrefixKey    string `mapstructure:"redisVideoPrefixKey"`
	RedisCommentPrefixKey  string `mapstructure:"redisCommentPrefixKey"`
	RedisObjectPrefixKey   string `mapstructure:"redisObjectPrefixKey"`
	RedisReactionPrefixKey string `mapstructure:"redisReactionPrefixKey"`
	RedisViewerPrefixKey   string `mapstructure:"redisViewerPrefixKey"`
}

type KafkaTopics struct {
	// video topics
	VideoCreate  kafkaClient.TopicConfig `mapstructure:"videoCreate"`
	VideoCreated kafkaClient.TopicConfig `mapstructure:"videoCreated"`
	VideoUpdate  kafkaClient.TopicConfig `mapstructure:"videoUpdate"`
	VideoUpdated kafkaClient.TopicConfig `mapstructure:"videoUpdated"`
	VideoDelete  kafkaClient.TopicConfig `mapstructure:"videoDelete"`
	VideoDeleted kafkaClient.TopicConfig `mapstructure:"videoDeleted"`
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
			configPath = fmt.Sprintf("%s/writer_service/config/config.yaml", getwd)
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
	jaegerAddr := os.Getenv(constants.JaegerHostPort)
	if jaegerAddr != "" {
		cfg.Jaeger.HostPort = jaegerAddr
	}
	kafkaBrokers := os.Getenv(constants.KafkaBrokers)
	if kafkaBrokers != "" {
		cfg.Kafka.Brokers = []string{kafkaBrokers}
	}

	cfg_inst = conf{
		App: App{
			Env:           cfg.App.Env,
			SecretKey:     cfg.App.SecretKey,
			TokenToServer: cfg.App.TokenToServer,
		},
		MediaCdn:        cfg.Media.Cdn,
		MediaS3:         cfg.Media.S3,
		MediaDisplay:    cfg.Media.Display,
		ExternalService: cfg.ExternalService,
	}
	return cfg, nil
}

func (c conf) GetPlatformService() string {
	return c.ExternalService.Platform
}

func (c conf) GetTokenToServer() string {
	return c.App.TokenToServer
}
