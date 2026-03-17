package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/glu/video-real-time-ranking/core/pkg/constants"
	kafkaClient "github.com/glu/video-real-time-ranking/core/pkg/kafka"
	"github.com/heptiolabs/healthcheck"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

const (
	stackSize = 1 << 10 // 1 KB
)

func (s *server) connectKafkaBrokers(ctx context.Context) error {
	kafkaConn, err := kafkaClient.NewKafkaConn(ctx, s.cfg.Kafka)
	if err != nil {
		return errors.Wrap(err, "kafka.NewKafkaCon")
	}

	s.kafkaConn = kafkaConn

	brokers, err := kafkaConn.Brokers()
	if err != nil {
		return errors.Wrap(err, "kafkaConn.Brokers")
	}

	s.log.Infof("kafka connected to brokers: %+v", brokers)

	return nil
}

func (s *server) getConsumerGroupTopics() []string {
	return []string{
		s.cfg.KafkaTopics.VideoCreated.TopicName,
		s.cfg.KafkaTopics.VideoUpdated.TopicName,
		s.cfg.KafkaTopics.VideoDeleted.TopicName,
	}
}

func (s *server) runHealthCheck(ctx context.Context) {
	health := healthcheck.NewHandler()

	health.AddLivenessCheck(s.cfg.ServiceName, healthcheck.AsyncWithContext(ctx, func() error {
		return nil
	}, time.Duration(s.cfg.Probes.CheckIntervalSeconds)*time.Second))

	health.AddReadinessCheck(constants.Redis, healthcheck.AsyncWithContext(ctx, func() error {
		return s.redisClient.Ping(ctx).Err()
	}, time.Duration(s.cfg.Probes.CheckIntervalSeconds)*time.Second))

	health.AddReadinessCheck(constants.MongoDB, healthcheck.AsyncWithContext(ctx, func() error {
		return s.mongoClient.Ping(ctx, nil)
	}, time.Duration(s.cfg.Probes.CheckIntervalSeconds)*time.Second))

	health.AddReadinessCheck(constants.Kafka, healthcheck.AsyncWithContext(ctx, func() error {
		_, err := s.kafkaConn.Brokers()
		if err != nil {
			return err
		}
		return nil
	}, time.Duration(s.cfg.Probes.CheckIntervalSeconds)*time.Second))

	go func() {
		s.log.Infof("Reader microservice Kubernetes probes listening on port: %s", s.cfg.Probes.Port)
		if err := http.ListenAndServe(s.cfg.Probes.Port, health); err != nil {
			s.log.WarnMsg("ListenAndServe", err)
		}
	}()
}

func (s *server) runMetrics(cancel context.CancelFunc) {
	metricsServer := &fasthttp.Server{
		Handler: s.metricsHandler,
	}

	go func() {
		addr := fmt.Sprintf(":%s", s.cfg.Probes.PrometheusPort)
		if err := metricsServer.ListenAndServe(addr); err != nil {
			s.log.Printf("metricsServer.ListenAndServe: %v", err)
			cancel()
		}
	}()

	s.log.Printf("Metrics server is running on port: %s", s.cfg.Probes.PrometheusPort)
}

func (s *server) metricsHandler(ctx *fasthttp.RequestCtx) {
	// Your middleware/recovery logic here
	// Note: FastHTTP doesn't have a built-in middleware system like Echo
	// You need to implement the middleware logic manually if needed

	// Serve Prometheus metrics using the fasthttpadaptor and promhttp packages
	if string(ctx.Path()) == s.cfg.Probes.PrometheusPath {
		fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
	}
}
