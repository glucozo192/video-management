package kafka

import (
	"context"
	"strconv"
	"sync"

	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	kafkaMessages "github.com/glu/video-real-time-ranking/core/proto/kafka"
	"github.com/glu/video-real-time-ranking/reader_service/config"
	"github.com/glu/video-real-time-ranking/reader_service/internal/domain/usecase"
	"github.com/glu/video-real-time-ranking/reader_service/internal/metrics"
	"github.com/go-playground/validator"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	PoolSize = 30
)

type readerMessageProcessor struct {
	log          logger.Logger
	cfg          *config.Config
	v            *validator.Validate
	videoUsecase usecase.IVideoUsecase
	metrics      *metrics.ReaderServiceMetrics
}

func NewReaderMessageProcessor(
	log logger.Logger,
	cfg *config.Config,
	v *validator.Validate,
	videoUsecase usecase.IVideoUsecase,
	metrics *metrics.ReaderServiceMetrics,
) *readerMessageProcessor {
	return &readerMessageProcessor{
		log:          log,
		cfg:          cfg,
		v:            v,
		videoUsecase: videoUsecase,
		metrics:      metrics,
	}
}

func (s *readerMessageProcessor) ProcessMessages(ctx context.Context, r *kafka.Reader, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		m, err := r.FetchMessage(ctx)
		if err != nil {
			s.log.Warnf("workerID: %v, err: %v", workerID, err)
			continue
		}

		s.logProcessMessage(m, workerID)

		switch m.Topic {
		case s.cfg.KafkaTopics.VideoCreated.TopicName:
			s.processVideoCreated(ctx, r, m)
		case s.cfg.KafkaTopics.VideoUpdated.TopicName:
			s.processVideoUpdated(ctx, r, m)
		case s.cfg.KafkaTopics.VideoDeleted.TopicName:
			s.processVideoDeleted(ctx, r, m)
		}
	}
}

// processVideoCreated: cache miss is handled naturally by GetVideoById — no action needed here.
func (s *readerMessageProcessor) processVideoCreated(ctx context.Context, r *kafka.Reader, m kafka.Message) {
	s.metrics.SuccessKafkaMessages.Inc()
	if err := r.CommitMessages(ctx, m); err != nil {
		s.log.WarnMsg("commitMessage VideoCreated", err)
	}
}

// processVideoUpdated: invalidate Redis cache so next read fetches fresh data from DB.
func (s *readerMessageProcessor) processVideoUpdated(ctx context.Context, r *kafka.Reader, m kafka.Message) {
	var msg kafkaMessages.VideoUpdate
	if err := proto.Unmarshal(m.Value, &msg); err != nil {
		s.log.WarnMsg("proto.Unmarshal VideoUpdate", err)
		s.metrics.ErrorKafkaMessages.Inc()
		if err := r.CommitMessages(ctx, m); err != nil {
			s.log.WarnMsg("commitMessage", err)
		}
		return
	}

	s.videoUsecase.InvalidateCache(ctx, strconv.Itoa(int(msg.GetVideoID())))
	s.metrics.SuccessKafkaMessages.Inc()
	if err := r.CommitMessages(ctx, m); err != nil {
		s.log.WarnMsg("commitMessage VideoUpdated", err)
	}
}

// processVideoDeleted: invalidate Redis cache for the deleted video.
func (s *readerMessageProcessor) processVideoDeleted(ctx context.Context, r *kafka.Reader, m kafka.Message) {
	var msg kafkaMessages.VideoDelete
	if err := proto.Unmarshal(m.Value, &msg); err != nil {
		s.log.WarnMsg("proto.Unmarshal VideoDelete", err)
		s.metrics.ErrorKafkaMessages.Inc()
		if err := r.CommitMessages(ctx, m); err != nil {
			s.log.WarnMsg("commitMessage", err)
		}
		return
	}

	s.videoUsecase.InvalidateCache(ctx, strconv.Itoa(int(msg.GetVideoID())))
	s.metrics.SuccessKafkaMessages.Inc()
	if err := r.CommitMessages(ctx, m); err != nil {
		s.log.WarnMsg("commitMessage VideoDeleted", err)
	}
}
