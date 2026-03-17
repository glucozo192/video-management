package product

import (
	"context"
	"strconv"

	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/ent"
	"github.com/glu/video-real-time-ranking/reader_service/config"
	models2 "github.com/glu/video-real-time-ranking/reader_service/internal/domain/models"
	"github.com/glu/video-real-time-ranking/reader_service/internal/domain/repositories"
	"github.com/glu/video-real-time-ranking/reader_service/internal/domain/usecase"
	"github.com/opentracing/opentracing-go"
)

type videoUsecase struct {
	log       logger.Logger
	cfg       *config.Config
	redisRepo repositories.IVideoCacheRepository
	entRepo   repositories.IVideoRepositoryReader
}

func NewVideoUsecase(
	log logger.Logger,
	cfg *config.Config,
	redisRepo repositories.IVideoCacheRepository,
	entRepo repositories.IVideoRepositoryReader,
) usecase.IVideoUsecase {
	return &videoUsecase{
		log:       log,
		cfg:       cfg,
		redisRepo: redisRepo,
		entRepo:   entRepo,
	}
}

func (v *videoUsecase) GetVideoById(ctx context.Context, id uint) (*ent.Videos, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "getVideoByIdHandler.Handle")
	defer span.Finish()

	if video, err := v.redisRepo.GetVideo(ctx, strconv.Itoa(int(id))); err == nil && video != nil {
		return video, nil
	}

	videoResp, err := v.entRepo.GetVideoById(ctx, id)
	if err != nil {
		return nil, err
	}

	v.redisRepo.PutVideo(ctx, strconv.Itoa(int(id)), videoResp)
	return videoResp, nil
}

// InvalidateCache removes a video from Redis cache by key so the next read fetches fresh data from DB.
func (v *videoUsecase) InvalidateCache(ctx context.Context, key string) {
	v.redisRepo.DelVideo(ctx, key)
	v.log.Debugf("Cache invalidated for key: %s", key)
}

func (v *videoUsecase) SearchVideo(ctx context.Context, query models2.SearchVideoRequest, key string) (*models2.VideosListResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "videoUsecase.SearchVideo")
	defer span.Finish()

	// 1. Try Redis cache first (cache-aside pattern)
	if key != "" {
		if videoCacheResp, err := v.redisRepo.GetVideosByKey(ctx, key); err == nil && videoCacheResp.Videos != nil {
			v.log.Debugf("SearchVideo cache HIT key: %s", key)
			return &videoCacheResp, nil
		}
	}

	// 2. Cache miss → query DB
	videoResp, err := v.entRepo.SearchVideoByParams(ctx, query)
	if err != nil {
		return nil, err
	}

	// 3. Store in cache (non-blocking — cache failure must not block the response)
	if key != "" {
		if err = v.redisRepo.PutVideos(ctx, key, *videoResp); err != nil {
			v.log.WarnMsg("redisRepo.PutVideos", err)
		}
	}

	return videoResp, nil
}
