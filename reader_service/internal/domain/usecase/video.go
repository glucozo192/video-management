package usecase

import (
	"context"

	"github.com/glu/video-real-time-ranking/ent"
	"github.com/glu/video-real-time-ranking/reader_service/internal/domain/models"
)

type IVideoUsecase interface {
	GetVideoById(ctx context.Context, id uint) (*ent.Videos, error)
	SearchVideo(ctx context.Context, query models.SearchVideoRequest, key string) (*models.VideosListResponse, error)
	InvalidateCache(ctx context.Context, key string)
}
