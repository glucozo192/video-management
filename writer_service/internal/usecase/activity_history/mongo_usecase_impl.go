package usecase

import (
	"context"
	"time"

	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/models"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/repositories"
	domainUsecase "github.com/glu/video-real-time-ranking/writer_service/internal/domain/usecase"
)

type activityHistoryUsecase struct {
	log  logger.Logger
	repo repositories.IActivityHistoryRepositoryReader
}

// NewActivityHistoryUsecase creates a new ActivityHistory usecase.
func NewActivityHistoryUsecase(log logger.Logger, repo repositories.IActivityHistoryRepositoryReader) domainUsecase.IActivityHistoryUsecase {
	return &activityHistoryUsecase{log: log, repo: repo}
}

// Log records an activity history entry asynchronously.
// action: e.g. "VIDEO_CREATED", "VIDEO_UPDATED", "VIDEO_DELETED"
// user: the user performing the action (from auth context or system)
// videoID: the ID of the affected video
// note: optional extra description
func (u *activityHistoryUsecase) Log(ctx context.Context, action, entityType string, entityID uint, payload interface{}) {
	_, err := u.repo.CreateActivityHistory(ctx, &models.ActivityHistory{
		VideoID:   entityID,
		Actions:   action,
		User:      entityType,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	if err != nil {
		u.log.WarnMsg("activityHistory.Log: CreateActivityHistory", err)
	}
}

