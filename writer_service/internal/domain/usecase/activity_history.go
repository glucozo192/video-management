package usecase

import "context"

// IActivityHistoryUsecase defines the contract for logging activity history events.
type IActivityHistoryUsecase interface {
	Log(ctx context.Context, action, entityType string, entityID uint, payload interface{})
}
