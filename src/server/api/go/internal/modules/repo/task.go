package repo

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/memodb-io/Acontext/internal/modules/model"
	"gorm.io/gorm"
)

type TaskRepo interface {
	ListBySessionWithCursor(ctx context.Context, sessionID uuid.UUID, afterCreatedAt time.Time, afterID uuid.UUID, limit int, timeDesc bool) ([]model.Task, error)
	HasSuccessTask(ctx context.Context, sessionID uuid.UUID) (bool, error)
}

type taskRepo struct{ db *gorm.DB }

func NewTaskRepo(db *gorm.DB) TaskRepo {
	return &taskRepo{db: db}
}

func (r *taskRepo) ListBySessionWithCursor(ctx context.Context, sessionID uuid.UUID, afterCreatedAt time.Time, afterID uuid.UUID, limit int, timeDesc bool) ([]model.Task, error) {
	q := r.db.WithContext(ctx).Where("session_id = ? AND is_planning = false", sessionID)

	// Apply cursor-based pagination filter if cursor is provided
	if !afterCreatedAt.IsZero() && afterID != uuid.Nil {
		// Determine comparison operator based on sort direction
		comparisonOp := ">"
		if timeDesc {
			comparisonOp = "<"
		}
		q = q.Where(
			"(created_at "+comparisonOp+" ?) OR (created_at = ? AND id "+comparisonOp+" ?)",
			afterCreatedAt, afterCreatedAt, afterID,
		)
	}

	// Apply ordering based on sort direction
	orderBy := "created_at ASC, id ASC"
	if timeDesc {
		orderBy = "created_at DESC, id DESC"
	}

	var items []model.Task
	return items, q.Order(orderBy).Limit(limit).Find(&items).Error
}

func (r *taskRepo) HasSuccessTask(ctx context.Context, sessionID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.WithContext(ctx).Raw(
		"SELECT EXISTS(SELECT 1 FROM tasks WHERE session_id = ? AND status = 'success')",
		sessionID,
	).Scan(&exists).Error
	return exists, err
}
