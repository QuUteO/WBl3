package service

import (
	model "DelayedNotifier/internal"
	"context"

	"github.com/google/uuid"
)

type NotificationRepository interface {
	CreateNotification(ctx context.Context, notification *model.Notification) error
	GetNotification(ctx context.Context, notificationID uuid.UUID) (*model.CreateNotification, error)
	DeleteNotification(ctx context.Context, notificationID uuid.UUID) error
}

type Service struct {
	repository NotificationRepository
}

func New(repository NotificationRepository) *Service {
	return &Service{repository: repository}
}

func (s *Service) CreateNotification(ctx context.Context, notification *model.CreateNotification) error {

	return nil
}
