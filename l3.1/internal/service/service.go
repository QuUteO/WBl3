package service

import (
	model "DelayedNotifier/internal"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrUnsupportedChannel = errors.New("неподдерживаемый канал уведомления")
	ErrEmptyMessage       = errors.New("сообщение уведомления не может быть пустым")
	ErrScheduledInPast    = errors.New("нельзя запланировать отправку в прошлое")
	ErrScheduledTooFar    = errors.New("нельзя запланировать отправку более чем на 1 год вперед")
	ErrEmptyNotification  = errors.New("пустая структура уведомлений")
)

const (
	channelEmail    = "email"
	channelTelegram = "telegram"
	emptyString     = ""
)

type NotificationRepository interface {
	CreateNotification(ctx context.Context, notification *model.Notification) error
	GetNotification(ctx context.Context, notificationID uuid.UUID) (*model.Notification, error) // Тип изменен на полный
	DeleteNotification(ctx context.Context, notificationID uuid.UUID) error
}

type NotificationPublisher interface {
	Push(ctx context.Context, data *model.Notification, routingKey string) error
}

type Service struct {
	repository NotificationRepository
	publisher  NotificationPublisher
}

func New(repository NotificationRepository, publisher NotificationPublisher) *Service {
	return &Service{
		repository: repository,
		publisher:  publisher,
	}
}

func (s *Service) CreateNotification(ctx context.Context, req *model.CreateNotification) (*model.Notification, error) {
	if req.Channel != channelEmail && req.Channel != channelTelegram {
		return nil, ErrUnsupportedChannel
	}

	if req.Message == emptyString {
		return nil, ErrEmptyMessage
	}

	now := time.Now()

	if req.ScheduledAt.Before(now.Add(-5 * time.Second)) {
		return nil, ErrScheduledInPast
	}

	if req.ScheduledAt.After(now.AddDate(1, 0, 0)) {
		return nil, ErrScheduledTooFar
	}

	notification := model.Notification{
		ID:          uuid.New(),
		Recipient:   req.Recipient,
		Channel:     req.Channel,
		Message:     req.Message,
		ScheduledAt: req.ScheduledAt,
		Status:      "scheduled",
		RetryCount:  0,
		MaxRetries:  5,
		SentAt:      nil,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	err := s.repository.CreateNotification(ctx, &notification)
	if err != nil {
		return nil, err
	}

	var routingKey string
	switch notification.Channel {
	case channelTelegram:
		routingKey = "notification.telegram"
	case channelEmail:
		routingKey = "notification.email"
	default:
		routingKey = "notification"
	}

	err = s.publisher.Push(ctx, &notification, routingKey)
	if err != nil {
		return nil, err
	}

	return &notification, nil
}

func (s *Service) GetNotification(ctx context.Context, id uuid.UUID) (*model.Notification, error) {
	notification, err := s.repository.GetNotification(ctx, id)
	if err != nil {
		return nil, err
	}

	if notification == nil {
		return nil, ErrEmptyNotification
	}

	return notification, nil
}

func (s *Service) DeleteNotification(ctx context.Context, id uuid.UUID) error {
	return s.repository.DeleteNotification(ctx, id)
}

func (s *Service) ProcessNotification(ctx context.Context, n *model.Notification) error {
	// Сюда переедет логика ретраев и отправки во внешнее API (Telegram/Email),
	// о которой ты размышлял. Пока оставляем пустую заглушку для компиляции.
	return nil
}
