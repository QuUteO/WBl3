package service

import (
	model "DelayedNotifier/internal"
	"DelayedNotifier/internal/config"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/smtp"
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
	GetNotification(ctx context.Context, notificationID uuid.UUID) (*model.Notification, error)
	DeleteNotification(ctx context.Context, notificationID uuid.UUID) error

	UpdateStatus(ctx context.Context, id uuid.UUID, status string, at *time.Time, count int) error
	UpdateRetryInfo(ctx context.Context, id uuid.UUID, status string, count int, at time.Time) error
}

type NotificationPublisher interface {
	Push(ctx context.Context, data *model.Notification, routingKey string) error
}

type Service struct {
	repository NotificationRepository
	publisher  NotificationPublisher
	botToken   string
	smtp       *config.SMTP
}

func New(repository NotificationRepository, publisher NotificationPublisher, botToken string, smtp *config.SMTP) *Service {
	return &Service{
		repository: repository,
		publisher:  publisher,
		botToken:   botToken,
		smtp:       smtp,
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

	// Возвращаем динамический выбор routingKey в зависимости от канала
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
	err := s.SendToExternalAPI(ctx, n)

	now := time.Now()
	if err == nil {
		n.Status = "sent"
		n.SentAt = &now
		n.UpdatedAt = now

		return s.repository.UpdateStatus(ctx, n.ID, n.Status, n.SentAt, n.RetryCount)
	}

	n.RetryCount++
	n.UpdatedAt = now

	if n.RetryCount >= n.MaxRetries {
		n.Status = "failed"
		log.Printf("Уведомление %s превысило лимит попыток и помечено как failed", n.ID)
	} else {
		n.Status = "retry"
		n.ScheduledAt = now.Add(5 * time.Minute)
		log.Printf("Ошибка отправки уведомления %s (попытка %d/%d). Переносим на %v", n.ID, n.RetryCount, n.MaxRetries, n.ScheduledAt)
	}

	repoErr := s.repository.UpdateRetryInfo(ctx, n.ID, n.Status, n.RetryCount, n.ScheduledAt)
	if repoErr != nil {
		log.Printf("Критическая ошибка обновления статуса ретрая в БД: %v", repoErr)
	}

	return err
}

// Наполняем метод реальной отправкой
func (s *Service) SendToExternalAPI(ctx context.Context, n *model.Notification) error {
	switch n.Channel {
	case channelTelegram:
		return s.sendToTelegram(ctx, n)
	case channelEmail:
		return s.sendToEmail(ctx, n)
	default:
		return fmt.Errorf("неподдерживаемый канал отправки: %s", n.Channel)
	}
}

func (s *Service) sendToTelegram(ctx context.Context, n *model.Notification) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", s.botToken)

	payload := map[string]string{
		"chat_id": n.Recipient,
		"text":    n.Message,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга для telegram: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка отправки http-запроса в telegram: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram api вернул статус-код: %d", resp.StatusCode)
	}

	return nil
}

func (s *Service) sendToEmail(ctx context.Context, n *model.Notification) error {
	msg := []byte("Subject: Новое уведомление\n\n" + n.Message)
	auth := smtp.PlainAuth("", s.smtp.SenderEmail, s.smtp.Password, s.smtp.SMTPHost)

	err := smtp.SendMail(s.smtp.SMTPHost+":"+s.smtp.SMTPPort, auth, s.smtp.SenderEmail, []string{n.Recipient}, msg)
	if err != nil {
		return fmt.Errorf("ошибка отправки письма через smtp: %w", err)
	}

	return nil
}
