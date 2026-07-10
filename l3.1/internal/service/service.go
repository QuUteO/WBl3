package service

import (
	model "DelayedNotifier/internal"
	"DelayedNotifier/internal/config"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
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
	ErrTooEarly           = errors.New("время отправки уведомления еще не наступило")
)

const (
	channelEmail    = "email"
	channelTelegram = "telegram"
	statusCancelled = "cancelled"
	statusFailed    = "failed"
	statusSent      = "sent"
	emptyString     = ""
)

type NotificationRepository interface {
	CreateNotification(ctx context.Context, notification *model.Notification) error
	GetNotification(ctx context.Context, notificationID uuid.UUID) (*model.Notification, error)
	DeleteNotification(ctx context.Context, notificationID uuid.UUID) error
	ListNotifications(ctx context.Context, limit int) ([]*model.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status string, at *time.Time, count int) error
	UpdateRetryInfo(ctx context.Context, id uuid.UUID, status string, count int, at time.Time) error
	FetchReadyNotifications(ctx context.Context, limit int) ([]*model.Notification, error)
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

	if notification.ScheduledAt.After(now.Add(10 * time.Second)) {
		return &notification, nil
	}

	routingKey := "notification-queue"

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

func (s *Service) ListNotifications(ctx context.Context, limit int) ([]*model.Notification, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	return s.repository.ListNotifications(ctx, limit)
}

func (s *Service) DeleteNotification(ctx context.Context, id uuid.UUID) error {
	return s.repository.DeleteNotification(ctx, id)
}

func (s *Service) ProcessNotification(ctx context.Context, n *model.Notification) error {
	current, err := s.repository.GetNotification(ctx, n.ID)
	if err != nil {
		return err
	}
	if current.Status == statusCancelled || current.Status == statusSent || current.Status == statusFailed {
		return nil
	}
	n = current

	now := time.Now()

	if n.ScheduledAt.After(now.Add(5 * time.Second)) {
		_ = s.repository.UpdateRetryInfo(ctx, n.ID, "scheduled", n.RetryCount, n.ScheduledAt)
		return ErrTooEarly
	}

	err = s.SendToExternalAPI(ctx, n)

	if err == nil {
		n.Status = "sent"
		n.SentAt = &now
		n.UpdatedAt = now
		return s.repository.UpdateStatus(ctx, n.ID, n.Status, n.SentAt, n.RetryCount)
	}

	// Если ошибка — увеличиваем счетчик ретраев
	n.RetryCount++
	n.UpdatedAt = now

	if n.RetryCount >= n.MaxRetries {
		n.Status = "failed"
		log.Printf("Уведомление %s превысило лимит попыток и помечено как failed", n.ID)
	} else {
		n.Status = "retry"
		backoffMinutes := 1 << uint(n.RetryCount)
		n.ScheduledAt = now.Add(time.Duration(backoffMinutes) * time.Minute)
		log.Printf("Ошибка отправки уведомления %s (попытка %d/%d). Переносим на %v", n.ID, n.RetryCount, n.MaxRetries, n.ScheduledAt)
	}

	repoErr := s.repository.UpdateRetryInfo(ctx, n.ID, n.Status, n.RetryCount, n.ScheduledAt)
	if repoErr != nil {
		log.Printf("Критическая ошибка обновления статуса ретрая в БД: %v", repoErr)
	}

	return err
}

func (s *Service) CheckAndPublishDelayed(ctx context.Context) error {
	notifications, err := s.repository.FetchReadyNotifications(ctx, 50)
	if err != nil {
		return fmt.Errorf("ошибка выборки отложенных уведомлений: %w", err)
	}

	for _, n := range notifications {
		routingKey := "notification-queue"

		err = s.publisher.Push(ctx, n, routingKey)
		if err != nil {
			log.Printf("Не удалось отправить отложенное уведомление %s в очередь: %v", n.ID, err)
			_ = s.repository.UpdateRetryInfo(ctx, n.ID, "scheduled", n.RetryCount, n.ScheduledAt)
			continue
		}
	}
	return nil
}

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
	addr := fmt.Sprintf("%s:%s", s.smtp.SMTPHost, s.smtp.SMTPPort)

	msg := []byte(fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: =?UTF-8?B?0J3QvtCy0L7QtSDRg9Cy0LXQtNC+0LzQu9C10L3QuNC1?=\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"utf-8\"\r\n\r\n%s",
		s.smtp.SenderEmail, n.Recipient, n.Message))

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	// 1. Обычное TCP подключение (для 587)
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("ошибка tcp-подключения: %w", err)
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, s.smtp.SMTPHost)
	if err != nil {
		return fmt.Errorf("ошибка создания smtp-клиента: %w", err)
	}
	defer client.Quit()

	// 2. Переключаемся в TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: false,
		ServerName:         s.smtp.SMTPHost,
	}
	if err = client.StartTLS(tlsConfig); err != nil {
		return fmt.Errorf("ошибка starttls: %w", err)
	}

	// 3. Авторизация
	auth := smtp.PlainAuth("", s.smtp.SenderEmail, s.smtp.Password, s.smtp.SMTPHost)
	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("ошибка авторизации: %w", err)
	}

	if err = client.Mail(s.smtp.SenderEmail); err != nil {
		return err
	}
	if err = client.Rcpt(n.Recipient); err != nil {
		return err
	}

	w, err := client.Data()
	if err != nil {
		return err
	}
	_, err = w.Write(msg)
	if err != nil {
		return err
	}

	return w.Close()
}
