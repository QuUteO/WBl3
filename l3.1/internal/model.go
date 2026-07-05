package model

import (
	"time"

	"github.com/google/uuid"
)

// CreateNotification - запрос на создание уведомления (входящие данные от пользователя)
type CreateNotification struct {
	Recipient   string    `json:"recipient"`
	Channel     string    `json:"channel"` // email, telegram
	Message     string    `json:"message"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// CreateNotificationResponse - ответ на создание уведомления
type CreateNotificationResponse struct {
	NotificationID string `json:"notification_id"`
	Status         string `json:"status"`
}

// Notification - полная модель для работы с БД
type Notification struct {
	ID          uuid.UUID  `json:"id" db:"id"`
	Recipient   string     `json:"recipient" db:"recipient"`
	Channel     string     `json:"channel" db:"channel"`
	Message     string     `json:"message" db:"message"`
	ScheduledAt time.Time  `json:"scheduled_at" db:"scheduled_at"`
	Status      string     `json:"status" db:"status"` // scheduled, sent, failed, cancelled
	RetryCount  int        `json:"retry_count" db:"retry_count"`
	MaxRetries  int        `json:"max_retries" db:"max_retries"`
	Error       *string    `json:"error,omitempty" db:"error"`
	SentAt      *time.Time `json:"sent_at,omitempty" db:"sent_at"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at"`
}

// GetNotificationResponse - ответ на GET /notify/{id}
type GetNotificationResponse struct {
	ID          string     `json:"id"`
	Recipient   string     `json:"recipient"`
	Channel     string     `json:"channel"`
	Message     string     `json:"message"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	Status      string     `json:"status"`
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	Error       *string    `json:"error,omitempty"`
	SentAt      *time.Time `json:"sent_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// DeleteNotificationResponse - ответ на DELETE /notify/{id}
type DeleteNotificationResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}
