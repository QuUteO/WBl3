package model

import "time"

type CreateNotification struct {
	Recipient   string    `json:"recipient"`
	Channel     string    `json:"channel"` // email telegram
	Message     string    `json:"message"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type CreateNotificationResponse struct {
	NotificationID string `json:"notification_id"`
	Status         string `json:"status"`
}
