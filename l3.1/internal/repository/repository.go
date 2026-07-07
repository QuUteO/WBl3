package repository

import (
	model "DelayedNotifier/internal"
	"context"

	"github.com/google/uuid"
	pgxdriver "github.com/wb-go/wbf/dbpg/pgx-driver"
)

type Repository struct {
	conn *pgxdriver.Postgres
}

func New(conn *pgxdriver.Postgres) *Repository {
	return &Repository{
		conn: conn,
	}
}

func (r *Repository) CreateNotification(ctx context.Context, notification *model.Notification) error {
	_, err := r.conn.Exec(ctx, "INSERT INTO notifications (id, recipient, channel, message, scheduled_at, status, retry_count, max_retries, sent_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		notification.ID,
		notification.Recipient,
		notification.Channel,
		notification.Message,
		notification.ScheduledAt,
		notification.Status,
		notification.RetryCount,
		notification.MaxRetries,
		notification.SentAt,
		notification.CreatedAt,
		notification.UpdatedAt,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) GetNotification(ctx context.Context, notificationID uuid.UUID) (*model.Notification, error) {
	var n model.Notification

	row := r.conn.QueryRow(ctx, "SELECT id, recipient, channel, message, scheduled_at, status, retry_count, max_retries, sent_at, created_at, updated_at FROM notifications WHERE id = $1", notificationID)

	err := row.Scan(
		&n.ID,
		&n.Recipient,
		&n.Channel,
		&n.Message,
		&n.ScheduledAt,
		&n.Status,
		&n.RetryCount,
		&n.MaxRetries,
		&n.SentAt,
		&n.CreatedAt,
		&n.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &n, nil
}

func (r *Repository) DeleteNotification(ctx context.Context, notificationID uuid.UUID) error {
	_, err := r.conn.Exec(ctx, "DELETE FROM notifications WHERE id = $1", notificationID)
	if err != nil {
		return err
	}

	return nil
}
