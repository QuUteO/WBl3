package repository

import (
	model "DelayedNotifier/internal"
	"context"
	"time"

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

func (r *Repository) UpdateStatus(ctx context.Context, id uuid.UUID, status string, at *time.Time, count int) error {
	sql := "UPDATE notifications SET status = $2, sent_at = $3, updated_at = $4, retry_count = $5 WHERE id = $1"

	_, err := r.conn.Exec(ctx, sql, id, status, at, time.Now(), count)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) UpdateRetryInfo(ctx context.Context, id uuid.UUID, status string, count int, at time.Time) error {
	sql := "UPDATE notifications SET status = $2, retry_count = $3, scheduled_at = $4, updated_at = $5 WHERE id = $1"

	_, err := r.conn.Exec(ctx, sql, id, status, count, at, time.Now())
	if err != nil {
		return err
	}

	return nil
}

// FetchReadyNotifications атомарно выбирает готовые к отправке записи и блокирует их статусом 'processing'
func (r *Repository) FetchReadyNotifications(ctx context.Context, limit int) ([]*model.Notification, error) {
	query := `
		UPDATE notifications 
		SET status = 'processing', updated_at = $1
		WHERE id IN (
			SELECT id FROM notifications 
			WHERE status IN ('scheduled', 'retry') AND scheduled_at <= $1
			ORDER BY scheduled_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, recipient, channel, message, scheduled_at, status, retry_count, max_retries, sent_at, created_at, updated_at`

	rows, err := r.conn.Query(ctx, query, time.Now(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*model.Notification
	for rows.Next() {
		var n model.Notification
		err := rows.Scan(
			&n.ID, &n.Recipient, &n.Channel, &n.Message, &n.ScheduledAt,
			&n.Status, &n.RetryCount, &n.MaxRetries, &n.SentAt, &n.CreatedAt, &n.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		list = append(list, &n)
	}

	return list, nil
}
