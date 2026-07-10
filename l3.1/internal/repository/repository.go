package repository

import (
	model "DelayedNotifier/internal"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wb-go/wbf/dbpg/pgx-driver"
	"github.com/wb-go/wbf/redis"
	"github.com/wb-go/wbf/retry"
)

type Repository struct {
	conn          *pgxdriver.Postgres
	redisClient   *redis.Client
	redisStrategy retry.Strategy
}

func New(conn *pgxdriver.Postgres, rClient *redis.Client, rStrategy retry.Strategy) *Repository {
	return &Repository{
		conn:          conn,
		redisClient:   rClient,
		redisStrategy: rStrategy,
	}
}

// Вспомогательный метод для генерации ключа кэша
func (r *Repository) buildCacheKey(id uuid.UUID) string {
	return fmt.Sprintf("notification:%s", id.String())
}

func (r *Repository) CreateNotification(ctx context.Context, notification *model.Notification) error {
	// 1. Сначала пишем в основную базу (Postgres)
	_, err := r.conn.Exec(ctx, "INSERT INTO notifications (id, recipient, channel, message, scheduled_at, status, retry_count, max_retries, sent_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		notification.ID, notification.Recipient, notification.Channel, notification.Message, notification.ScheduledAt,
		notification.Status, notification.RetryCount, notification.MaxRetries, notification.SentAt, notification.CreatedAt, notification.UpdatedAt,
	)
	if err != nil {
		return err
	}

	// 2. Сразу «греем» кэш в Redis, чтобы разгрузить Postgres на последующие GET-запросы
	jsonData, err := json.Marshal(notification)
	if err == nil {
		// Используем метод из твоей либы: принимает стратегию, ключ, значение (строку) и TTL
		_ = r.redisClient.SetWithExpirationAndRetry(ctx, r.redisStrategy, r.buildCacheKey(notification.ID), string(jsonData), 2*time.Hour)
	}

	return nil
}

func (r *Repository) GetNotification(ctx context.Context, notificationID uuid.UUID) (*model.Notification, error) {
	key := r.buildCacheKey(notificationID)

	// 1. Пытаемся быстро прочитать из Redis с ретраями
	val, err := r.redisClient.GetWithRetry(ctx, r.redisStrategy, key)
	if err == nil && val != "" {
		var n model.Notification
		if json.Unmarshal([]byte(val), &n) == nil {
			return &n, nil // Кэш-хит! Отдали данные мгновенно из памяти
		}
	}

	// Если вернулась ошибка "NoMatches" (ключ не найден), не паникуем, идем в Postgres
	if err != nil && !errors.Is(err, redis.NoMatches) {
		// Ошибку сети с Redis можно просто залогировать, чтобы приложение не падало
		fmt.Printf("Redis error (GetNotification): %v\n", err)
	}

	// 2. Кэш-мисс: Читаем из Postgres
	var n model.Notification
	row := r.conn.QueryRow(ctx, "SELECT id, recipient, channel, message, scheduled_at, status, retry_count, max_retries, sent_at, created_at, updated_at FROM notifications WHERE id = $1", notificationID)
	err = row.Scan(&n.ID, &n.Recipient, &n.Channel, &n.Message, &n.ScheduledAt, &n.Status, &n.RetryCount, &n.MaxRetries, &n.SentAt, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return nil, err
	}

	// 3. Сохраняем свежие данные в Redis, чтобы следующий запрос попал в кэш
	jsonData, err := json.Marshal(&n)
	if err == nil {
		_ = r.redisClient.SetWithExpirationAndRetry(ctx, r.redisStrategy, key, string(jsonData), 2*time.Hour)
	}

	return &n, nil
}

func (r *Repository) ListNotifications(ctx context.Context, limit int) ([]*model.Notification, error) {
	rows, err := r.conn.Query(ctx, "SELECT id, recipient, channel, message, scheduled_at, status, retry_count, max_retries, sent_at, created_at, updated_at FROM notifications ORDER BY created_at DESC LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*model.Notification
	for rows.Next() {
		var n model.Notification
		if err := rows.Scan(&n.ID, &n.Recipient, &n.Channel, &n.Message, &n.ScheduledAt, &n.Status, &n.RetryCount, &n.MaxRetries, &n.SentAt, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, err
		}
		list = append(list, &n)
	}

	return list, rows.Err()
}

func (r *Repository) DeleteNotification(ctx context.Context, notificationID uuid.UUID) error {
	// Отмена сохраняет запись, чтобы GET /notify/{id} мог вернуть финальный статус.
	_, err := r.conn.Exec(ctx, "UPDATE notifications SET status = 'cancelled', updated_at = $2 WHERE id = $1 AND status IN ('scheduled', 'processing', 'retry')", notificationID, time.Now())
	if err != nil {
		return err
	}

	// 2. Инвалидация кэша: удаляем ключ из Redis, чтобы GET-ручка больше не отдавала старые данные
	_ = r.redisClient.DelWithRetry(ctx, r.redisStrategy, r.buildCacheKey(notificationID))

	return nil
}

func (r *Repository) UpdateStatus(ctx context.Context, id uuid.UUID, status string, at *time.Time, count int) error {
	sql := "UPDATE notifications SET status = $2, sent_at = $3, updated_at = $4, retry_count = $5 WHERE id = $1"
	_, err := r.conn.Exec(ctx, sql, id, status, at, time.Now(), count)
	if err != nil {
		return err
	}

	// Инвалидируем кэш, так как статус изменился (например, на 'sent')
	_ = r.redisClient.DelWithRetry(ctx, r.redisStrategy, r.buildCacheKey(id))

	return nil
}

func (r *Repository) UpdateRetryInfo(ctx context.Context, id uuid.UUID, status string, count int, at time.Time) error {
	sql := "UPDATE notifications SET status = $2, retry_count = $3, scheduled_at = $4, updated_at = $5 WHERE id = $1"
	_, err := r.conn.Exec(ctx, sql, id, status, count, at, time.Now())
	if err != nil {
		return err
	}

	// Сбрасываем кэш, чтобы при GET запросе были актуальные данные по ретраям
	_ = r.redisClient.DelWithRetry(ctx, r.redisStrategy, r.buildCacheKey(id))

	return nil
}

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
		err := rows.Scan(&n.ID, &n.Recipient, &n.Channel, &n.Message, &n.ScheduledAt, &n.Status, &n.RetryCount, &n.MaxRetries, &n.SentAt, &n.CreatedAt, &n.UpdatedAt)
		if err != nil {
			return nil, err
		}
		list = append(list, &n)
	}

	return list, nil
}
