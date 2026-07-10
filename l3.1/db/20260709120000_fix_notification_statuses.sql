-- +goose Up
ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_status_check;

ALTER TABLE notifications
    ADD CONSTRAINT notifications_status_check
    CHECK (status IN ('scheduled', 'processing', 'retry', 'sent', 'failed', 'cancelled'));

DROP INDEX IF EXISTS idx_notifications_status_scheduled;
CREATE INDEX idx_notifications_status_scheduled
    ON notifications(status, scheduled_at)
    WHERE status IN ('scheduled', 'retry');

DROP INDEX IF EXISTS idx_notifications_retry;
CREATE INDEX idx_notifications_retry
    ON notifications(status, retry_count)
    WHERE status = 'retry' AND retry_count < max_retries;

-- +goose Down
DROP INDEX IF EXISTS idx_notifications_retry;
CREATE INDEX idx_notifications_retry
    ON notifications(status, retry_count)
    WHERE status = 'failed' AND retry_count < max_retries;

DROP INDEX IF EXISTS idx_notifications_status_scheduled;
CREATE INDEX idx_notifications_status_scheduled
    ON notifications(status, scheduled_at)
    WHERE status = 'scheduled';

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_status_check;

ALTER TABLE notifications
    ADD CONSTRAINT notifications_status_check
    CHECK (status IN ('scheduled', 'sent', 'failed', 'cancelled'));
