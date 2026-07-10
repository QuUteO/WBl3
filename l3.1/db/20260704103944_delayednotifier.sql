-- +goose Up
CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('email', 'telegram')),
    message TEXT NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'processing', 'retry', 'sent', 'failed', 'cancelled')),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 5,
    error TEXT,
    sent_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_notifications_status_scheduled
    ON notifications(status, scheduled_at)
    WHERE status IN ('scheduled', 'retry');

CREATE INDEX idx_notifications_retry
    ON notifications(status, retry_count)
    WHERE status = 'retry' AND retry_count < max_retries;

-- +goose Down
DROP TABLE IF EXISTS notifications;
