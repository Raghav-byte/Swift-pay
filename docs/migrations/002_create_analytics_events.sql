-- Run once in Supabase SQL Editor before starting analytics-worker (JPA ddl-auto=validate).

CREATE TABLE IF NOT EXISTS sp_analytics_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id      UUID NOT NULL,
    sender_id           UUID NOT NULL,
    receiver_id         UUID NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(10) NOT NULL DEFAULT 'INR',
    completed_at        TIMESTAMPTZ NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_completed_at ON sp_analytics_events(completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_currency ON sp_analytics_events(currency);
