-- Local Docker Postgres init (Compose). Supabase deployments use docs/SWIFTPAY_ARCHITECTURE.md §3.

CREATE TABLE IF NOT EXISTS sp_accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id        UUID NOT NULL,
    balance         NUMERIC(19,4) NOT NULL DEFAULT 0,
    currency        VARCHAR(10) NOT NULL DEFAULT 'INR',
    version         INTEGER NOT NULL DEFAULT 0,
    created_by      UUID,
    modified_by     UUID,
    date_created    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    date_modified   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_accounts_owner UNIQUE (owner_id),
    CONSTRAINT chk_balance_non_negative CHECK (balance >= 0)
);

CREATE INDEX IF NOT EXISTS idx_accounts_owner_id ON sp_accounts(owner_id);

CREATE TABLE IF NOT EXISTS sp_transactions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id           UUID NOT NULL,
    receiver_id         UUID NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(10) NOT NULL DEFAULT 'INR',
    status              VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    idempotency_key     VARCHAR(255) NOT NULL,
    created_by          UUID,
    modified_by         UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transactions_idempotency UNIQUE (idempotency_key),
    CONSTRAINT chk_amount_positive CHECK (amount > 0),
    CONSTRAINT chk_sender_not_receiver CHECK (sender_id != receiver_id)
);

CREATE INDEX IF NOT EXISTS idx_tx_sender_id ON sp_transactions(sender_id);
CREATE INDEX IF NOT EXISTS idx_tx_receiver_id ON sp_transactions(receiver_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_idempotency ON sp_transactions(idempotency_key);

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
