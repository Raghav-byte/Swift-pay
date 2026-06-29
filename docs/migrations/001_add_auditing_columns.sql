-- Run once in Supabase SQL Editor (or any Postgres client) before starting services with JPA validate.
-- Adds optional auditor columns referenced by AuditableBaseEntity.

ALTER TABLE sp_accounts
    ADD COLUMN IF NOT EXISTS created_by UUID,
    ADD COLUMN IF NOT EXISTS modified_by UUID;

ALTER TABLE sp_transactions
    ADD COLUMN IF NOT EXISTS created_by UUID,
    ADD COLUMN IF NOT EXISTS modified_by UUID;
