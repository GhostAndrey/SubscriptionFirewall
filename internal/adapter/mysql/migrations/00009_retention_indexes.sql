-- +goose Up
ALTER TABLE transactions ADD INDEX idx_transactions_authorized_at (authorized_at);
ALTER TABLE audit_log ADD INDEX idx_audit_created_at (created_at);

-- +goose Down
ALTER TABLE transactions DROP INDEX idx_transactions_authorized_at;
ALTER TABLE audit_log DROP INDEX idx_audit_created_at;
