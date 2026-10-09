-- +goose Up
-- Batched sweep reads only subscriptions that are due, ordered by the due
-- date so it can stop early. Leading column next_expected_at matches the
-- ORDER BY, which keeps the scan from sorting and lets LIMIT cut off work.
ALTER TABLE subscriptions
    ADD INDEX idx_subscriptions_sweep (next_expected_at, state);

-- +goose Down
ALTER TABLE subscriptions
    DROP INDEX idx_subscriptions_sweep;