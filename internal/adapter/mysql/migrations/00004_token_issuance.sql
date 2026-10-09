-- +goose Up
ALTER TABLE virtual_tokens
    ADD COLUMN issuance_key VARCHAR(128) NULL AFTER state;

-- +goose Down
ALTER TABLE virtual_tokens
    DROP COLUMN issuance_key;