-- +goose Up
CREATE TABLE IF NOT EXISTS lifecycle_sync_outbox (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity_type VARCHAR(32)  NOT NULL,
    entity_id   VARCHAR(64)  NOT NULL,
    action      VARCHAR(32)  NOT NULL,
    status      VARCHAR(16)  NOT NULL DEFAULT 'pending',
    attempts    INT          NOT NULL DEFAULT 0,
    created_at  DATETIME(3)  NOT NULL,
    updated_at  DATETIME(3)  NOT NULL,
    INDEX idx_lifecycle_claim (status, attempts, id),
    INDEX idx_lifecycle_pending (status, updated_at)
) ENGINE=InnoDB;

-- +goose Down
DROP TABLE IF EXISTS lifecycle_sync_outbox;