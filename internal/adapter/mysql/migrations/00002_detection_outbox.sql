-- +goose Up
CREATE TABLE IF NOT EXISTS detection_outbox (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id    VARCHAR(64) NOT NULL,
    status     VARCHAR(16) NOT NULL DEFAULT 'pending',
    attempts   INT         NOT NULL DEFAULT 0,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    INDEX idx_outbox_claim (status, attempts, id)
) ENGINE=InnoDB;

-- +goose Down
DROP TABLE IF EXISTS detection_outbox;
