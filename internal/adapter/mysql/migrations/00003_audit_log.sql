-- +goose Up
CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    actor       VARCHAR(128) NOT NULL,
    action      VARCHAR(32)  NOT NULL,
    entity_type VARCHAR(32)  NOT NULL,
    entity_id   VARCHAR(64)  NOT NULL,
    created_at  DATETIME(3)  NOT NULL,
    INDEX idx_audit_entity (entity_type, entity_id)
) ENGINE=InnoDB;

-- +goose Down
DROP TABLE IF EXISTS audit_log;
