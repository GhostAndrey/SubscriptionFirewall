# Changelog

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/),
версионирование — [SemVer](https://semver.org/lang/ru/).

## [Unreleased]

### Added

- `GET /readyz` — readiness-проба с проверкой доступности БД (503, если БД недоступна).
- `GET /version` — версия, commit и дата сборки; метрика `subscription_firewall_build_info`.
- `HEALTHCHECK` в Dockerfile и docker-compose через подкоманду `firewall healthcheck`.
- Request-id: middleware `X-Request-Id` (генерация или проброс входящего), request_id в логах запросов и echoed в ответах.
- OpenAPI-спецификация API: `api/openapi.yaml`.
- MySQL в docker-compose с healthcheck; стек поднимается одной командой `docker compose up`.
- Интеграционные тесты MySQL-адаптера в CI (сервис mysql в workflow).
- Публикация Docker-образа в GHCR (`.github/workflows/release.yml`), Dependabot.
- Env-конфигурация: уровня логов (`SUBSCRIPTION_FIREWALL_LOG_LEVEL`), пула БД (`SUBSCRIPTION_FIREWALL_DB_MAX_OPEN_CONNS`, `_MAX_IDLE_CONNS`, `_CONN_MAX_LIFETIME`), таймаутов HTTP (`SUBSCRIPTION_FIREWALL_READ_TIMEOUT`, `_WRITE_TIMEOUT`, `_SHUTDOWN_TIMEOUT`).
- `SUBSCRIPTION_FIREWALL_STORAGE=auto|memory|mysql` — явный выбор хранилища (по умолчанию auto: mysql при заданном DSN, иначе memory).
- `.env.example`, `LICENSE`, этот CHANGELOG; цели `test-race` и `cover` в Makefile.
- OpenTelemetry-трейсинг: HTTP-спаны и спаны детекции, экспорт OTLP/HTTP (`SUBSCRIPTION_FIREWALL_OTLP_ENDPOINT`); Jaeger в compose под профилем `tracing`.
- Redis rate limit: при заданном `SUBSCRIPTION_FIREWALL_REDIS_ADDR` лимит по IP общий для всех реплик; Redis в compose.
- Схема БД переведена на версионированные goose-миграции (`internal/adapter/mysql/migrations/*.sql`) вместо `CREATE TABLE IF NOT EXISTS` в коде.
- Обвязка эмитента карт: timeout, retry с backoff и circuit breaker (`SUBSCRIPTION_FIREWALL_ISSUER_*`).
- `docs/CONTRIBUTING.md` — как собрать, протестировать и расширять сервис.

### Changed

- Лицензия изменена с MIT на Apache-2.0 (текст в `LICENSE`, упоминания в README и `api/openapi.yaml`).
- Репозиторий переведён в приватный режим.
- Репозиторий снова открыт как публичный; у проекта появились описание на GitHub, topics-теги и обновлённое вступление README.
