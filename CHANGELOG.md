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
- Обвязка эмиттера карт: timeout, retry с backoff и circuit breaker (`SUBSCRIPTION_FIREWALL_ISSUER_*`).
- Идемпотентный выпуск карт: бронь в состоянии `Issuing` с ключом `issue:<user>:<merchant>`, восстановление брошенных броней по `SUBSCRIPTION_FIREWALL_ISSUANCE_TIMEOUT`, стабильный `IdempotencyKey` в `ports.IssueRequest`.
- `internal/lifecycle` — единая точка переходов для пары «подписка + карта»: компенсация обратимых действий и отложенная синхронизация необратимых через `lifecycle_sync_outbox` с фоновым сикером.
- Переход подписки в `Zombie` на sweep замораживает связанную карту.
- Метрики `subscriptions_swept_total{state}`, `lifecycle_sync_total{entity,action,result}`, `linked_action_failures_total{entity,action}`, `lifecycle_queue_depth`.
- Валидация инжеста по ограничениям схемы: длины `id`/`user_id`/`merchant_id`/`merchant_name` и границы `authorized_at` (не дальше суток вперёд, не старше двух лет) — `400` вместо `500` от СУБД.
- Оптимистичная блокировка подписок: колонка `version`, `UPDATE … WHERE id = ? AND version = ?` и атомарное создание через `CreateIfAbsent`.
- `docs/CONTRIBUTING.md` — как собрать, протестировать и расширять сервис.
- Индекс `idx_subscriptions_sweep` (миграция `00007`) и порт `ListPendingZombieTransition`: sweep читает только подписки, способные перейти в зомби, батчами вместо полного `ListAll` каждую минуту.
- Предфильтрация кандидатов для детектора: `GROUP BY merchant_id HAVING COUNT(*) >= 2` в базе плюс точечная загрузка истории только этих мерчантов.
- Тест списания лимита через несколько независимых пулов соединений — эмуляция нескольких реплик.
- `SUBSCRIPTION_FIREWALL_ISSUER` — явный выбор эмитента карт вместо жёстко вшитой заглушки; сборка провайдера вынесена в `buildCardIssuer`, неизвестное значение останавливает старт, `simulated` пишет предупреждение о непригодности для продакшена.
- Тесты `cmd/firewall` (парсинг конфига, `addressPort`, сборка эмитента) и `internal/obs` (санитизация логов, метрики).
- Тесты отката миграций по `information_schema` и сохранности данных при пересоздании колонок.
- Тест списания лимита через несколько независимых пулов соединений — эмуляция нескольких реплик.

### Fixed

- `POST /v1/tokens/{id}/authorize` больше не принимает `amount_minor <= 0`: отрицательная сумма уменьшала счётчик расходов и обходила месячный лимит. Проверка продублирована в домене (`domain.ErrInvalidAmount`), в ответе — `400`.
- Списание лимита выполняется атомарно (`SELECT … FOR UPDATE` в одной транзакции), поэтому одновременные авторизации с разных реплик не могут превысить месячный лимит.
- Две реплики больше не выпускают две карты на пару (user, merchant) и не создают две подписки.
- Блокировка sweep-а берётся и освобождается на одном соединении пула и отпускается при отменённом контексте — раньше `GET_LOCK`/`RELEASE_LOCK` могли попасть на разные соединения.
- Сервис без API-ключей не поднимается: `httpapi.NewServer` возвращает ошибку вместо тихого открытия эндпоинтов.
- `envBool` больше не глотает опечатки: неверное значение в `SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH` или `_OTLP_INSECURE` теперь останавливает старт.
- `addressPort` проверяет, что порт числовой и в диапазоне: нечитаемый `SUBSCRIPTION_FIREWALL_ADDRESS` больше не превращает healthcheck в невалидный URL.
- `SUBSCRIPTION_FIREWALL_RATE_BURST` применяется и при Redis-лимитере, который раньше его игнорировал.
- Redis-лимитер отдаёт `503` на `POST /v1/tokens/{id}/authorize` при недоступности Redis вместо fail-open на денежном эндпоинте.
- Тело запроса разбирается до конца: JSON после первого объекта даёт `400`, а не игнорируется.
- Статус ответа пишется один раз (первый `WriteHeader`), при панике в лог и метрики попадает `500`, а не `0`; `statusRecorder` пробрасывает `Flush`, `Hijack` и `Unwrap` вместо двойной обёртки.
- Сравнение API-ключей идёт по SHA-256 в постоянном времени и больше не выдаёт длину ключа через `ConstantTimeCompare`.
- `subscriptions_detected_total{state="Zombie"}` больше не растёт от sweep-а — переходы вынесены в `subscriptions_swept_total`.
- `UpdateVersion` подписки различает отсутствующую строку и конфликт версии: раньше оба случая выглядели как `ErrVersionConflict`, и удалённая подписка выдавала себя за retry-ошибку.
- `Save` подписки и карты больше не может молча переписать чужую строку: апсерт по уникальному ключу `(user_id, merchant_id)` возвращает `ErrAlreadyExists`, если запись поглотилась существующей строкой с другим id.
- Добавлены `mysql.Rollback` и `mysql.MigrationVersion` плюс тесты идемпотентности, отката и сохранности данных при миграциях.

### Changed

- Лицензия изменена с MIT на Apache-2.0 (текст в `LICENSE`, упоминания в README и `api/openapi.yaml`).
- Репозиторий переведён в приватный режим.
- Репозиторий снова открыт как публичный; у проекта появились описание на GitHub, topics-теги и обновлённое вступление README.
