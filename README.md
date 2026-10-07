# Subscription Firewall

![CI](https://github.com/GhostAndrey/SubscriptionFirewall/actions/workflows/ci.yml/badge.svg)
![Release](https://github.com/GhostAndrey/SubscriptionFirewall/actions/workflows/release.yml/badge.svg)
![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)

У каждой подписки — собственная одноразовая виртуальная карта с месячным лимитом. Мерчант не спишет ни копейки сверх разрешённого, а заморозка подписки в один шаг замораживает и её карту. На этом принципе стоит весь сервис.

Go-микросервис читает поток транзакций и сам разбирается, что перед ним подписка: группирует платежи по мерчанту, меряет медианный интервал между ними, требует стабильности сумм (±5%) и совпадения валюты, сверяется с «подписочными» MCC. Трёх списаний достаточно для вердикта — подписочным категориям хватает двух. Найденное получает виртуальную карту и живёт дальше по понятным правилам: триал → активная → заморожена → завершена, а мерчант, который перестал списывать, отправляется в зомби.

## Архитектура

Гексагональная: домен и use-case слои не зависят от адаптеров.

- `cmd/firewall` — сборка, конфигурация из env, graceful shutdown.
- `internal/domain` — сущности и правила жизненного цикла (подписка, виртуальный токен, транзакция).
- `internal/ports` — интерфейсы репозиториев и эмиттера карт.
- `internal/adapter/httpapi` — REST API, middleware (auth, recovery, logging, metrics).
- `internal/adapter/memory` — in-memory репозитории (для тестов и dev).
- `internal/adapter/mysql` — MySQL-репозитории (включаются через `SUBSCRIPTION_FIREWALL_DB_DSN`).
- `internal/adapter/issuer` — эмитент виртуальных карт (заглушка).
- `internal/detector` — детектор регулярных платежей (медианный интервал, стабильность сумм, MCC).
- `internal/pipeline` — асинхронная очередь детекции по пользователям.
- `internal/subscription`, `internal/token` — use-case сервисы.
- `internal/obs` — метрики Prometheus и санитизация логов.
- `pkg/masking` — маскирование PAN и идентификаторов.

## Поток данных

1. `POST /v1/transactions` сохраняет транзакцию (без PAN) и ставит пользователя в очередь детекции.
2. Воркеры ищут регулярные списания; найденные подписки получают виртуальную карту и запись о подписке.
3. Списания по виртуальной карте авторизуются через `POST /v1/tokens/{id}/authorize` с проверкой месячного лимита.
4. Фоновый sweep-джоб переводит просроченные подписки в `Zombie`.

Freeze/reactivate/terminate подписки и её виртуальной карты синхронизированы: заморозка подписки замораживает карту, и наоборот.

## Запуск

```bash
# локально (без auth — только для разработки)
make run

# через docker compose (поднимает сервис + MySQL)
SUBSCRIPTION_FIREWALL_API_KEYS=local-dev-key make docker-run
```

Спецификация API: [api/openapi.yaml](api/openapi.yaml). Лицензия — [Apache-2.0](LICENSE). Руководство для контрибьюторов: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

Без настроенных ключей API сервис не стартует — нужно либо задать `SUBSCRIPTION_FIREWALL_API_KEYS`, либо явно разрешить анонимный доступ (`SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH=true`).

## Аутентификация и скоупы

Все эндпоинты, кроме `GET /healthz` и `GET /metrics`, требуют API-ключ в заголовке `X-API-Key` или `Authorization: Bearer <key>`.

Формат ключей: `ключ:скоуп1,скоуп2`; ключ без скоупов имеет полный доступ. Скоупы:

- `ingest` — инжест транзакций (`POST /v1/transactions`);
- `read` — все GET-эндпоинты;
- `manage` — freeze/reactivate/terminate и авторизация списаний;
- `full` — всё сразу.

Запрос с валидным ключом без нужного скоупа получает `403`. Сравнение ключей выполняется за константное время.

```bash
curl -H "X-API-Key: local-dev-key" http://localhost:8080/v1/users/user-1/subscriptions
```

## Гарантии обработки

Инжест транзакции и постановка задачи детекции пишутся в одной SQL-транзакции (таблица `detection_outbox`). Воркеры разбирают очередь через `SELECT ... FOR UPDATE SKIP LOCKED`, так что несколько реплик не обрабатывают одну задачу дважды; упавшая задача переигрывается (детекция идемпотентна), после 8 попыток уходит в статус `failed`. Задачи зависших воркеров возвращает maintenance-цикл. `freezes`/`reactivates`/`terminates` пишутся в `audit_log` с маскированным ключом-актором.

## Конфигурация

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SUBSCRIPTION_FIREWALL_ADDRESS` | `:8080` | Адрес HTTP-сервера |
| `SUBSCRIPTION_FIREWALL_API_KEYS` | — | API-ключи через запятую |
| `SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH` | `false` | Разрешить запуск без ключей (только dev) |
| `SUBSCRIPTION_FIREWALL_STORAGE` | `auto` | `auto` — mysql при заданном DSN, иначе memory; `memory`/`mysql` фиксируют выбор |
| `SUBSCRIPTION_FIREWALL_WORKERS` | `4` | Воркеры детекции |
| `SUBSCRIPTION_FIREWALL_QUEUE_CAPACITY` | `1024` | Ёмкость очереди детекции |
| `SUBSCRIPTION_FIREWALL_SWEEP_INTERVAL` | `1m` | Период sweep-джа зомби |
| `SUBSCRIPTION_FIREWALL_ISSUER_MONTHLY_LIMIT` | `100000` | Месячный лимит карты (minor units) |
| `SUBSCRIPTION_FIREWALL_DB_DSN` | — | DSN MySQL; без переменной — in-memory хранилище |
| `SUBSCRIPTION_FIREWALL_DB_MAX_OPEN_CONNS` | `16` | Максимум открытых соединений с БД |
| `SUBSCRIPTION_FIREWALL_DB_MAX_IDLE_CONNS` | `16` | Максимум простаивающих соединений |
| `SUBSCRIPTION_FIREWALL_DB_CONN_MAX_LIFETIME` | `5m` | Время жизни соединения |
| `SUBSCRIPTION_FIREWALL_LOG_LEVEL` | `info` | Уровень логов: `debug`, `info`, `warn`, `error` |
| `SUBSCRIPTION_FIREWALL_READ_TIMEOUT` | `10s` | HTTP read timeout |
| `SUBSCRIPTION_FIREWALL_WRITE_TIMEOUT` | `10s` | HTTP write timeout |
| `SUBSCRIPTION_FIREWALL_SHUTDOWN_TIMEOUT` | `10s` | Таймаут graceful shutdown |
| `SUBSCRIPTION_FIREWALL_RATE_LIMIT` | `100` | Запросов в секунду на клиентский IP (0 — выключить) |
| `SUBSCRIPTION_FIREWALL_RATE_BURST` | `200` | Допустимый burst сверх лимита (in-memory limiter) |
| `SUBSCRIPTION_FIREWALL_REDIS_ADDR` | — | Адрес Redis; при заданном rate limit общий для всех реплик |
| `SUBSCRIPTION_FIREWALL_REDIS_PASSWORD` | — | Пароль Redis (если требуется) |
| `SUBSCRIPTION_FIREWALL_OTLP_ENDPOINT` | — | OTLP-эндпоинт трейсинга (`host:port`); без переменной трейсинг выключен |
| `SUBSCRIPTION_FIREWALL_OTLP_INSECURE` | `true` | Экспорт трейсов по http (без TLS) |
| `SUBSCRIPTION_FIREWALL_ISSUER_TIMEOUT` | `5s` | Таймаут одного вызова эмитента карт |
| `SUBSCRIPTION_FIREWALL_ISSUER_RETRIES` | `2` | Повторы после первой неудачи |
| `SUBSCRIPTION_FIREWALL_ISSUER_BREAKER_THRESHOLD` | `5` | Подряд неудач, открывающих circuit breaker |
| `SUBSCRIPTION_FIREWALL_ISSUER_BREAKER_COOLDOWN` | `30s` | Пауза breaker-а до пробного вызова |

Пример DSN: `firewall:пароль@tcp(127.0.0.1:3306)/subscription_firewall?parseTime=true&loc=UTC`. Схема применяется версионированными миграциями [goose](https://pressly.github.io/goose/) (SQL-файлы в `internal/adapter/mysql/migrations`) при старте — повторный запуск безопасен. Интеграционные тесты адаптера запускаются при заданном `SUBSCRIPTION_FIREWALL_TEST_DSN`.

## API

| Метод и путь | Описание |
|---|---|
| `GET /healthz` | Liveness (без auth) |
| `GET /readyz` | Readiness: `503`, если БД недоступна (без auth) |
| `GET /version` | Версия, commit и дата сборки (без auth) |
| `GET /metrics` | Prometheus (без auth) |
| `POST /v1/transactions` | Инжест транзакции, идемпотентен по `id` (`409` на повтор) |
| `GET /v1/users/{user_id}/subscriptions?limit&offset` | Подписки пользователя (пагинация) |
| `GET /v1/subscriptions/{id}` | Подписка по id |
| `POST /v1/subscriptions/{id}/freeze` / `reactivate` / `terminate` | Жизненный цикл подписки (синхронно с картой) |
| `GET /v1/users/{user_id}/tokens?limit&offset` | Виртуальные карты пользователя |
| `GET /v1/tokens/{id}` | Токен по id |
| `POST /v1/tokens/{id}/freeze` / `reactivate` / `terminate` | Жизненный цикл карты (синхронно с подпиской) |
| `POST /v1/tokens/{id}/authorize` | Авторизация списания по карте |

Коды ошибок: `400` — валидация, `401` — нет ключа, `403` — не хватает скоупа или карта не активна, `402` — превышен лимит, `404` — не найдено, `409` — дубликат/невалидный переход, `429` — rate limit.

## Метрики

`subscription_firewall_*` в `/metrics`: инжест, детекции, найденные подписки по состояниям, авторизации по результатам, глубина очереди, дропы при переполнении, HTTP-запросы и латентность, `build_info` (версия/commit/дата сборки).

## Наблюдаемость

- **Request-id**: каждый ответ содержит `X-Request-Id`; входящий заголовок пробрасывается, иначе генерируется. `request_id` пишется в логи HTTP-запросов и инжеста — используйте его для сквозной корреляции.
- **Контейнерный healthcheck**: образ собирается на distroless (без шелла), поэтому `HEALTHCHECK` вызывает подкоманду `/firewall healthcheck`, которая опрашивает `/readyz` того же процесса.
- **Трейсинг**: при заданном `SUBSCRIPTION_FIREWALL_OTLP_ENDPOINT` HTTP-запросы и детекция оборачиваются в OpenTelemetry-спаны (экспорт OTLP/HTTP, W3C trace context). Локально удобно поднять Jaeger: `docker compose --profile tracing up -d jaeger` (UI на `:16686`).

## CI/CD

- `.github/workflows/ci.yml` — vet, `go test -race` с покрытием и линтер; интеграционные тесты MySQL-адаптера гоняются против сервиса `mysql:8.4` в workflow.
- `.github/workflows/release.yml` — сборка и публикация образа в GHCR при пуше в `main` и тегах `v*`; версия/commit/дата прошиваются через ldflags.
- `.github/dependabot.yml` — еженедельные обновления gomod, GitHub Actions и Dockerfile.

## Ограничения

- Эмитент карт заглушечный; для продакшена — реальный провайдер виртуальных карт (обвязка с timeout/retry/circuit breaker уже стоит перед эмитентом).
- Очередь детекции in-memory без БД теряет задачи при рестарте (dev-режим); с MySQL — outbox с гарантией at-least-once.
- Пагинация выполняется в приложении; при больших объёмах — перенести в SQL.
- Трейс-контекст не распространяется из HTTP-запроса в задачу outbox: спаны детекции не связаны со спаном инжеста (корреляция по `user_id` и request-id в логах).
