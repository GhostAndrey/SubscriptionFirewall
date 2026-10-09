# Contributing

Короткий маршрут от клона до принятого пул-реквеста.

## Сборка и запуск

Нужны Go 1.27+ и, по желанию, Docker.

```bash
make run   # in-memory хранилище, без auth — только для разработки
```

Полный стек (сервис + MySQL + Redis) поднимается через `.env`:

```bash
cp .env.example .env   # задать SUBSCRIPTION_FIREWALL_API_KEYS
docker compose up --build
```

С трейсингом: `docker compose --profile tracing up -d` (Jaeger UI на :16686),
в `.env` добавить `SUBSCRIPTION_FIREWALL_OTLP_ENDPOINT=jaeger:4318`.

## Тесты и линтер

```bash
make test        # юнит-тесты
make test-race   # то же с детектором гонок
make cover       # покрытие
make lint        # golangci-lint v2 (в CI запинен на v2.13.2)
```

Интеграционные тесты MySQL-адаптера выполняются, только когда задан
`SUBSCRIPTION_FIREWALL_TEST_DSN`; в CI они гоняются против сервиса
`mysql:8.4` в workflow. Локально подойдёт любой MySQL 8.x — адаптер
проверен и на 8.0.

### `-race` на Windows

Детектор гонок требует C-компилятор, поэтому `CGO_ENABLED` должен быть `1`:

```powershell
winget install --id BrechtSanders.WinLibs.POSIX.UCRT -e
# перезапустить оболочку, затем:
$env:CGO_ENABLED = "1"
make test-race
```

### MySQL без Docker

Если Docker недоступен, подойдёт локальный сервер. Отдельный инстанс в
температурном каталоге не трогает существующие базы:

```powershell
$basedir = "C:\Program Files\MySQL\MySQL Server 8.0"
$base = "$env:TEMP\firewall-mysql"
New-Item -ItemType Directory -Force -Path "$base\data","$base\tmp","$base\log"
& "$basedir\bin\mysqld.exe" --initialize-insecure "--basedir=$basedir" "--datadir=$base\data"
```

Затем `my.ini` в `$base` с `datadir`, `tmpdir`, `log-error` и свободным
`port`, запуск `mysqld --defaults-file=$base\my.ini` и база с пользователем:

```sql
CREATE DATABASE subscription_firewall_test CHARACTER SET utf8mb4;
CREATE USER 'firewall'@'%' IDENTIFIED BY 'firewall';
GRANT ALL PRIVILEGES ON subscription_firewall_test.* TO 'firewall'@'%';
```

Прогон с `SUBSCRIPTION_FIREWALL_TEST_DSN=firewall:firewall@tcp(127.0.0.1:<port>)/subscription_firewall_test?parseTime=true&loc=UTC`
поднимает миграции сам и проверяет их откат (`internal/adapter/mysql/migrations_test.go`).

## Что где лежит

Карта кода и устройство сервиса — в [README](../README.md). Ключевое:

- домен (`internal/domain`) не зависит ни от чего внешнего; интерфейсы
  к внешнему миру — в `internal/ports`;
- новый адаптер = реализация порта + wiring в `cmd/firewall/main.go`;
- изменение схемы = новый файл `internal/adapter/mysql/migrations/000NN_*.sql`
  (goose-формат `-- +goose Up` / `-- +goose Down`, применяется при старте);
  откат тоже проверяется — `migrations_test.go` прогоняет `DownTo` и сверяет
  `information_schema`, поэтому добавьте сюда свой случай, если меняете схему;
- новый эндпоинт = хендлер в `internal/adapter/httpapi/handlers.go` +
  маршрут в `server.go` + скоуп в `auth.go` (`requiredScope`) +
  обновление `api/openapi.yaml`.

## Требования к пул-реквестам

- тесты на новую логику; при изменении API — обновлённые
  `api/openapi.yaml` и README;
- `make lint` и `make test-race` проходят локально;
- коммиты в духе Conventional Commits (`feat:`, `fix:`, `docs:`,
  `chore:`) — так пишет историю и сам проект;
- гарантии обработки не ослабляются: outbox при инжесте, идемпотентность
  детекции и синхронность freeze/reactivate/terminate для подписки и её
  карты — часть контракта, а не деталь реализации.

## Сообщения об ошибках

В issue прикладывайте версию (`GET /version`), шаги воспроизведения,
ожидаемое и фактическое поведение, логи без чувствительных данных.
Логи сервиса и так санитизируются, но это не повод вставлять в issue
реальные PAN или API-ключи.
