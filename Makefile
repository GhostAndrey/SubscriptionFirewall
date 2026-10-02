.PHONY: build test test-race cover vet lint run docker-build docker-run

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	go tool cover -func=coverage.txt

vet:
	go vet ./...

lint:
	golangci-lint run

run:
	SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH=true go run ./cmd/firewall

docker-build:
	docker build -t subscription-firewall .

docker-run:
	SUBSCRIPTION_FIREWALL_API_KEYS=local-dev-key docker compose up --build
