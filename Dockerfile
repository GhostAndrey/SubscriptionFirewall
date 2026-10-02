FROM golang:1.27-alpine AS build
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X subscriptionfirewall/internal/version.Version=${VERSION} -X subscriptionfirewall/internal/version.Commit=${COMMIT} -X subscriptionfirewall/internal/version.BuildDate=${BUILD_DATE}" \
    -o /out/firewall ./cmd/firewall

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/firewall /firewall
EXPOSE 8080
USER nonroot
# /firewall healthcheck probes /readyz in-process: the distroless image ships
# no shell, curl or wget for a classic healthcheck command.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 CMD ["/firewall", "healthcheck"]
ENTRYPOINT ["/firewall"]
