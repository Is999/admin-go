FROM golang:1.26.6 AS build

ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.buildVersion=${VERSION}" -o /out/admin ./cmd/admin
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.buildVersion=${VERSION}" -o /out/admin-migrate ./cmd/migrate

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates clamdscan curl tzdata \
  && rm -rf /var/lib/apt/lists/*
COPY deploy/docker/clamd.conf /etc/clamav/clamd.conf

WORKDIR /app
RUN groupadd --system app && useradd --system --gid app --home-dir /app --no-create-home app \
  && mkdir -p /app/etc /app/logs /app/data \
  && chown -R app:app /app
COPY --from=build /out/admin /usr/local/bin/admin
COPY --from=build /out/admin-migrate /usr/local/bin/admin-migrate
COPY etc/config.sample.yaml /app/etc/config.sample.yaml
COPY etc/config.d /app/etc/config.d

EXPOSE 8899
USER app

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8899/api/live || exit 1

ENTRYPOINT ["/usr/local/bin/admin"]
CMD ["-f", "/app/etc/config.yaml", "-mode", "7"]
