APP := admin
BIN := bin/$(APP)
VERSION ?= dev
PACKAGE := dist/$(APP)-$(VERSION).tar.gz
LDFLAGS ?= -s -w -X main.buildVersion=$(VERSION)
DOCKER_COMPOSE ?= docker compose
INTEGRATION_MYSQL_DSN ?= root:password@tcp(127.0.0.1:3310)/admin?charset=utf8mb4&parseTime=true&loc=Local&readTimeout=5s&writeTimeout=5s
INTEGRATION_REDIS_ADDRS ?= 127.0.0.1:6390
INTEGRATION_REDIS_CLUSTER_ADDRS ?= 127.0.0.1:6391,127.0.0.1:6392,127.0.0.1:6393
INTEGRATION_REDIS_CLUSTER_ADDR_MAP ?= redis-cluster-1:7001=127.0.0.1:6391,redis-cluster-2:7002=127.0.0.1:6392,redis-cluster-3:7003=127.0.0.1:6393
INTEGRATION_REDIS_USERNAME ?=
INTEGRATION_REDIS_PASSWORD ?=
INTEGRATION_WAIT_TIMEOUT ?= 120
SECRET_SCAN_PATHS := $(wildcard etc/*.sample.yaml) deploy .gitlab-ci.yml Makefile README.md docs/site docs/prometheus docs/grafana
PROMTOOL_IMAGE ?= prom/prometheus:v2.55.1
PROMETHEUS_RULES := $(wildcard docs/prometheus/*.yml)
PROMETHEUS_RULES_IN_CONTAINER := $(patsubst docs/prometheus/%,/rules/%,$(PROMETHEUS_RULES))
GOVULNCHECK_VERSION ?= v1.6.0
GO_TOOLCHAIN ?= go1.26.6

.PHONY: fmt fmt-check test shardingsphere-check test-race vet build build-tools package check ci diff-check branch-drift-check update-route-security-manifest secret-scan promtool-check govulncheck security-scan integration-env-up integration-env-down integration-test migrate-status migrate-dry-run migrate-up migrate-bootstrap clean

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))"

test: shardingsphere-check
	go test ./...

shardingsphere-check:
	./deploy/shardingsphere/render-global_test.sh

test-race:
	go test -race -count=1 ./...

vet:
	go vet ./...

build:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/admin

build-tools:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(APP)-migrate ./cmd/migrate

package: build build-tools
	mkdir -p dist
	tar -czf $(PACKAGE) bin etc/*.sample.yaml etc/config.d deploy docs/prometheus docs/grafana docs/site/角色文档/运维 README.md

secret-scan:
	@! grep -R -n -E 'BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY|(^|[[:space:]])rsa_private_key_server:[[:space:]]*[^[:space:]#]+|(^|[[:space:]])aes_key:[[:space:]]*[^[:space:]#]+|(^|[[:space:]])aes_iv:[[:space:]]*[^[:space:]#]+' $(SECRET_SCAN_PATHS)

promtool-check:
	@if command -v promtool >/dev/null 2>&1; then promtool check rules $(PROMETHEUS_RULES); elif command -v docker >/dev/null 2>&1; then docker run --rm --entrypoint promtool -v "$$(pwd)/docs/prometheus:/rules:ro" $(PROMTOOL_IMAGE) check rules $(PROMETHEUS_RULES_IN_CONTAINER); else echo "promtool and docker not found, skip"; fi

govulncheck:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

security-scan: secret-scan govulncheck

diff-check:
	git diff --check

BRANCH_BASE ?= main
BRANCH_VARIANT ?=

branch-drift-check:
	./scripts/check-table-sharding-drift.sh "$(BRANCH_BASE)" "$(BRANCH_VARIANT)"

update-route-security-manifest:
	UPDATE_ROUTE_SECURITY_MANIFEST=1 go test -run TestDefaultRouteSecurityManifestMatchesSnapshots ./internal/handler

check: fmt-check test vet build build-tools secret-scan promtool-check govulncheck diff-check

ci: branch-drift-check check test-race

integration-env-up:
	$(DOCKER_COMPOSE) -f deploy/integration/docker-compose.yml up -d --wait --wait-timeout $(INTEGRATION_WAIT_TIMEOUT)
	$(DOCKER_COMPOSE) -f deploy/integration/docker-compose.yml run --rm --no-deps redis-cluster-init

integration-env-down:
	$(DOCKER_COMPOSE) -f deploy/integration/docker-compose.yml down -v

integration-test: integration-env-up
	@INTEGRATION_REDIS_ADDRS='$(INTEGRATION_REDIS_ADDRS)' \
		INTEGRATION_REDIS_CLUSTER_ADDRS='$(INTEGRATION_REDIS_CLUSTER_ADDRS)' \
		INTEGRATION_REDIS_CLUSTER_ADDR_MAP='$(INTEGRATION_REDIS_CLUSTER_ADDR_MAP)' \
		INTEGRATION_REDIS_USERNAME='$(INTEGRATION_REDIS_USERNAME)' \
		INTEGRATION_REDIS_PASSWORD='$(INTEGRATION_REDIS_PASSWORD)' \
		go test -count=1 -tags=integration ./internal/infra/redislimit ./internal/task/queue
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 -tags=integration ./internal/database
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 -tags=integration ./internal/logic/admin
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 -tags=integration ./internal/audit
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 -tags=integration ./internal/infra/collectorx
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 -tags=integration ./internal/jobs/archive
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 ./internal/sharding/migration
	INTEGRATION_MYSQL_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 ./internal/logic/runtimeconfig -run '^(TestPublishPreparedSnapshotSerializesConcurrentDraftSave|TestPublishInitialDraftAvoidsDuplicateRelease)$$'
	SHARD_BACKFILL_TEST_DSN='$(INTEGRATION_MYSQL_DSN)' go test -count=1 ./cmd/shardbackfill -run '^TestBackfillResumesAndVerifiesOnMySQL$$'

MIGRATE_CONFIG ?= ./etc/config.yaml

migrate-status:
	go run ./cmd/migrate -f $(MIGRATE_CONFIG) -action=status

migrate-dry-run:
	go run ./cmd/migrate -f $(MIGRATE_CONFIG) -action=dry-run

migrate-up:
	go run ./cmd/migrate -f $(MIGRATE_CONFIG) -action=up

migrate-bootstrap:
	go run ./cmd/migrate -f $(MIGRATE_CONFIG) -action=up -allow-bootstrap -allow-destructive

clean:
	rm -rf bin dist
