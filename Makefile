SHELL := bash
.DEFAULT_GOAL := help

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE   ?= ledgerline:dev
CLUSTER ?= ledgerline

.PHONY: help
help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

## --- Go ---------------------------------------------------------------------
.PHONY: build test test-short lint vuln generate run token
build: ## Build the binary into bin/
	go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/ledgerline ./cmd/ledgerline

test: ## Unit + integration tests (Postgres via testcontainers if Docker is up)
	go test -race -count=1 ./...

test-short: ## Unit tests only, no Docker
	go test -short -count=1 ./...

lint: ## golangci-lint
	golangci-lint run ./...

vuln: ## govulncheck
	govulncheck ./...

generate: ## Regenerate sqlc code from internal/store/queries
	sqlc generate

run: ## Run locally with the in-memory store and the fake provider
	LEDGERLINE_JWT_SECRET=dev-only-jwt-secret-change-me-0123456789 LEDGERLINE_PUBLIC_DEMO=true \
	LEDGERLINE_LOG_FORMAT=text go run ./cmd/ledgerline

token: ## Mint a dev token for tenant "demo" (matches `make run` / compose secret)
	@LEDGERLINE_JWT_SECRET=dev-only-jwt-secret-change-me-0123456789 go run ./cmd/ledgerline token -tenant demo

## --- Containers -------------------------------------------------------------
.PHONY: image up down smoke
image: ## Build the container image
	docker build -t $(IMAGE) --build-arg VERSION=$(VERSION) .

up: ## docker compose: API + Postgres + Jaeger
	docker compose up --build -d

down: ## Stop compose and delete its volume
	docker compose down -v

smoke: ## Smoke-test a running instance (BASE_URL, TOKEN, CHECK_BREAKER)
	scripts/smoke.sh

## --- Kubernetes (kind) ------------------------------------------------------
.PHONY: kind-up deploy e2e kind-down helm-lint
kind-up: ## Create kind cluster with Postgres and dev Vault (official chart)
	CLUSTER=$(CLUSTER) scripts/kind-up.sh

deploy: ## Build image, load into kind, helm upgrade --install, helm test
	CLUSTER=$(CLUSTER) IMAGE=$(IMAGE) scripts/deploy.sh

e2e: ## Smoke test through the in-cluster Service (incl. breaker + Vault checks)
	scripts/kind-e2e.sh

kind-down: ## Delete the kind cluster
	kind delete cluster --name $(CLUSTER)

helm-lint: ## helm lint + render the chart with kind values
	helm lint charts/ledgerline
	helm lint charts/ledgerline -f deploy/kind/values-kind.yaml
	helm template ledgerline charts/ledgerline -f deploy/kind/values-kind.yaml --kube-version 1.33.0 >/dev/null
