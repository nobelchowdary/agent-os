GO ?= go
IMAGE ?= agent-os:dev
export GOTOOLCHAIN = auto

.PHONY: help fmt fmt-check vet test test-python build smoke check image preview manifests

help:
	@printf '%s\n' 'make check      Check formatting, vet, Go/Python tests, build and preview' 'make build      Build bin/agentos' 'make image      Build the local API image (IMAGE=...)' 'make preview    Run a loopback-only Job preview API; set AGENTOS_LOCAL_TOKEN first' 'make manifests  Render Kubernetes manifests; does not apply them'

fmt:
	$(GO) fmt ./...

fmt-check:
	@test -z "$$(gofmt -l cmd internal)" || { echo 'Go formatting needs attention; run make fmt'; exit 1; }

vet:
	$(GO) vet ./...

test:
	$(GO) test -race ./...

test-python:
	python3 -m unittest discover -s scripts/tests -p 'test_*.py'

build:
	$(GO) build -trimpath -o bin/agentos ./cmd/agentos

smoke: build
	python3 scripts/smoke_preview.py

check: fmt-check vet test test-python smoke

image:
	docker build --tag $(IMAGE) .

preview:
	$(GO) run ./cmd/agentos --preview --listen=127.0.0.1:8080

manifests:
	kubectl kustomize deploy
