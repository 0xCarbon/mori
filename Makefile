# Mori developer gates. `make ci` is the single local gate; CI runs the same
# contract. Every build, vet and test runs with CGO_ENABLED=0 except `race`:
# the race detector requires cgo, and it is the one documented exception.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := ci

export CGO_ENABLED := 0
export GOTOOLCHAIN := local
export GOFLAGS := -mod=readonly
GO ?= go
GOLANGCI_LINT ?= golangci-lint
GO_MODERNIZE ?= go-modernize

# Maintained Go sources. project/evidence holds frozen nested modules whose
# bytes are receipts and are never reformatted.
GO_FILES = $(shell find . -name '*.go' -not -path './.git/*' -not -path './project/evidence/*' -not -path './dist/*')

# Cross targets are compiled and vetted; tests execute on the host only.
CROSS_TARGETS := linux/386 linux/arm64 darwin/arm64 windows/amd64 freebsd/amd64

.PHONY: bench-smoke ci check fmt fmt-check fix modernize-check vet lint build cross check-deps test race integ subnet cov bench fuzz-smoke modernize tidy-check help

help:
	@echo "make ci           all gates: fmt, modernize, vet, lint, build, cross, deps, tidy, test, race, bench-smoke"
	@echo "make check        fast static gates (no tests)"
	@echo "make test         full suite, CGO_ENABLED=0"
	@echo "make race         full suite under the race detector (cgo exception)"
	@echo "make bench        benchmarks into dist/bench.txt (compare with go run ./tools/benchcmp)"
	@echo "make fuzz-smoke   short fuzz run of every fuzz target"
	@echo "make modernize    optional: golang-modernization skill gate (go-modernize on PATH)"
	@echo "make fix          apply go fix modernizations and gofmt"

ci: check tidy-check test race bench-smoke

check: fmt-check modernize-check vet lint build cross check-deps

fmt:
	gofmt -s -w $(GO_FILES)

fmt-check:
	@out="$$(gofmt -s -l $(GO_FILES))"; if [ -n "$$out" ]; then echo "Unformatted Go files (run make fmt):"; echo "$$out"; exit 1; fi

fix:
	$(GO) fix ./...
	gofmt -s -w $(GO_FILES)

modernize-check:
	@diff="$$($(GO) fix -diff ./...)"; if [ -n "$$diff" ]; then echo "Modernization findings (review, then run make fix):"; echo "$$diff"; exit 1; fi

vet:
	$(GO) vet ./...

lint:
	@command -v $(GOLANGCI_LINT) >/dev/null || { echo "ERROR: $(GOLANGCI_LINT) not found; install golangci-lint v2 built with Go 1.27+ or set GOLANGCI_LINT"; exit 1; }
	$(GOLANGCI_LINT) run --config .golangci.yml ./...

build:
	$(GO) build ./...

cross:
	@for t in $(CROSS_TARGETS); do \
		echo "==> $$t"; \
		GOOS=$${t%/*} GOARCH=$${t#*/} $(GO) build ./... ; \
		GOOS=$${t%/*} GOARCH=$${t#*/} $(GO) vet ./... ; \
	done

check-deps:
	$(GO) run ./tools/checkdeps

tidy-check:
	GOFLAGS= $(GO) mod tidy -diff

test:
	$(GO) test -count=1 ./...

race:
	CGO_ENABLED=1 $(GO) test -race -count=1 ./...

# Integration tests that need a real network (see integ_test.go).
integ: subnet
	INTEG_TESTS=yes $(GO) test -count=1 ./...

# On macOS the tests need extra loopback addresses; run this once first.
subnet:
	@sh -c "'$(CURDIR)/test/setup_subnet.sh'"

cov:
	@mkdir -p dist
	$(GO) test -count=1 -coverprofile=dist/coverage.out ./...
	$(GO) tool cover -func=dist/coverage.out | tail -1

# Benchmarks are Tier-1 smoke unless run interleaved on a quiet host; see
# project/goal.md for the evidence rules.
BENCH ?= .
BENCH_COUNT ?= 10
# Every benchmark once, so they cannot rot between measurement campaigns.
bench-smoke:
	$(GO) test -run '^$$' -bench . -benchtime 1x ./... > /dev/null

bench:
	@mkdir -p dist
	$(GO) test -run '^$$' -bench '$(BENCH)' -benchmem -count=$(BENCH_COUNT) ./... | tee dist/bench.txt

FUZZ_TIME ?= 10s
fuzz-smoke:
	@targets="$$($(GO) test -list '^Fuzz' . | grep '^Fuzz' || true)"; \
	if [ -z "$$targets" ]; then echo "ERROR: no fuzz targets found"; exit 1; fi; \
	for t in $$targets; do \
		echo "==> $$t ($(FUZZ_TIME))"; \
		$(GO) test -run '^$$' -fuzz "^$$t$$" -fuzztime $(FUZZ_TIME) . ; \
	done

# Optional local gate from the golang-modernization skill: go fix plus
# golangci-lint v2, reported as findings without applying fixes.
modernize:
	$(GO_MODERNIZE) --golangci-lint $(GOLANGCI_LINT) --config .golangci.yml .
