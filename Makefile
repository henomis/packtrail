.PHONY: all test lint fmt vet clean coverage help license tidy fix doc check ui build-ui modernize

# Variables
GO := go
GOFLAGS := -v
MODULE := $(shell $(GO) list -m)
PKGS := $(shell $(GO) list ./... | grep -v /examples/)
GOLANGCI_LINT := golangci-lint
BIN_DIR := bin

# Default target
all: test lint

## help: Display this help message
help:
	@echo "Available targets:"
	@echo "  make test        - Run all tests"
	@echo "  make lint        - Run golangci-lint"
	@echo "  make fmt         - Format all Go files"
	@echo "  make fix         - Run go fix on all packages"
	@echo "  make vet         - Run go vet"
	@echo "  make tidy        - Tidy and verify go modules"
	@echo "  make coverage    - Generate test coverage report"
	@echo "  make ui          - Run the packtrail-ui dashboard"
	@echo "  make build-ui    - Build the packtrail-ui binary into bin/"
	@echo "  make clean       - Clean build artifacts and cache"
	@echo "  make license     - Add license headers to all Go files"
	@echo "  make modernize   - Apply Go modernization fixes (go tool modernize)"
	@echo "  make help        - Display this help message"

## test: Run all tests (excluding examples)
test:
	@echo "Running tests..."
	$(GO) test $(GOFLAGS) -race -timeout 5m $(PKGS)

## coverage: Generate test coverage report
coverage:
	@echo "Generating coverage report..."
	$(GO) test -race -timeout 5m -coverprofile=coverage.out -covermode=atomic $(PKGS)
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

## ui: Run the packtrail-ui dashboard
ui:
	@echo "Starting packtrail-ui on :8088 (NATS_URL defaults to nats://localhost:4222)..."
	$(GO) run ./cmd/packtrail-ui --namespace packtrail --addr :8088

## build-ui: Build the packtrail-ui binary (static assets are embedded; honors GOOS/GOARCH)
build-ui:
	@echo "Building $(BIN_DIR)/packtrail-ui..."
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w" -o $(BIN_DIR)/packtrail-ui ./cmd/packtrail-ui

## lint: Run golangci-lint
lint:
	@echo "Running linters..."
	$(GOLANGCI_LINT) run ./...

## fmt: Format all Go files
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...
	gofumpt -l -w .
	gci write --skip-generated -s standard -s default -s "prefix($(MODULE))" .

## fix: Run go fix on all packages
fix:
	@echo "Running go fix..."
	$(GO) fix ./...

## vet: Run go vet
vet:
	@echo "Running go vet..."
	$(GO) vet ./...

## tidy: Tidy go modules
tidy:
	@echo "Tidying go modules..."
	$(GO) mod tidy
	$(GO) mod verify

## clean: Clean build artifacts and cache
clean:
	@echo "Cleaning..."
	rm -f coverage.out coverage.html
	rm -rf $(BIN_DIR)

## license: Add Apache license headers to all Go files
license:
	@echo "Adding license headers..."
	find . -type f -name '*.go' -exec addlicense -c "Simone Vellei" -l apache {} +

## doc: Generate Go documentation in HTML format
doc:
	@echo "Generating Go documentation..."
	@echo "Open http://localhost:6060/pkg/github.com/henomis/packtrail/ in your browser. Press Ctrl+C to stop."
	godoc -http=:6060

## modernize: Apply Go modernization fixes
modernize:
	@echo "Running go modernize..."
	$(GO) tool modernize -fix ./...

## check: Run all checks (test + lint + vet)
check: test lint vet
	@echo "All checks passed!"
