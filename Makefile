.PHONY: all build run dev peer clean test test-integration bench fmt vet tidy deps redis-up redis-down help

BINARY_NAME := cyphertrap
MAIN_PATH   := ./cmd/server
BIN_DIR     := bin
BINARY      := $(BIN_DIR)/$(BINARY_NAME)

GO := go
PORT ?= 7878

all: build

build: $(BINARY)

$(BINARY):
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BINARY) $(MAIN_PATH)

run: build
	$(BINARY)

dev:
	$(GO) run $(MAIN_PATH)

peer:
	telnet localhost $(PORT)

test:
	$(GO) test -v -race -count=1 ./...

test-integration:
	$(GO) test -v -race -count=1 -tags=integration ./...

bench:
	$(GO) test -bench=. -benchmem ./...

clean:
	rm -rf $(BIN_DIR)
	$(GO) clean

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

deps:
	$(GO) mod download

redis-up:
	docker run -d --name cyphertrap-redis -p 6379:6379 redis:alpine

redis-down:
	docker rm -f cyphertrap-redis

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build             Build the server binary ($(BINARY))"
	@echo "  run               Build and run the server (requires .env)"
	@echo "  dev               Run the server with go run (requires .env)"
	@echo "  peer              Connect to the server via telnet (PORT=$(PORT))"
	@echo "  test              Run unit tests"
	@echo "  test-integration  Run integration tests (requires Redis)"
	@echo "  bench             Run benchmarks"
	@echo "  clean             Remove build artifacts"
	@echo "  fmt               Format Go source"
	@echo "  vet               Run go vet"
	@echo "  tidy              Tidy go.mod and go.sum"
	@echo "  deps              Download module dependencies"
	@echo "  redis-up          Start a local Redis container for development"
	@echo "  redis-down        Stop and remove the local Redis container"
	@echo "  help              Show this help message"
