BINARY_NAME=treds
CLI_BINARY_NAME=treds-cli

# Use GOARCH and GOOS from environment, default to amd64 and the current OS.
GOARCH ?= amd64
GOOS ?= $(shell go env GOOS)

# Default build for current OS, using GOARCH and GOOS from env
build:
	GOARCH=$(GOARCH) GOOS=$(GOOS) go build $(filter-out $@,$(MAKECMDGOALS)) -o ${BINARY_NAME}

# Build the cli in the client folder
build-cli:
	GOARCH=$(GOARCH) GOOS=$(GOOS) go build $(filter-out $@,$(MAKECMDGOALS)) -o ${CLI_BINARY_NAME} ./client

# Run the default binary for the current OS
run: build
	./${BINARY_NAME} $(filter-out $@,$(MAKECMDGOALS))

# Run the cli binary
run-cli: build-cli
	./${CLI_BINARY_NAME} $(filter-out $@,$(MAKECMDGOALS))

# Clean up binaries
clean:
	go clean
	rm -f ${BINARY_NAME}
	rm -f ${CLI_BINARY_NAME}

# Run tests
test:
	go test $(filter-out $@,$(MAKECMDGOALS)) ./...

# Run tests with coverage
test_coverage:
	go test $(filter-out $@,$(MAKECMDGOALS)) ./... -coverprofile=coverage.out

# Install dependencies
dep:
	go mod download

# Vet the code
vet:
	go vet

# Run linting
lint:
	golangci-lint run --enable-all
