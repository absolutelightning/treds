BINARY_NAME=treds
CLI_BINARY_NAME=treds-cli

# Default build for current OS (no GOOS specified)
build:
	go build -o ${BINARY_NAME}

# Build for Darwin (macOS)
build-darwin:
	GOARCH=amd64 GOOS=darwin go build -o ${BINARY_NAME}-darwin

# Build for Linux
build-linux:
	GOARCH=amd64 GOOS=linux go build -o ${BINARY_NAME}-linux

# Build for Windows
build-windows:
	GOARCH=amd64 GOOS=windows go build -o ${BINARY_NAME}-windows

# Build for all platforms
build_all: build-darwin build-linux build-windows

# Run the default binary for the current OS
run: build
	./${BINARY_NAME}

# Build the client in the client folder
build-cli:
	go build -o ${CLI_BINARY_NAME} ./client

# Run the client binary
run-cli: build-cli
	./${CLI_BINARY_NAME}

# Clean up binaries
clean:
	go clean
	rm -f ${BINARY_NAME}
	rm -f ${BINARY_NAME}-darwin
	rm -f ${BINARY_NAME}-linux
	rm -f ${BINARY_NAME}-windows
	rm -f ${CLI_BINARY_NAME}

# Run tests
test:
	go test ./...

# Run tests with coverage
test_coverage:
	go test ./... -coverprofile=coverage.out

# Install dependencies
dep:
	go mod download

# Vet the code
vet:
	go vet

# Run linting
lint:
	golangci-lint run --enable-all
