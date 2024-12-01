# Step 1: Build the Treds binary for the target platforms
FROM --platform=$BUILDPLATFORM golang:1.22 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and Makefile
COPY go.mod go.sum Makefile ./

# Download Go modules
RUN go mod download

# Copy the rest of the project files
COPY . .

# Build the binary for the target platform
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o treds ./cmd/treds

# Step 2: Use a smaller image and copy only the Treds binary
FROM --platform=$TARGETPLATFORM alpine:latest

# Set working directory in the final image
WORKDIR /root/

# Install minimal dependencies for runtime
RUN apk --no-cache add ca-certificates

# Copy the binary from the builder stage
COPY --from=builder /app/treds .

# Ensure the binary is executable
RUN chmod +x ./treds

# Run the binary
CMD ["./treds"]
