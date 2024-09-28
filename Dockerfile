# Step 1: Build the Treds binary using Makefile
FROM golang:1.22 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and Makefile
COPY go.mod go.sum Makefile ./

# Download Go modules
RUN go mod download

# Copy the rest of the project files
COPY . .

# Run `make build` to build the binary
RUN make build

# Step 2: Use a smaller image and copy only the Treds binary
FROM alpine:latest

# Set working directory in the final image
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/treds .

# Ensure the binary is executable (if needed)
RUN chmod +x ./treds

# Run the binary
CMD ["./treds"]

