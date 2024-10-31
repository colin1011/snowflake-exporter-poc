# Stage 1: Build the application
FROM golang:1.21-alpine AS builder

# Install git and CA certificates
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go module files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
# CGO_ENABLED=0 creates a statically linked binary
# -o specifies the output binary name
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o snowflake_exporter \
    .

# Stage 2: Create minimal runtime image
FROM alpine:latest

# Install CA certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Create a non-root user
RUN addgroup -S exporter && adduser -S exporter -G exporter

# Set working directory
WORKDIR /home/exporter

# Copy the compiled binary from the builder stage
COPY --from=builder /app/snowflake_exporter .

# Change ownership to non-root user
RUN chown -R exporter:exporter /home/exporter

# Switch to non-root user
USER exporter

# Expose the metrics port
EXPOSE 9090

# Default environment variables (can be overridden)
ENV EXPORTER_PORT=9090

# Health check to verify the application is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s \
  CMD wget -q -O- http://localhost:${EXPORTER_PORT}/metrics || exit 1

# Command to run the exporter
ENTRYPOINT ["./snowflake_exporter"]