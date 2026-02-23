FROM golang:1.26.0-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o /rtb-processor ./cmd/main.go

# -----------------------------------------------
FROM alpine:3.18

RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Create download directory
RUN mkdir -p /tmp/rtb-downloads && chown appuser:appgroup /tmp/rtb-downloads

COPY --from=builder /rtb-processor /usr/local/bin/rtb-processor

USER appuser

ENTRYPOINT ["/usr/local/bin/rtb-processor"]
