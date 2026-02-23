# RTB Processor

A Go application that polls a remote SFTP folder for ZIP files, processes their JSON contents, and stores data in ClickHouse while publishing notifications to RabbitMQ.

## Architecture

```
SFTP Server → [Poll & Download] → [Worker Pool] → [ClickHouse] + [RabbitMQ]
```

### Flow
1. **Poller** periodically scans the SFTP remote directory for `.zip` files
2. Each new zip file is downloaded to the local directory
3. A **worker goroutine** (limited by `MAX_WORKERS`) processes each zip file
4. For each zip:
   - A record is inserted into `rtb_files` table (with UUID `file_id`)
   - Each JSON line from each file within the zip is batch-inserted into `rtb_data`
   - A message with the `file_id` is published to RabbitMQ
5. **Graceful shutdown** waits for in-progress files to complete before exiting

## ClickHouse Tables

```sql
-- Created automatically on startup
CREATE TABLE rtb_files (
    file_id     String,
    file_name   String,
    inserted_at DateTime
) ENGINE = MergeTree() ORDER BY (file_id, inserted_at);

CREATE TABLE rtb_data (
    file_id     String,
    rtb_data    String,   -- raw JSON as String
    inserted_at DateTime
) ENGINE = MergeTree() ORDER BY (file_id, inserted_at);
```

## RabbitMQ Message Format

Published to queue after each successful zip file:
```json
{"file_id": "550e8400-e29b-41d4-a716-446655440000"}
```

## Configuration (ENV Variables)

| Variable | Default | Description |
|---|---|---|
| `SFTP_HOST` | `localhost` | SFTP server hostname |
| `SFTP_PORT` | `22` | SFTP server port |
| `SFTP_USER` | `user` | SFTP username |
| `SFTP_PASSWORD` | `` | SFTP password |
| `SFTP_REMOTE_DIR` | `/upload` | Remote directory to poll |
| `SFTP_POLL_INTERVAL_SECONDS` | `30` | Polling interval in seconds |
| `LOCAL_DOWNLOAD_DIR` | `/tmp/rtb-downloads` | Local directory for downloaded zips |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse hostname |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native protocol port |
| `CLICKHOUSE_DB` | `default` | ClickHouse database name |
| `CLICKHOUSE_USER` | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | `` | ClickHouse password |
| `CLICKHOUSE_BATCH_SIZE` | `1000` | Number of JSON rows per batch insert |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `RABBITMQ_QUEUE` | `rtb_files` | RabbitMQ queue name |
| `MAX_WORKERS` | `4` | Max concurrent zip processing goroutines |

## Quick Start

### Using Docker Compose (recommended)

```bash
# Copy and edit env file
cp .env.example .env
# Edit .env with your SFTP/ClickHouse/RabbitMQ settings

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f rtb-processor

# Stop gracefully
docker-compose down
```

### Building manually

```bash
# Download dependencies
go mod download

# Build
go build -o rtb-processor ./cmd/main.go

# Run
export SFTP_HOST=my-sftp.example.com
export SFTP_USER=sftpuser
export SFTP_PASSWORD=secret
# ... set other env vars ...
./rtb-processor
```

### Build Docker image only

```bash
docker build -t rtb-processor:latest .
docker run --env-file .env rtb-processor:latest
```

## Key Features

- **SFTP Reconnect**: Automatic reconnection with exponential backoff when SFTP connection drops
- **RabbitMQ Reconnect**: Uses `github.com/AsidStorm/go-amqp-reconnect/rabbitmq` for automatic AMQP reconnection
- **Worker Pool**: `sync.WaitGroup` + semaphore channel limits concurrent goroutines to `MAX_WORKERS`
- **Batch Inserts**: JSON lines are buffered and inserted in configurable batches for ClickHouse efficiency
- **Graceful Shutdown**: `SIGTERM`/`SIGINT` cancels the context; in-progress file processing completes before exit; second signal forces exit
- **Idempotency**: Processed filenames are tracked in-memory to avoid reprocessing within a run
- **JSON Validation**: Each line is validated before insertion; invalid lines are skipped with a warning

## Project Structure

```
.
├── cmd/
│   └── main.go                  # Entry point, wiring, graceful shutdown
├── internal/
│   ├── config/
│   │   └── config.go            # ENV-based configuration
│   ├── sftp/
│   │   └── client.go            # SFTP client with reconnect logic
│   ├── repository/
│   │   └── clickhouse.go        # ClickHouse table creation + insert operations
│   ├── messaging/
│   │   └── rabbitmq.go          # RabbitMQ publisher with auto-reconnect
│   └── processor/
│       ├── zip_processor.go     # Zip file parsing + ClickHouse + RabbitMQ
│       └── poller.go            # SFTP polling + worker pool management
├── Dockerfile                   # Multi-stage Docker build
├── docker-compose.yml           # Full stack: app + ClickHouse + RabbitMQ
├── go.mod
└── .env.example
```
