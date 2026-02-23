package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"rtb-processor/internal/config"
)

type ClickHouseRepo struct {
	conn driver.Conn
	cfg  *config.Config
}

type RtbFile struct {
	FileID     string
	FileName   string
	InsertedAt time.Time
}

type RtbData struct {
	FileID     string
	RtbData    string
	InsertedAt time.Time
}

func NewClickHouseRepo(cfg *config.Config) (*ClickHouseRepo, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		DialTimeout:     30 * time.Second,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	repo := &ClickHouseRepo{conn: conn, cfg: cfg}
	if err := repo.createTables(context.Background()); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *ClickHouseRepo) createTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS rtb_files (
			file_id     String,
			file_name   String,
			inserted_at DateTime
		) ENGINE = MergeTree()
		ORDER BY (file_id, inserted_at)`,

		`CREATE TABLE IF NOT EXISTS rtb_data (
			file_id     String,
			rtb_data    String,
			inserted_at DateTime
		) ENGINE = MergeTree()
		ORDER BY (file_id, inserted_at)`,
	}

	for _, q := range queries {
		if err := r.conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	log.Println("ClickHouse tables verified/created")
	return nil
}

// InsertFile inserts a record into rtb_files and returns the file_id
func (r *ClickHouseRepo) InsertFile(ctx context.Context, fileID, fileName string) error {
	return r.conn.Exec(ctx,
		`INSERT INTO rtb_files (file_id, file_name, inserted_at) VALUES (?, ?, ?)`,
		fileID, fileName, time.Now(),
	)
}

// InsertRtbDataBatch inserts a batch of rtb_data records
func (r *ClickHouseRepo) InsertRtbDataBatch(ctx context.Context, fileID string, jsonLines []string) error {
	if len(jsonLines) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, `INSERT INTO rtb_data (file_id, rtb_data, inserted_at)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, line := range jsonLines {
		if err := batch.Append(fileID, line, now); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

func (r *ClickHouseRepo) Close() error {
	return r.conn.Close()
}
