package processor

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"rtb-processor/internal/config"
	"rtb-processor/internal/messaging"
	"rtb-processor/internal/repository"

	"github.com/google/uuid"
)

type ZipProcessor struct {
	cfg       *config.Config
	chRepo    *repository.ClickHouseRepo
	publisher *messaging.RabbitMQPublisher
}

func NewZipProcessor(
	cfg *config.Config,
	chRepo *repository.ClickHouseRepo,
	publisher *messaging.RabbitMQPublisher,
) *ZipProcessor {
	return &ZipProcessor{cfg: cfg, chRepo: chRepo, publisher: publisher}
}

// ProcessZipFile fully processes one zip file:
//  1. Insert a record into rtb_files
//  2. For every JSON-lines file inside the zip, batch-insert rows into rtb_data
//  3. Publish a message to RabbitMQ
//
// The function always runs to completion — it is the caller's responsibility
// (the Poller) to decide whether to start new work after a shutdown signal.
func (p *ZipProcessor) ProcessZipFile(ctx context.Context, filePath string) error {
	fileName := filepath.Base(filePath)
	fileID := uuid.New().String()
	startTime := time.Now()

	log.Printf("[%s] Starting processing: %s", fileID, fileName)

	// 1. Record the file in rtb_files.
	if err := p.chRepo.InsertFile(ctx, fileID, fileName); err != nil {
		return fmt.Errorf("insert rtb_files: %w", err)
	}

	// 2. Open the zip and process every entry.
	reader, err := zip.OpenReader(filePath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer reader.Close()

	totalLines := 0
	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}
		n, err := p.processZipEntry(ctx, fileID, f)
		if err != nil {
			// Log and continue — don't abort the whole zip for one bad entry.
			log.Printf("[%s] Error in entry %s: %v", fileID, f.Name, err)
		}
		totalLines += n
	}

	// 3. Publish to RabbitMQ.
	if err := p.publisher.PublishFileProcessed(fileID); err != nil {
		// Non-fatal — the data is already in ClickHouse.
		log.Printf("[%s] Warning: RabbitMQ publish failed: %v", fileID, err)
	}

	log.Printf("[%s] Done: %s — %d lines in %s", fileID, fileName, totalLines, time.Since(startTime))
	return nil
}

// processZipEntry reads all JSON lines from a single entry inside the zip,
// buffering them into batches before writing to ClickHouse.
func (p *ZipProcessor) processZipEntry(ctx context.Context, fileID string, f *zip.File) (int, error) {
	rc, err := f.Open()
	if err != nil {
		return 0, fmt.Errorf("open zip entry %s: %w", f.Name, err)
	}
	defer rc.Close()

	// Use a generous scanner buffer for large JSON objects.
	const maxLine = 10 * 1024 * 1024 // 10 MB
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, maxLine), maxLine)

	batch := make([]string, 0, p.cfg.ClickHouseBatchSize)
	totalLines := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := p.chRepo.InsertRtbDataBatch(ctx, fileID, batch); err != nil {
			return fmt.Errorf("batch insert: %w", err)
		}
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !json.Valid([]byte(line)) {
			log.Printf("[%s] Skipping invalid JSON line in %s", fileID, f.Name)
			continue
		}

		batch = append(batch, line)
		totalLines++

		if len(batch) >= p.cfg.ClickHouseBatchSize {
			if err := flush(); err != nil {
				return totalLines, err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return totalLines, fmt.Errorf("scanner error in %s: %w", f.Name, err)
	}

	return totalLines, flush()
}
