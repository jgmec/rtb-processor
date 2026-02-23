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
	return &ZipProcessor{
		cfg:       cfg,
		chRepo:    chRepo,
		publisher: publisher,
	}
}

// ProcessZipFile processes a single zip file:
// 1. Insert record into rtb_files
// 2. Read each JSON line from each file in the zip and batch insert into rtb_data
// 3. Publish message to RabbitMQ
func (p *ZipProcessor) ProcessZipFile(ctx context.Context, filePath string) error {
	fileName := filepath.Base(filePath)
	fileID := uuid.New().String()
	startTime := time.Now()

	log.Printf("[%s] Starting processing of %s", fileID, fileName)

	// Insert into rtb_files
	if err := p.chRepo.InsertFile(ctx, fileID, fileName); err != nil {
		return fmt.Errorf("failed to insert file record: %w", err)
	}

	// Open the zip
	reader, err := zip.OpenReader(filePath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer reader.Close()

	totalLines := 0

	// Process each file in the zip
	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}

		linesProcessed, err := p.processZipEntry(ctx, fileID, f)
		if err != nil {
			log.Printf("[%s] Error processing entry %s: %v", fileID, f.Name, err)
			continue
		}
		totalLines += linesProcessed
	}

	// Publish to RabbitMQ
	if err := p.publisher.PublishFileProcessed(fileID); err != nil {
		// Log but don't fail — file was processed successfully
		log.Printf("[%s] Warning: failed to publish RabbitMQ message: %v", fileID, err)
	}

	log.Printf("[%s] Finished processing %s: %d lines in %s",
		fileID, fileName, totalLines, time.Since(startTime))

	return nil
}

func (p *ZipProcessor) processZipEntry(ctx context.Context, fileID string, f *zip.File) (int, error) {
	rc, err := f.Open()
	if err != nil {
		return 0, fmt.Errorf("failed to open zip entry %s: %w", f.Name, err)
	}
	defer rc.Close()

	decoder := json.NewDecoder(rc)
	_ = decoder // We use a scanner for line-by-line reading to handle multiple JSON objects per file

	// Re-open for scanner (json.NewDecoder reads ahead, making it tricky for line scanning)
	// Use bufio.Scanner for line-by-line reading, using the decoder for validation
	rc.Close()
	rc2, err := f.Open()
	if err != nil {
		return 0, fmt.Errorf("failed to re-open zip entry %s: %w", f.Name, err)
	}
	defer rc2.Close()

	scanner := bufio.NewScanner(rc2)
	// Increase buffer size for large JSON lines
	const maxScannerBuffer = 10 * 1024 * 1024 // 10MB
	scanner.Buffer(make([]byte, maxScannerBuffer), maxScannerBuffer)

	batch := make([]string, 0, p.cfg.ClickHouseBatchSize)
	totalLines := 0

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := p.chRepo.InsertRtbDataBatch(ctx, fileID, batch); err != nil {
			return fmt.Errorf("failed to insert batch: %w", err)
		}
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Validate JSON
		if !json.Valid([]byte(line)) {
			log.Printf("[%s] Skipping invalid JSON line in %s", fileID, f.Name)
			continue
		}

		batch = append(batch, line)
		totalLines++

		if len(batch) >= p.cfg.ClickHouseBatchSize {
			if err := flushBatch(); err != nil {
				return totalLines, err
			}
		}

		// Check for context cancellation
		select {
		case <-ctx.Done():
			// Flush remaining and return
			_ = flushBatch()
			return totalLines, ctx.Err()
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		return totalLines, fmt.Errorf("scanner error in %s: %w", f.Name, err)
	}

	// Flush remaining
	if err := flushBatch(); err != nil {
		return totalLines, err
	}

	return totalLines, nil
}
