package processor

import (
	"context"
	"log"
	"sync"
	"time"

	"rtb-processor/internal/config"
	sftpclient "rtb-processor/internal/sftp"
)

type Poller struct {
	cfg          *config.Config
	sftp         *sftpclient.Client
	zipProcessor *ZipProcessor
	processedFiles map[string]struct{}
	mu           sync.Mutex
}

func NewPoller(cfg *config.Config, sftp *sftpclient.Client, zp *ZipProcessor) *Poller {
	return &Poller{
		cfg:          cfg,
		sftp:         sftp,
		zipProcessor: zp,
		processedFiles: make(map[string]struct{}),
	}
}

func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.cfg.SFTPPollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("Poller started, polling every %ds", p.cfg.SFTPPollIntervalSeconds)

	// Run immediately on start
	p.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Poller shutting down")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *Poller) poll(ctx context.Context) {
	files, err := p.sftp.ListZipFiles()
	if err != nil {
		log.Printf("Failed to list remote files: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("No zip files found in remote directory")
		return
	}

	// Semaphore for limiting concurrent workers
	sem := make(chan struct{}, p.cfg.MaxWorkers)
	var wg sync.WaitGroup

	for _, filename := range files {
		// Check if already processed
		p.mu.Lock()
		if _, ok := p.processedFiles[filename]; ok {
			p.mu.Unlock()
			continue
		}
		// Mark as in-progress
		p.processedFiles[filename] = struct{}{}
		p.mu.Unlock()

		// Check context before spawning goroutine
		select {
		case <-ctx.Done():
			return
		default:
		}

		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(fname string) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			if err := p.processFile(ctx, fname); err != nil {
				log.Printf("Error processing %s: %v", fname, err)
				// Remove from processed so it will be retried next poll
				p.mu.Lock()
				delete(p.processedFiles, fname)
				p.mu.Unlock()
			}
		}(filename)
	}

	wg.Wait()
}

func (p *Poller) processFile(ctx context.Context, filename string) error {
	localPath, err := p.sftp.DownloadFile(filename)
	if err != nil {
		return err
	}

	return p.zipProcessor.ProcessZipFile(ctx, localPath)
}
