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
	cfg            *config.Config
	sftp           *sftpclient.Client
	zipProcessor   *ZipProcessor
	processedFiles map[string]struct{}
	mu             sync.Mutex

	// wg tracks ALL running worker goroutines across all poll cycles.
	// Run() waits on this before returning, ensuring no worker is
	// abandoned when the process exits.
	wg sync.WaitGroup

	// sem is a fixed-size semaphore shared across all poll cycles
	// so the MaxWorkers limit is respected globally, not per-cycle.
	sem chan struct{}
}

func NewPoller(cfg *config.Config, sftp *sftpclient.Client, zp *ZipProcessor) *Poller {
	return &Poller{
		cfg:            cfg,
		sftp:           sftp,
		zipProcessor:   zp,
		processedFiles: make(map[string]struct{}),
		sem:            make(chan struct{}, cfg.MaxWorkers),
	}
}

// Run polls the SFTP directory on a ticker and dispatches worker goroutines.
// It blocks until ctx is cancelled AND all in-flight workers have finished —
// this is what makes shutdown truly graceful.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.cfg.SFTPPollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("Poller started, polling every %ds", p.cfg.SFTPPollIntervalSeconds)

	// Poll immediately on startup.
	p.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Poller: shutdown signal received, waiting for in-flight workers...")
			// Block here until every in-progress zip file finishes.
			p.wg.Wait()
			log.Println("Poller: all workers done, exiting cleanly")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// poll lists remote zip files and dispatches a goroutine for each new file.
// Workers are tracked by the shared p.wg — this function does NOT wait for
// them itself. Waiting happens in Run() so that shutdown can join all workers.
func (p *Poller) poll(ctx context.Context) {
	// Never start new work once shutdown has been requested.
	select {
	case <-ctx.Done():
		return
	default:
	}

	files, err := p.sftp.ListZipFiles()
	if err != nil {
		log.Printf("Poller: failed to list remote files: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("Poller: no new zip files found")
		return
	}

	for _, filename := range files {
		// Skip files that are already processed or queued.
		p.mu.Lock()
		if _, ok := p.processedFiles[filename]; ok {
			p.mu.Unlock()
			continue
		}
		p.processedFiles[filename] = struct{}{}
		p.mu.Unlock()

		// Stop queuing new files if shutdown was requested between iterations.
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.wg.Add(1)
		go func(fname string) {
			defer p.wg.Done()

			// Acquire a worker slot. We do NOT select on ctx here because
			// graceful shutdown means "finish what you queued, don't start
			// brand-new work" — blocking for a slot is fine.
			p.sem <- struct{}{}
			defer func() { <-p.sem }()

			// Use context.Background() so that the shutdown signal does NOT
			// abort a zip file mid-processing. The file will complete, publish
			// to RabbitMQ, and only then will the slot be released.
			if err := p.processFile(context.Background(), fname); err != nil {
				log.Printf("Poller: error processing %s: %v", fname, err)
				// Allow retry on next run.
				p.mu.Lock()
				delete(p.processedFiles, fname)
				p.mu.Unlock()
			}
		}(filename)
	}
}

func (p *Poller) processFile(ctx context.Context, filename string) error {
	localPath, err := p.sftp.DownloadFile(filename)
	if err != nil {
		return err
	}
	return p.zipProcessor.ProcessZipFile(ctx, localPath)
}
