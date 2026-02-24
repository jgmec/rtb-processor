package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"rtb-processor/internal/config"
	"rtb-processor/internal/messaging"
	"rtb-processor/internal/processor"
	"rtb-processor/internal/repository"
	sftpclient "rtb-processor/internal/sftp"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting RTB Processor...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Printf("Config loaded — MaxWorkers=%d, BatchSize=%d, PollInterval=%ds",
		cfg.MaxWorkers, cfg.ClickHouseBatchSize, cfg.SFTPPollIntervalSeconds)

	chRepo, err := repository.NewClickHouseRepo(cfg)
	if err != nil {
		log.Fatalf("ClickHouse: %v", err)
	}
	defer chRepo.Close()

	publisher, err := messaging.NewRabbitMQPublisher(cfg)
	if err != nil {
		log.Fatalf("RabbitMQ: %v", err)
	}
	defer publisher.Close()

	sftp := sftpclient.NewClient(cfg)
	defer sftp.Close()

	zipProc := processor.NewZipProcessor(cfg, chRepo, publisher)
	poller := processor.NewPoller(cfg, sftp, zipProc)

	// ── Graceful shutdown ────────────────────────────────────────────────────
	// ctx is passed to poller.Run(). When cancelled, the poller stops accepting
	// new files but lets every already-running worker finish completely.
	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("Received %s — stopping new work, waiting for in-flight files...", sig)
		cancel() // tells the poller to stop queuing new files

		// A second signal forces an immediate exit (e.g. if workers are stuck).
		sig2 := <-sigCh
		log.Printf("Received second signal %s — forcing exit", sig2)
		os.Exit(1)
	}()
	// ────────────────────────────────────────────────────────────────────────

	// Blocks here until ctx is cancelled AND all in-flight workers are done.
	poller.Run(ctx)

	// defers (ClickHouse, RabbitMQ, SFTP close) run here — after all workers
	// have finished, so connections are still valid during the last writes.
	log.Println("RTB Processor stopped gracefully")
}
