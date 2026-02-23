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

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	log.Printf("Configuration loaded. MaxWorkers=%d, BatchSize=%d", cfg.MaxWorkers, cfg.ClickHouseBatchSize)

	// Setup ClickHouse
	chRepo, err := repository.NewClickHouseRepo(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer chRepo.Close()
	log.Println("ClickHouse connected")

	// Setup RabbitMQ
	publisher, err := messaging.NewRabbitMQPublisher(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer publisher.Close()
	log.Println("RabbitMQ connected")

	// Setup SFTP client
	sftp := sftpclient.NewClient(cfg)
	defer sftp.Close()

	// Setup processors
	zipProc := processor.NewZipProcessor(cfg, chRepo, publisher)
	poller := processor.NewPoller(cfg, sftp, zipProc)

	// Graceful shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("Received signal %s, initiating graceful shutdown...", sig)
		cancel()
		// Wait for a second signal to force quit
		sig2 := <-sigCh
		log.Printf("Received second signal %s, forcing exit", sig2)
		os.Exit(1)
	}()

	// Run the poller (blocks until context is cancelled)
	poller.Run(ctx)

	log.Println("RTB Processor stopped gracefully")
}
