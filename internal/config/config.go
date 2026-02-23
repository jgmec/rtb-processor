package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	// SFTP settings
	SFTPHost       string
	SFTPPort       int
	SFTPUser       string
	SFTPPassword   string
	SFTPRemoteDir  string
	SFTPPollIntervalSeconds int

	// Local storage
	LocalDownloadDir string

	// ClickHouse settings
	ClickHouseHost     string
	ClickHousePort     int
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseBatchSize int

	// RabbitMQ settings
	RabbitMQURL   string
	RabbitMQQueue string

	// Processing settings
	MaxWorkers int
}

func Load() (*Config, error) {
	cfg := &Config{
		SFTPHost:                getEnv("SFTP_HOST", "localhost"),
		SFTPPort:                getEnvInt("SFTP_PORT", 22),
		SFTPUser:                getEnv("SFTP_USER", "user"),
		SFTPPassword:            getEnv("SFTP_PASSWORD", ""),
		SFTPRemoteDir:           getEnv("SFTP_REMOTE_DIR", "/upload"),
		SFTPPollIntervalSeconds: getEnvInt("SFTP_POLL_INTERVAL_SECONDS", 30),

		LocalDownloadDir: getEnv("LOCAL_DOWNLOAD_DIR", "/tmp/rtb-downloads"),

		ClickHouseHost:      getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickHousePort:      getEnvInt("CLICKHOUSE_PORT", 9000),
		ClickHouseDB:        getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser:      getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePassword:  getEnv("CLICKHOUSE_PASSWORD", ""),
		ClickHouseBatchSize: getEnvInt("CLICKHOUSE_BATCH_SIZE", 1000),

		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQQueue: getEnv("RABBITMQ_QUEUE", "rtb_files"),

		MaxWorkers: getEnvInt("MAX_WORKERS", 4),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.SFTPHost == "" {
		return fmt.Errorf("SFTP_HOST is required")
	}
	if c.SFTPUser == "" {
		return fmt.Errorf("SFTP_USER is required")
	}
	if c.RabbitMQURL == "" {
		return fmt.Errorf("RABBITMQ_URL is required")
	}
	return nil
}

func (c *Config) ClickHouseDSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		c.ClickHouseUser,
		c.ClickHousePassword,
		c.ClickHouseHost,
		c.ClickHousePort,
		c.ClickHouseDB,
	)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
