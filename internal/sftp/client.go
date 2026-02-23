package sftp

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"rtb-processor/internal/config"
)

type Client struct {
	cfg        *config.Config
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	mu         sync.Mutex
}

func NewClient(cfg *config.Config) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) connect() error {
	sshConfig := &ssh.ClientConfig{
		User: c.cfg.SFTPUser,
		Auth: []ssh.AuthMethod{
			ssh.Password(c.cfg.SFTPPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // In production, use proper host key verification
		Timeout:         30 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", c.cfg.SFTPHost, c.cfg.SFTPPort)
	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to SSH: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}

	// Close old connections if any
	if c.sftpClient != nil {
		c.sftpClient.Close()
	}
	if c.sshClient != nil {
		c.sshClient.Close()
	}

	c.sshClient = sshClient
	c.sftpClient = sftpClient
	log.Println("SFTP connected successfully")
	return nil
}

func (c *Client) ensureConnected() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sftpClient != nil {
		// Test connection with a simple operation
		_, err := c.sftpClient.Getwd()
		if err == nil {
			return nil
		}
		log.Printf("SFTP connection lost, reconnecting: %v", err)
	}

	// Retry with backoff
	backoff := time.Second
	maxBackoff := 30 * time.Second
	for {
		err := c.connect()
		if err == nil {
			return nil
		}
		log.Printf("SFTP reconnect failed: %v, retrying in %s", err, backoff)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sftpClient != nil {
		c.sftpClient.Close()
	}
	if c.sshClient != nil {
		c.sshClient.Close()
	}
}

// ListZipFiles returns list of .zip files in the remote directory
func (c *Client) ListZipFiles() ([]string, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entries, err := c.sftpClient.ReadDir(c.cfg.SFTPRemoteDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read remote dir: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".zip") {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

// DownloadFile downloads a file from remote to local directory
func (c *Client) DownloadFile(remoteFilename string) (string, error) {
	if err := c.ensureConnected(); err != nil {
		return "", err
	}

	remotePath := filepath.Join(c.cfg.SFTPRemoteDir, remoteFilename)
	localPath := filepath.Join(c.cfg.LocalDownloadDir, remoteFilename)

	// Ensure local dir exists
	if err := os.MkdirAll(c.cfg.LocalDownloadDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create local dir: %w", err)
	}

	// Check if already downloaded
	if _, err := os.Stat(localPath); err == nil {
		return localPath, nil
	}

	c.mu.Lock()
	remoteFile, err := c.sftpClient.Open(remotePath)
	c.mu.Unlock()
	if err != nil {
		return "", fmt.Errorf("failed to open remote file %s: %w", remotePath, err)
	}
	defer remoteFile.Close()

	localFile, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	if _, err := io.Copy(localFile, remoteFile); err != nil {
		os.Remove(localPath)
		return "", fmt.Errorf("failed to download file: %w", err)
	}

	log.Printf("Downloaded: %s -> %s", remotePath, localPath)
	return localPath, nil
}
