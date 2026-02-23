package messaging

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"

	"rtb-processor/internal/config"
)

type RabbitMQPublisher struct {
	conn      *rabbitmq.Connection
	channel   *rabbitmq.Channel
	queueName string
}

type FileMessage struct {
	FileID string `json:"file_id"`
}

func NewRabbitMQPublisher(cfg *config.Config) (*RabbitMQPublisher, error) {
	conn, err := rabbitmq.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		cfg.RabbitMQQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	log.Printf("RabbitMQ connected, queue '%s' declared", cfg.RabbitMQQueue)

	return &RabbitMQPublisher{
		conn:      conn,
		channel:   ch,
		queueName: cfg.RabbitMQQueue,
	}, nil
}

func (p *RabbitMQPublisher) PublishFileProcessed(fileID string) error {
	msg := FileMessage{FileID: fileID}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	err = p.channel.Publish(
		"",          // exchange
		p.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		rabbitmq.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("Published message for file_id: %s", fileID)
	return nil
}

func (p *RabbitMQPublisher) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}
