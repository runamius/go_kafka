package service

import (
	"context"
	"encoding/json"
	"fmt"
	"pizza-shop/config"
	"pizza-shop/logger"
	"time"

	"github.com/segmentio/kafka-go"
)

type IMessagePublisher interface {
	PublishEvent(topicName string, body interface{}) error
	Close() error
}

type KafkaMessagePublisher struct {
	conn        *config.KafkaConnection
	kafkaWriter *kafka.Writer
}

func (k *KafkaMessagePublisher) PublishEvent(topicName string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal message body %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := []byte(fmt.Sprintf("key.%d", time.Now().UnixMilli()))
	message := kafka.Message{
		Topic: topicName,
		Key:   key,
		Value: data,
	}

	err = k.kafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to send message to kafka topic %s: %w", topicName, err)
	}

	logger.Log(fmt.Sprintf("message has been published to kafka topic %s, key: %s",
		topicName, string(key)))
	return nil
}

func GetKafkaMessagePublisher(topic string) *KafkaMessagePublisher {
	conn := config.GetNewKafkaConnection(topic, "")

	return &KafkaMessagePublisher{
		conn:        conn,
		kafkaWriter: conn.GetWriter(),
	}
}

func (k *KafkaMessagePublisher) Close() error {
	if k.kafkaWriter != nil {
		return k.kafkaWriter.Close()
	}
	return nil
}
