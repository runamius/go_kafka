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

type IMessageConsumer interface {
	ConsumeMessage() (Message, error)
	GetReader() *kafka.Reader
	Close() error
}

type Message struct {
	Data         map[string]interface{}
	KafkaMessage kafka.Message
	Topic        string
}

type KafkaMessageConsumer struct {
	conn   *config.KafkaConnection
	Reader *kafka.Reader
}

func (k *KafkaMessageConsumer) ConsumeMessage() (Message, error) {
	var data map[string]interface{}
	var event = Message{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := k.Reader.ReadMessage(ctx)
	if err != nil {
		return event, fmt.Errorf("failed to read message from kafka %v", err)
	}

	err = json.Unmarshal(msg.Value, &data)
	if err != nil {
		return event, fmt.Errorf("parse message error %v", err)
	}
	event.Data = data
	event.KafkaMessage = msg
	return event, nil
}

func (k *KafkaMessageConsumer) GetReader() *kafka.Reader {
	return k.Reader
}

func (k *KafkaMessageConsumer) Close() error {
	err := k.Reader.Close()
	if err != nil {
		logger.Log(fmt.Sprintf("error closing Kafka reader %v", err))
	}
	logger.Log(fmt.Sprintf("kafka reader closed %v", err))
	return nil
}

func GetNewKafkaConsumer(topic string, groupId string) *KafkaMessageConsumer {
	conn := config.GetNewKafkaConnection(topic, groupId)
	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   topic,
			GroupID: groupId,
		},
	)
	return &KafkaMessageConsumer{
		conn:   conn,
		Reader: reader,
	}
}
