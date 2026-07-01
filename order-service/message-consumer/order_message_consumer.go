package messageconsumer

import (
	"context"
	"fmt"
	"pizza-shop/logger"
	"pizza-shop/repository"
	"pizza-shop/service"
)

type OrderMessageConsumer struct {
	consumer             service.IMessageConsumer
	orderConsumerChannel chan service.Message
	workerCount          int
	repositories         repository.Repositories
}

func (omc *OrderMessageConsumer) StartConsuming() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range omc.workerCount {
		go omc.registerConsumerWorker(i, ctx)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Log("stopped message consumption")
			return
		default:
			message, err := omc.consumer.ConsumeMessage()
			if err != nil {
				continue
			}

			select {
			case omc.orderConsumerChannel <- message:
			default:
				logger.Log("worker pull is busy, dropping messages")
			}
		}
	}
}

func (omc *OrderMessageConsumer) registerConsumerWorker(id int, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-omc.orderConsumerChannel:
			logger.Log(fmt.Sprintf("worker %d - Processed Message %v", id, message.Data))
			_, err := omc.repositories.OrderRepository.Create(message.Data, nil)
			if err != nil {
				logger.Log(fmt.Sprintf("FAILED Message %v, id : %v err : %v", message.Data, id, err))
			} else {
				omc.consumer.GetReader().CommitMessages(ctx, message.KafkaMessage)
			}
		}
	}
}

func GetOrderMessageConsumer(consumer service.IMessageConsumer, repositories repository.Repositories) *OrderMessageConsumer {
	return &OrderMessageConsumer{
		consumer:     consumer,
		repositories: repositories,
	}
}
