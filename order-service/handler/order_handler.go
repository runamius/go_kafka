package handler

import (
	"fmt"
	"pizza-shop/config/constants"
	"pizza-shop/logger"
	"pizza-shop/service"
	"pizza-shop/utils"

	"github.com/gin-gonic/gin"
)

var messagePublisherWorker = make(chan map[string]interface{}, 1000)

type OrderHandler struct {
	publisher service.IMessagePublisher
}

func (h *OrderHandler) CreateOrder(ctx *gin.Context) {
	payload := make(map[string]interface{})
	if err := ctx.ShouldBindJSON(payload); err != nil {
		logger.Log(fmt.Sprintf("Error mapping body %v", err))
		ctx.JSON(400, gin.H{
			"message":    "Bad Request",
			"statusCode": 400,
		})
		return
	}

	id := utils.GetId()
	payload["_id"] = id

	messagePublisherWorker <- payload

	ctx.JSON(200, gin.H{
		"message": "order is being undertaken, wait",
		"status":  200,
	})
}

func registerMessagePublisherWorker(id int, publisher *service.IMessagePublisher) {
	for message := range messagePublisherWorker {
		err := (*publisher).PublishEvent(constants.TOPIC_ORDER, message)
		if err != nil {
			logger.Log(fmt.Sprintf("worker %d failed to publish event %v", id, err))
		}
	}
}

func GetOrderHandler(publisher service.IMessagePublisher) *OrderHandler {
	h := &OrderHandler{
		publisher: publisher,
	}

	for i := 0; i < cap(messagePublisherWorker); i++ {
		go registerMessagePublisherWorker(i, &publisher)
	}

	return h
}
