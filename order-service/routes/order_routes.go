package routes

import (
	"pizza-shop/handler"
	"pizza-shop/service"

	"github.com/gin-gonic/gin"
)

func registerOrderRoutes(r *gin.RouterGroup, publisher service.IMessagePublisher) {
	orderHandler := handler.GetOrderHandler(publisher)

	r.POST("/create", orderHandler.CreateOrder)

}
