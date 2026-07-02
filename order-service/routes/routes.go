package routes

import (
	"pizza-shop/service"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, publisher service.IMessagePublisher) {
	orderRoutes := r.Group("/order-service")
	_ = orderRoutes
	{
		registerOrderRoutes(orderRoutes, publisher)
	}
}
