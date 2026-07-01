package routes

import (
	"pizza-shop/service"

	"github.com/gin-gonic/gin"
)

func registerOrderRoutes(r *gin.RouterGroup, publisher service.IMessagePublisher)
