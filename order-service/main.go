package main

import (
	"fmt"
	"net/http"
	"pizza-shop/config/constants"
	messageconsumer "pizza-shop/message-consumer"
	"pizza-shop/repository"
	"pizza-shop/routes"
	"pizza-shop/service"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)

	app := gin.Default()
	app.Use(gin.Recovery())
	app.GET("/ping", func(ctx *gin.Context) {
		ctx.JSON(200, gin.H{
			"message": "service is up and running",
		})
	})

	var repositories = repository.GetRepositories()
	var orderConsumer = messageconsumer.GetOrderMessageConsumer(
		service.GetNewKafkaConsumer(constants.TOPIC_ORDER, "order-message"),
		*repositories,
	)
	go orderConsumer.StartConsuming()

	routes.RegisterRoutes(
		app,
		service.GetKafkaMessagePublisher(constants.TOPIC_ORDER),
	)

	srv := &http.Server{
		Addr:           fmt.Sprintf(":%d", 8001),
		Handler:        app,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    30 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}

}
