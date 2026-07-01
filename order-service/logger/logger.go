package logger

import (
	"os"
	"pizza-shop/utils"
)

func Log(message any) {
	logStatus := os.Getenv("LOG_LEVEL")
	if logStatus == "debug" {
		utils.AppendToFile("log.txt", message.(string))
	}
}
