package repository

import "pizza-shop/config"

type Repositories struct {
	OrderRepository IRepository
}

func GetRepositories() *Repositories {
	return &Repositories{
		OrderRepository: GetMongoRepository(config.GetEnvProperty("MONGO_DB_NAME"), "orders"),
	}
}
