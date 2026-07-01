package repository

import (
	"context"
	"pizza-shop/config"

	"go.mongodb.org/mongo-driver/mongo"
)

type IRepository interface {
	Create(data interface{}, ctx interface{}) (interface{}, error)
}

type MongoRepository struct {
	collection *mongo.Collection
}

func getSessionContext(sessionContext interface{}) mongo.SessionContext {
	ctx := context.Background()
	if sessionContext == nil {
		return mongo.NewSessionContext(ctx, mongo.SessionFromContext(ctx))
	}
	return sessionContext.(mongo.SessionContext)
}

func (mr *MongoRepository) Create(data interface{}, ctx interface{}) (interface{}, error) {
	sessionContext := getSessionContext(ctx)
	result, err := mr.collection.InsertOne(sessionContext, data)
	return result, err
}

func GetMongoRepository(dbName string, collectionName string) *MongoRepository {
	collection := config.GetDatabaseCollection(&dbName, collectionName)
	return &MongoRepository{
		collection: collection,
	}
}
