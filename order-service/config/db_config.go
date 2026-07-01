package config

import (
	"context"
	"fmt"
	"log"
	"pizza-shop/logger"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var DBClient *mongo.Client
var MONGO_DB_NAME = "pizza-shop-eda"

func init() {
	InitializeDB()
}

func InitializeDB() (*mongo.Client, error) {
	logger.Log("Initializing database once more")
	var err error
	if DBClient == nil {
		DBClient, err = initDatabase()
		if err != nil {
			log.Fatal("failed to initialize database %v", err)
		}
	}
	return DBClient, nil
}

func initDatabase() (*mongo.Client, error) {
	dbURL := GetEnvProperty("database_url")
	if dbURL == "" {
		return nil, fmt.Errorf("database is not set in env variable")
	}
	clientOptions := options.Client().ApplyURI(dbURL).
		SetMaxPoolSize(600).
		SetMinPoolSize(50).
		SetMaxConnIdleTime(30 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongoDb %v", err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("mongodb is unreacheble %v", err)
	}
	log.Printf("connected to MongoDB %s\n", dbURL)
	return client, nil
}

func GetDatabaseCollection(dbName *string, collectionName string) *mongo.Collection {
	if dbName == nil {
		dbName = &MONGO_DB_NAME
	}
	client, err := InitializeDB()
	if err != nil || client == nil {
		log.Fatalf("MongoDB client initialization failed %v", err)
	}
	return client.Database(*dbName).Collection(collectionName)
}

func GetMongoClient() *mongo.Client {
	client, err := InitializeDB()
	if err != nil {
		log.Fatalf("MongoDb client initialization failed %v", err)
	}
	return client
}
