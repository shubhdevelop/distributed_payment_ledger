package db

import (
	"context"
	"errors"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func MongoDBInit(URI string) (*mongo.Client, error) {
	docs := "www.mongodb.com/docs/drivers/go/current/"
	if URI == "" {
		log.Fatal("Set your 'MONGODB_URI' environment variable. " +
			"See: " + docs +
			"usage-examples/#environment-variable")
		return nil, errors.New("DB_URI not set")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(URI))
	if err != nil {
		panic(err)
	}
	client.Ping(ctx, &readpref.ReadPref{})
	return client, nil
}
