// Package main
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/db"
	creditHanlder "github.com/shubhdevelop/distributed_payment_ledger/internal/handlers"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/valkey"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	fmt.Println("hello world")
	ctx := context.Background()
	valkeyClient, err := valkey.ValkeyInit(ctx, "valkey", 6379)
	if err != nil {
		fmt.Printf("Error connecting to Valkey: %w", err)
	}
	mongoClient, err := db.MongoDBInit("mongodb://mongo:27017/")
	if err != nil {
		fmt.Printf("Error connecting to MongoDb: %w", err)
	}

	mongoClient.Ping(context.Background(), readpref.PrimaryPreferred())

	err = valkey.LoadValkeyScripts(ctx, valkeyClient)
	if err != nil {
		fmt.Printf("Error loading scripts to Valkey: %w", err)
	}
	router := http.NewServeMux()
	router.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		creditHanlder.TransferHandler(ctx, valkeyClient, w, r)
	})

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		fmt.Printf("error Starting the server: %w", err)
	}
}
