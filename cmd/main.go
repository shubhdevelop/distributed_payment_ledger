// Package main
package main

import (
	"context"
	"fmt"
	"net/http"

	creditCache "github.com/shubhdevelop/distributed_payment_ledger/internal/cache/credits"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/cache/valkey"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/db"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/handlers"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/workers"
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

	err = creditCache.LoadValkeyScripts(ctx, valkeyClient)
	if err != nil {
		fmt.Printf("Error loading scripts to Valkey: %w", err)
	}

	creditWorkerContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerValkeyClient1, err := valkey.ValkeyInit(ctx, "valkey", 6379)
	if err != nil {
		fmt.Printf("Error connecting worker1 to Valkey: %w", err)
	}
	workerValkeyClient2, err := valkey.ValkeyInit(ctx, "valkey", 6379)
	if err != nil {
		fmt.Printf("Error connecting worker2 to Valkey: %w", err)
	}

	creditWorker1 := workers.NewWorker(mongoClient, workerValkeyClient1)
	creditWorker2 := workers.NewWorker(mongoClient, workerValkeyClient2)

	go creditWorker1.StartWorker(creditWorkerContext, "credits-worker-1")
	go creditWorker2.StartWorker(creditWorkerContext, "credits-worker-2")

	router := http.NewServeMux()
	router.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		creditHandler := handlers.NewCreditHandler(mongoClient, valkeyClient)
		creditHandler.TransferHandler(ctx, w, r)
	})

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		fmt.Printf("error Starting the server: %w", err)
	}
}
