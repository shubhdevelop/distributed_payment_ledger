// Package main
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"

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

	mongoClient, err := db.MongoDBInit("mongodb://rootuser:rootpassword@mongo:27017")
	if err != nil {
		fmt.Printf("Error connecting to MongoDb: %w", err)
	}

	err = creditCache.LoadValkeyScripts(ctx, valkeyClient)
	if err != nil {
		fmt.Printf("Error loading scripts to Valkey: %w", err)
	}

	creditWorkerContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerCount, err := strconv.Atoi(os.Getenv("WORKER_COUNT"))
	if err != nil {
		fmt.Println("worker count is not defined defaulting to single worker")
		workerCount = 1
	}

	for i := range workerCount {
		workerValkeyClient, err := valkey.ValkeyInit(ctx, "valkey", 6379)
		if err != nil {
			fmt.Printf("Error connecting worker%d to Valkey: %v", i, err)
		}
		creditWorker2 := workers.NewWorker(mongoClient, workerValkeyClient)
		go creditWorker2.StartWorker(creditWorkerContext, "credits-worker-"+strconv.Itoa(i))
	}
	router := http.NewServeMux()
	router.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		creditHandler := handlers.NewCreditHandler(mongoClient, valkeyClient)
		creditHandler.TransferHandler(ctx, w, r)
	})
	router.HandleFunc("POST /users", func(w http.ResponseWriter, r *http.Request) {
		userHandler := handlers.NewUserHandler(mongoClient, valkeyClient)
		userHandler.CreateUser(ctx, w, r)
	})

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		fmt.Printf("error Starting the server: %w", err)
	}
}
