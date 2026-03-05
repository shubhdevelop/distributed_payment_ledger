// Package main
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	creditCache "github.com/shubhdevelop/distributed_payment_ledger/internal/cache/credits"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/cache/valkey"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/db"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/handlers"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/workers"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

func NewServer(db *mongo.Client, cache *glide.Client) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second * 10)
		fmt.Fprintf(w, "completeed %v\n", time.Now())
	})
	mux.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		creditHandler := handlers.NewCreditHandler(db, cache)
		creditHandler.TransferHandler(r.Context(), w, r)
	})
	mux.HandleFunc("POST /users", func(w http.ResponseWriter, r *http.Request) {
		userHandler := handlers.NewUserHandler(db, cache)
		userHandler.CreateUser(r.Context(), w, r)
	})
	return &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
}

func StartServer(ctx context.Context, server *http.Server, timeoutDuration time.Duration) error {
	serverErr := make(chan error, 1)
	go func() {
		if err := http.ListenAndServe(server.Addr, server.Handler); !errors.Is(
			err, http.ErrServerClosed,
		) {
			serverErr <- err
		}
		close(serverErr)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return err
	case <-stop:
		fmt.Println("Stop signal retrieved")
	case <-ctx.Done():
		fmt.Println("Context cancelled", ctx.Err())
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		if closeErr := server.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}

	fmt.Println("Server Closed gracefully")
	return nil
}

func main() {
	ctx := context.Background()
	valkeyClient, err := valkey.ValkeyInit(ctx, "valkey", 6379)
	if err != nil {
		fmt.Printf("Error connecting to Valkey: %v", err)
	}

	mongoClient, err := db.MongoDBInit("mongodb://rootuser:rootpassword@mongo:27017")
	if err != nil {
		fmt.Printf("Error connecting to MongoDb: %v", err)
	}

	err = creditCache.LoadValkeyScripts(ctx, valkeyClient)
	if err != nil {
		fmt.Printf("Error loading scripts to Valkey: %v", err)
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

	err = StartServer(ctx, NewServer(mongoClient, valkeyClient), time.Second*8)
	if err != nil {
		log.Fatalf("error Starting the server: %v", err)
	}
}
