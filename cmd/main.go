// Package main
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/valkey"
)

func main() {
	fmt.Println("hello world")
	ctx := context.Background()
	valkeyClient, err := valkey.ValkeyInit(ctx, "localhost", 6379)
	if err != nil {
		fmt.Printf("Error connecting to Valkey: %w", err)
	}
	err = valkey.LoadValkeyScripts(ctx, valkeyClient)
	if err != nil {
		fmt.Printf("Error loading scripts to Valkey: %w", err)
	}
	router := http.NewServeMux()
	router.HandleFunc("POST /transfer", func(r http.ResponseWriter, w *http.Request) {
		err := valkey.Touch(ctx, "example_key", valkeyClient)
		if err != nil {
			fmt.Println("error ping redis")
		}
		_, err = r.Write([]byte("hello transfered!!"))
		if err != nil {
			fmt.Printf("error while writing to the r.Writer: %w", err)
		}
	})

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		fmt.Printf("error Starting the server: %w", err)
	}
}
