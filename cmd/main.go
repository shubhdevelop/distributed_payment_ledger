// Package main
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

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
	router.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey := r.URL.Query().Get("IKey")
		txnID := r.URL.Query().Get("tnxID")
		amount := r.URL.Query().Get("amt")
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")

		senderKey := "uid:" + from + ":credits"
		recieverKey := "uid:" + to + ":credits"

		val, err := valkey.TransferCredits(ctx, valkeyClient, senderKey, recieverKey, idempotencyKey, "transfer:response", amount, txnID, from, to)
		if err != nil {
			w.Write([]byte("Error transferring the credits"))
		}

		fmt.Println(val)
		ctx := context.Background()

		switch val.Code {
		case "ALREADY_PROCESSED":
			w.Write([]byte("The transaction is already processed for this trnasaction ID "))
		case "TRANSFERRED":
			keysAndIds := map[string]string{
				"transfer:response": val.LastID,
			}
			fmt.Println(keysAndIds)

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			resp, err := valkeyClient.XRead(ctx, keysAndIds)
			if err != nil {
				log.Fatal(err)
			}
			// for stream, data := range resp {
			// 	fmt.Println("Stream:", stream)

			// 	for _, entry := range data.Entries {
			// 		fmt.Println("ID:", entry.ID)
			// 		fmt.Println("Fields:", entry.Fields)
			// 	}
			// }
			fmt.Println(resp)
			_, err = w.Write([]byte("hello transfered!!"))
			if err != nil {
				fmt.Printf("error while writing to the r.Writer: %w", err)
			}
		}
	})

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		fmt.Printf("error Starting the server: %w", err)
	}
}
