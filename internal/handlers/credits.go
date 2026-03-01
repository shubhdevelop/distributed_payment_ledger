// Package creditHandler
package creditHanlder

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/valkey"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

func TransferHandler(ctx context.Context, valkeyClient *glide.Client, w http.ResponseWriter, r *http.Request) {
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

	switch val.Code {
	case "ALREADY_PROCESSED":
		w.Write([]byte("The transaction is already processed for this trnasaction ID "))
	case "TRANSFERRED":
		keysAndIds := map[string]string{
			"transfer:response": val.LastID,
		}
		fmt.Println(keysAndIds)

		opts := options.NewXRangeOptions()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, err := valkeyClient.XRangeWithOptions(ctx, "transfer:response", options.StreamBoundary(val.LastID), options.StreamBoundary(val.LastID), *opts)
		if err != nil {
			log.Fatal(err)
		}
		found := false
		for _, entry := range resp {
			for _, field := range entry.Fields {
				if field.Field == "txnId" && field.Value == txnID {
					found = true
					fmt.Println("Matched Entry ID:", entry.ID)
					fmt.Println("Matched Entry Fields:", entry.Fields)
					break
				}
			}
			if found {
				break
			}
		}

		if found {
			_, err = w.Write([]byte("hello transferred!!"))
			if err != nil {
				fmt.Printf("error while writing to the r.Writer: %v", err)
			}
		} else {
			w.Write([]byte("Transaction ID not found in response"))
		}
	}
}
