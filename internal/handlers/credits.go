// Package handlers
package handlers

import (
	"context"
	"fmt"
	"net/http"

	creditCache "github.com/shubhdevelop/distributed_payment_ledger/internal/cache/credits"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

type CreditHandler struct {
	db    *mongo.Client
	cache *glide.Client
}

func NewCreditHandler(db *mongo.Client, cache *glide.Client) *CreditHandler {
	return &CreditHandler{
		db:    db,
		cache: cache,
	}
}

func (h *CreditHandler) TransferHandler(ctx context.Context, w http.ResponseWriter,
	r *http.Request,
) {
	idempotencyKey := r.URL.Query().Get("IKey")
	txnID := r.URL.Query().Get("tnxID")
	amount := r.URL.Query().Get("amt")
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	senderKey := "uid:" + from + ":credits"
	recieverKey := "uid:" + to + ":credits"

	cache := creditCache.NewCreditCache(h.cache)
	val, err := cache.TransferCredits(ctx,
		senderKey, recieverKey, idempotencyKey,
		"transfer:response", amount, txnID, from, to,
	)
	if err != nil {
		w.Write([]byte("Error transferring the credits"))
	}

	fmt.Println(val)

	switch val.Code {
	case "ALREADY_PROCESSED":
		w.Write([]byte("The transaction is already processed for this trnasaction ID "))
	case "TRANSFERRED":

	}
}
