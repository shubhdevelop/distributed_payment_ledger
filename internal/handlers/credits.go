// Package handlers
package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/services"
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

type Response struct {
	Status  uint8  `json:"status"`
	Message string `json:"message"`
}

func (h *CreditHandler) TransferHandler(ctx context.Context, w http.ResponseWriter,
	r *http.Request,
) {
	idempotencyKey := r.URL.Query().Get("IKey")
	txnID := r.URL.Query().Get("tnxID")
	amt := r.URL.Query().Get("amt")
	_, err := strconv.Atoi(amt)
	if err != nil {
		http.Error(w, "The amount must be a valid integer", http.StatusBadRequest)
		return
	}
	streamKey := os.Getenv("STREAM_KEY")
	senderID := r.URL.Query().Get("from")
	recieverID := r.URL.Query().Get("to")
	creditService := services.NewCreditService(h.db, h.cache)
	val, err := creditService.TransferCredits(ctx, amt, senderID, recieverID, idempotencyKey, streamKey, txnID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if val == nil {
		http.Error(w, "empty transfer result", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	res, err := json.Marshal(Response{
		Status:  200,
		Message: "Transferred Succesfull",
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if val.Code == "TRANSFERRED" {
		w.Write([]byte(res))
	}
}
