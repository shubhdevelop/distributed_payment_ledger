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
	Status  uint16  `json:"status"`
	Message string `json:"message"`
}

func (h *CreditHandler) TransferHandler(ctx context.Context, w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set("Content-Type", "application/json")

	idempotencyKey := r.URL.Query().Get("IKey")
	txnID := r.URL.Query().Get("tnxID")
	amt := r.URL.Query().Get("amt")
	senderID := r.URL.Query().Get("from")
	recieverID := r.URL.Query().Get("to")

	if amt == "" || senderID == "" || recieverID == "" || idempotencyKey == "" || txnID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(Response{Status: 0, Message: "missing required query params: amt, from, to, IKey, tnxID"})
		return
	}
	if _, err := strconv.Atoi(amt); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(Response{Status: 400, Message: "The amount must be a valid integer"})
		return
	}
	streamKey := os.Getenv("STREAM_KEY")
	if streamKey == "" {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(Response{Status: 500, Message: "STREAM_KEY not configured"})
		return
	}

	creditService := services.NewCreditService(h.db, h.cache)
	val, err := creditService.TransferCredits(ctx, amt, senderID, recieverID, idempotencyKey, streamKey, txnID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(Response{Status: 400, Message: err.Error()})
		return
	}
	if val == nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(Response{Status: 500, Message: "empty transfer result"})
		return
	}

	if val.Code != "TRANSFERRED" {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(Response{Status: 200, Message: "unexpected transfer code: " + val.Code})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(Response{Status: 200, Message: "Transferred Successfully"})
}
