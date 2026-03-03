// Package services
package services

import (
	"context"
	"fmt"

	creditCache "github.com/shubhdevelop/distributed_payment_ledger/internal/cache/credits"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

type CreditService struct {
	db    *mongo.Client
	cache *glide.Client
}

func NewCreditServce(db *mongo.Client, cache *glide.Client) *CreditService {
	return &CreditService{
		db:    db,
		cache: cache,
	}
}

func (s *CreditService) TransferCredits(
	ctx context.Context,
	amount, senderID, recieverID, idempotencyKey, streamKey, txnID string,
) (*creditCache.CacheResult, error) {
	senderKey := "uid:" + senderID + ":credit"
	recieverKey := "uid:" + recieverID + ":credit"

	cache := creditCache.NewCreditCache(s.cache)
	val, err := cache.TransferCredits(
		ctx, amount,
		senderKey, recieverKey, idempotencyKey,
		streamKey, txnID, senderID, recieverID,
	)
	if err != nil {
		return nil, fmt.Errorf("error processing the transfer: %w", err)
	}

	switch val.Code {
	case "ALREADY_PROCESSED":
		return nil, fmt.Errorf("transaction Id: %s is alreadyprocessed", idempotencyKey)
	case "CACHE_MISS_BOTH":
		return nil, fmt.Errorf("noth the sender: %s and reciver: %s missing in the cacche", senderID, recieverID)
	case "CACHE_MISS_SENDER":
		return nil, fmt.Errorf("sender: %s missing in the cache", senderID)
	case "CACHE_MISS_RECIEVER":
		return nil, fmt.Errorf("reciver: %s missing in the cahce", recieverID)
	case "INSUFFICIENT_BALANCE":
		return nil, fmt.Errorf("sender: %s has insufficient balance for transfer of amount: %s", senderID, amount)
	case "TRANSFERRED":
		// Successful transfer
		return val, nil
	}
	return nil, fmt.Errorf("unexpected transfer result code: %s", val.Code)
}
