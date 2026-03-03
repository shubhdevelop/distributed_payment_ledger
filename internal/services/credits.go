// Package services
package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	creditCache "github.com/shubhdevelop/distributed_payment_ledger/internal/cache/credits"
	"github.com/shubhdevelop/distributed_payment_ledger/internal/repository"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/constants"
	"github.com/valkey-io/valkey-glide/go/v2/options"
	"go.mongodb.org/mongo-driver/mongo"
)

type CreditService struct {
	db    *mongo.Client
	cache *glide.Client
}

func NewCreditService(db *mongo.Client, cache *glide.Client) *CreditService {
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
		return nil, fmt.Errorf("transaction Id: %s is already processed", idempotencyKey)
	case "CACHE_MISS_BOTH":
		_, err := s.GetOrHydrateBalance(ctx, senderID)
		if err != nil {
			return nil, fmt.Errorf("error rehydrating sender cache: %w", err)
		}
		_, err = s.GetOrHydrateBalance(ctx, recieverID)
		if err != nil {
			return nil, fmt.Errorf("error rehydrating receiver cache: %w", err)
		}
		val, err := s.TransferCredits(ctx, amount, senderID, recieverID, idempotencyKey, streamKey, txnID)
		if err != nil {
			return nil, fmt.Errorf("error transferring credits %w", err)
		}
		return val, nil
	case "CACHE_MISS_SENDER":
		_, err = s.GetOrHydrateBalance(ctx, senderID)
		if err != nil {
			return nil, fmt.Errorf("error rehydrating sender cache: %w", err)
		}
		val, err = s.TransferCredits(ctx, amount, senderID, recieverID, idempotencyKey, streamKey, txnID)
		if err != nil {
			return nil, fmt.Errorf("error transferring credits %w", err)
		}
		return val, nil
	case "CACHE_MISS_RECIEVER":
		_, err = s.GetOrHydrateBalance(ctx, recieverID)
		if err != nil {
			return nil, fmt.Errorf("error rehydrating receiver cache: %w", err)
		}
		val, err = s.TransferCredits(ctx, amount, senderID, recieverID, idempotencyKey, streamKey, txnID)
		if err != nil {
			return nil, fmt.Errorf("error transferring credits %w", err)
		}
		return val, nil
	case "INSUFFICIENT_BALANCE":
		return nil, fmt.Errorf("sender: %s has insufficient balance for transfer of amount: %s", senderID, amount)
	case "TRANSFERRED":
		return val, nil
	}
	return nil, fmt.Errorf("unexpected transfer result code: %s", val.Code)
}

const maxHydrateRetries = 100 // ~10s at 100ms per retry

func (s *CreditService) GetOrHydrateBalance(ctx context.Context, userID string) (int, error) {
	creditCache := creditCache.NewCreditCache(s.cache)
	balanceKey := "uid:" + userID + ":credit"
	res, err := creditCache.GetBalance(ctx, balanceKey)
	if err == nil && res.Code == "RETRIEVED" {
		return int(res.Balance), nil
	}
	userRepo := repository.NewUserRepo(s.db)
	lockKey := "lock:hydrate:" + userID

	opts := options.NewSetOptions().SetOnlyIfDoesNotExist().SetExpiry(&options.Expiry{
		Type:     constants.Seconds,
		Duration: 30,
	})

	for attempt := 0; attempt < maxHydrateRetries; attempt++ {
		if ctx.Err() != nil {
			return -1, ctx.Err()
		}
		locked, err := s.cache.SetWithOptions(ctx, lockKey, "Processing", *opts)
		if err != nil {
			return -1, fmt.Errorf("error acquiring lock: %w", err)
		}
		if !locked.IsNil() {
			var bal int
			defer func() {
				_, _ = s.cache.Del(context.Background(), []string{lockKey})
			}()
			if bal, err = userRepo.GetUsersCreditsByID(ctx, userID); err != nil {
				return -1, err
			}
			_, err = s.cache.Set(ctx, "uid:"+userID+":credit", strconv.Itoa(bal))
			if err != nil {
				return -1, err
			}
			return bal, nil
		}
		// Lock held by another request; wait and retry
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return -1, errors.New("timed out waiting for hydrate lock")
}
