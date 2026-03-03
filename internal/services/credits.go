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

func (s *CreditService) GetOrHydrateBalance(ctx context.Context, userID string) (int, error) {
	creditCache := creditCache.NewCreditCache(s.cache)
	res, err := creditCache.GetBalance(ctx, userID)
	if err == nil {
		return int(res.Balance), nil
	}
	userRepo := repository.NewUserRepo(s.db)
	lockKey := "lock:hydrate:" + userID

	opts := options.NewSetOptions().SetOnlyIfDoesNotExist().SetExpiry(&options.Expiry{
		Type:     constants.Seconds,
		Duration: 30,
	})

	locked, err := s.cache.SetWithOptions(ctx, lockKey, "Processing", *opts)
	if err != nil {
		return -1, errors.New("error acquiring lock")
	}
	if !locked.IsNil() {
		var bal int
		defer func() {
			// Use background context so lock is released even if request context is cancelled
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
	} else {
		// Lock Failed: Someone else is fetching the data. Sleep 100ms and recursively retry.
		time.Sleep(100 * time.Millisecond)
		return s.GetOrHydrateBalance(ctx, userID)
	}
}
