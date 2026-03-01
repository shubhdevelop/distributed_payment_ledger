// Package creditCache provides basic constants and mathematical functions.
package creditCache

import (
	"context"
	"fmt"
	"log"
	"time"

	_ "embed"

	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

//go:embed credits_lib.lua
var creditScript string

type CacheResult struct {
	Status  int64
	Code    string
	Balance int64
	LastID  string
}

type CreditCache struct {
	cache *glide.Client
}

func NewCreditCache(cache *glide.Client) *CreditCache {
	return &CreditCache{
		cache: cache,
	}
}

func LoadValkeyScripts(ctx context.Context, client *glide.Client) error {
	scripts := []string{creditScript}
	for _, script := range scripts {
		_, err := client.CustomCommand(ctx, []string{
			"FUNCTION",
			"LOAD",
			"REPLACE",
			string(script),
		})
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

func (c *CreditCache) TransferCredits(ctx context.Context, fromKey, toKey, idempotencyKey, streamKey, amt, txnID, senderUserID, reciverUserID string) (*CacheResult, error) {
	val, err := c.cache.FCallWithKeysAndArgs(ctx, "transferCredits", []string{fromKey, toKey, idempotencyKey, streamKey}, []string{amt, txnID, senderUserID, reciverUserID})
	if err != nil {
		log.Printf("err execeuting transfer %v credits from this user %v to %v user %v", amt, fromKey, toKey, err)
		return &CacheResult{}, err
	}

	arr, ok := val.([]any)
	if !ok {
		log.Fatalf("unexpected type: %T", val)
	}
	result := &CacheResult{}

	// Safe parsing
	if status, ok := arr[0].(int64); ok {
		result.Status = status
	} else {
		return nil, fmt.Errorf("invalid status type")
	}

	if code, ok := arr[1].(string); ok {
		result.Code = code
	} else {
		return nil, fmt.Errorf("invalid code type")
	}

	if len(arr) >= 3 {
		if balance, ok := arr[2].(int64); ok {
			result.Balance = balance
		}
	}

	if len(arr) >= 4 {
		if lastID, ok := arr[3].(string); ok {
			result.LastID = lastID
		}
	}
	return result, nil
}

func (c *CreditCache) DeductCredits(ctx context.Context, balKey, idempotencyKey, streamKey, amt, txnID, userID string) (*CacheResult, error) {
	val, err := c.cache.FCallWithKeysAndArgs(ctx, "deductCredits", []string{balKey, idempotencyKey, streamKey}, []string{amt, txnID, userID})
	if err != nil {
		log.Printf("err execeuting credits deduction command: %v", err)
		return &CacheResult{}, err
	}

	arr, ok := val.([]any)
	if !ok {
		log.Fatalf("unexpected type: %T", val)
	}

	status := arr[0].(int64) // Lua number → int64
	code := arr[1].(string)  // same

	var balance int64
	if len(arr) == 3 {
		balance = arr[2].(int64)
	}
	return &CacheResult{
		status, code, balance, "0",
	}, nil
}

func (c *CreditCache) AddCredits(ctx context.Context, balKey, idempotencyKey, streamKey, amt, txnID, userID string) (*CacheResult, error) {
	val, err := c.cache.FCallWithKeysAndArgs(ctx, "addCredits", []string{balKey, idempotencyKey, streamKey}, []string{amt, txnID, userID})
	if err != nil {
		log.Printf("err execeuting addCredits command: %v", err)
		return &CacheResult{}, err
	}

	arr, ok := val.([]any)
	if !ok {
		log.Fatalf("unexpected type: %T", val)
	}

	status := arr[0].(int64) // Lua number → int64
	code := arr[1].(string)  // same

	var balance int64
	if len(arr) == 3 {
		balance = arr[2].(int64)
	}
	return &CacheResult{
		status, code, balance, "0",
	}, nil
}

func (c *CreditCache) Touch(ctx context.Context, key string) error {
	val, err := c.cache.Touch(ctx, []string{key})
	fmt.Println(val)
	if err != nil {
		log.Printf("err execeuting getBalance command: %v", err)
		return err
	}
	return nil
}

func (c *CreditCache) GetBalance(ctx context.Context, balKey string) (*CacheResult, error) {
	val, err := c.cache.FCallWithKeysAndArgs(ctx, "getBalance", []string{balKey}, []string{})
	if err != nil {
		log.Printf("err execeuting getBalance command: %v", err)
		return &CacheResult{}, err
	}

	arr, ok := val.([]any)
	if !ok {
		log.Fatalf("unexpected type: %T", val)
	}

	status := arr[0].(int64) // Lua number → int64
	code := arr[1].(string)  // same

	var balance int64
	if len(arr) == 3 {
		balance = arr[2].(int64)
	}
	return &CacheResult{
		status, code, balance, "0",
	}, nil
}

func (c *CreditCache) ReadCreditResponseStream(ctx context.Context, lastID string, txnID string) (*models.StreamEntry, error) {
	keysAndIds := map[string]string{
		"transfer:response": lastID,
	}
	fmt.Println(keysAndIds)

	opts := options.NewXRangeOptions()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.cache.XRangeWithOptions(ctx, "transfer:response", options.StreamBoundary(lastID),
		options.StreamBoundary(lastID),
		*opts,
	)
	if err != nil {
		return nil, fmt.Errorf("error reading the Steam: %w", err)
	}
	for _, entry := range resp {
		for _, field := range entry.Fields {
			if field.Field == "txnId" && field.Value == txnID {
				return &entry, nil
			}
		}
	}
	return nil, nil
}
