// Package valkey provides basic constants and mathematical functions.
package valkey

import (
	"context"
	"fmt"
	"log"

	_ "embed"

	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
)

//go:embed credits_lib.lua
var creditScript string

func ValkeyInit(ctx context.Context, host string, port int) (*glide.Client, error) {
	config := config.NewClientConfiguration().WithAddress(&config.NodeAddress{Host: host, Port: port})

	client, err := glide.NewClient(config)
	if err != nil {
		log.Println("There was an error: ", err)
		return nil, err
	}

	res, err := client.Ping(ctx)
	if err != nil {
		log.Println("There was an error: ", err)
	}
	log.Printf("Connected! Server responded: %s\n", res)
	return client, nil
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

type Result struct {
	Status  int64
	Code    string
	Balance int64
	LastID  string
}

func TransferCredits(ctx context.Context, client *glide.Client, fromKey, toKey, idempotencyKey, streamKey, amt, txnID, senderUserID, reciverUserID string) (*Result, error) {
	val, err := client.FCallWithKeysAndArgs(ctx, "transferCredits", []string{fromKey, toKey, idempotencyKey, streamKey}, []string{amt, txnID, senderUserID, reciverUserID})
	if err != nil {
		log.Printf("err execeuting transfer %v credits from this user %v to %v user %v", amt, fromKey, toKey, err)
		return &Result{}, err
	}

	arr, ok := val.([]any)
	if !ok {
		log.Fatalf("unexpected type: %T", val)
	}
	result := &Result{}

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

func DeductCredits(ctx context.Context, client *glide.Client, balKey, idempotencyKey, streamKey, amt, txnID, userID string) (*Result, error) {
	val, err := client.FCallWithKeysAndArgs(ctx, "deductCredits", []string{balKey, idempotencyKey, streamKey}, []string{amt, txnID, userID})
	if err != nil {
		log.Printf("err execeuting credits deduction command: %v", err)
		return &Result{}, err
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
	return &Result{
		status, code, balance, "0",
	}, nil
}

func AddCredits(ctx context.Context, client *glide.Client, balKey, idempotencyKey, streamKey, amt, txnID, userID string) (*Result, error) {
	val, err := client.FCallWithKeysAndArgs(ctx, "addCredits", []string{balKey, idempotencyKey, streamKey}, []string{amt, txnID, userID})
	if err != nil {
		log.Printf("err execeuting addCredits command: %v", err)
		return &Result{}, err
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
	return &Result{
		status, code, balance, "0",
	}, nil
}

func Touch(ctx context.Context, key string, client *glide.Client) error {
	val, err := client.Touch(ctx, []string{key})
	fmt.Println(val)
	if err != nil {
		log.Printf("err execeuting getBalance command: %v", err)
		return err
	}
	return nil
}

func GetBalance(ctx context.Context, client *glide.Client, balKey string) (*Result, error) {
	val, err := client.FCallWithKeysAndArgs(ctx, "getBalance", []string{balKey}, []string{})
	if err != nil {
		log.Printf("err execeuting getBalance command: %v", err)
		return &Result{}, err
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
	return &Result{
		status, code, balance, "0",
	}, nil
}
