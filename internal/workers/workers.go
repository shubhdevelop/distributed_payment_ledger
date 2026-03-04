// Package workers
package workers

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/repository"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/options"
	"go.mongodb.org/mongo-driver/mongo"
)

type Worker struct {
	db     *mongo.Client
	stream *glide.Client
}

func NewWorker(
	db *mongo.Client,
	stream *glide.Client,
) *Worker {
	return &Worker{
		db:     db,
		stream: stream,
	}
}

func (w *Worker) StartWorker(ctx context.Context, consumerName string) {
	streamKey := os.Getenv("STREAM_KEY")
	consumeGroup := os.Getenv("CONSUMER_GROUP")
	opts := options.XReadGroupOptions{
		Count: 10,
		NoAck: false,
		Block: time.Second * 0,
	}
	userRepo := repository.NewUserRepo(w.db)
	_, err := w.stream.XGroupCreateWithOptions(ctx, streamKey, consumeGroup, "$", options.XGroupCreateOptions{MkStream: true})
	if err != nil {
		fmt.Printf("Info: consumer group might already exist: %v\n", err)
	}
	_, err = w.stream.XGroupCreateConsumer(ctx, streamKey, consumeGroup, consumerName)
	if err != nil {
		fmt.Printf("Info: consumer might already exist: %v\n", err)
	}
	for {
		select {
		case <-ctx.Done():
			// TODO: Add a check for gracefully stopping the consumer
			fmt.Println("Stopping consumer...")
			return
		default:
			stream, err := w.stream.XReadGroupWithOptions(ctx, consumeGroup, consumerName, map[string]string{
				streamKey: ">",
			}, opts)
			if err != nil {
				fmt.Printf("error reading the %s stream for consumer Group: %s by worker: %s, error: %v\n", streamKey, consumeGroup, consumerName,
					err)
				time.Sleep(1 * time.Second) // Prevent tight loop on error
				continue
			}

			/*TODO:
			1.batch save all the transaction log in a single bulk write
			*/
			allResult := make([]map[string]string, 0)
			if len(stream) > 0 {
				for _, val := range stream {
					for _, entry := range val.Entries {
						fmt.Println("NEW MESSAGE RECEIVED ->", entry.Fields, ":", consumerName)
						result := make(map[string]string)
						for _, f := range entry.Fields {
							result[f.Field] = f.Value
						}
						allResult = append(allResult, result)
					}
				}
				netBalance := make(map[string]int)
				for _, result := range allResult {
					if result["type"] == "transfer" {
						continue
					}
					var userIDKey string
					switch result["type"] {
					case "allot":
						userIDKey = "reciever_user_id"
					case "deduct":
						userIDKey = "sender_user_id"
					}
					if _, ok := netBalance[result[userIDKey]]; ok {
						amount, err := strconv.Atoi(result["amount"])
						if err != nil {
							fmt.Println("error converting amount into integer")
							continue
						}
						netBalance[result[userIDKey]] += amount
					} else {
						amount, err := strconv.Atoi(result["amount"])
						if err != nil {
							fmt.Println("error converting amount into integer")
							continue
						}
						netBalance[result[userIDKey]] = amount
					}
				}
				fmt.Println("upating the userCredit balance with netEffect")
				err = userRepo.BatchUpdateUserCredits(ctx, &netBalance)
				if err != nil {
					fmt.Println("error updating the userCredits %w", err)
				}
			}
		}
	}
}
