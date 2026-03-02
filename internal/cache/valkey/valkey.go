// Package valkey provides basic constants and mathematical functions.
package valkey

import (
	"context"
	"log"

	_ "embed"

	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
)

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
