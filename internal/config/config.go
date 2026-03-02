// Package config
package config

import (
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

type App struct {
	cache *glide.Client
	db    *mongo.Client
}

func NewApp(cache *glide.Client, db *mongo.Client) *App {
	return &App{
		cache: cache,
		db:    db,
	}
}
