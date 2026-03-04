// Package services
package services

import (
	"context"
	"fmt"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/repository"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

type UserService struct {
	db    *mongo.Client
	cache *glide.Client
}

func NewUserService(db *mongo.Client, cache *glide.Client) *UserService {
	return &UserService{
		db:    db,
		cache: cache,
	}
}

func (s *UserService) CreateUser(
	ctx context.Context,
	name string, credits int,
) (*repository.User, error) {
	userRepo := repository.NewUserRepo(s.db)
	user, err := userRepo.CreateUser(ctx, name, credits)
	if err != nil {
		return nil, fmt.Errorf("error creating user: %w", err)
	}

	return user, nil
}
