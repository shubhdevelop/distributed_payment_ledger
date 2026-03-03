// Package handlers
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/shubhdevelop/distributed_payment_ledger/internal/services"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

type UserHandler struct {
	db    *mongo.Client
	cache *glide.Client
}

func NewUserHandler(db *mongo.Client, cache *glide.Client) *UserHandler {
	return &UserHandler{
		db:    db,
		cache: cache,
	}
}

// TODO: Move them to proper folder
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewError(code int, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

func (h *UserHandler) CreateUser(ctx context.Context, w http.ResponseWriter,
	r *http.Request,
) {
	name := r.URL.Query().Get("name")
	if credits, err := strconv.Atoi(r.URL.Query().Get("credits")); err != nil {
		errResp := NewError(400, err.Error())
		resp, err := json.Marshal(errResp)
		if err != nil {
			fmt.Println("error marshalling credits")
		}
		w.Write([]byte(resp))
		return
	} else {

		userService := services.NewUserService(h.db, h.cache)
		createdUser, err := userService.CreateUser(ctx, name, credits)
		if err != nil {
			errResp := NewError(400, err.Error())
			resp, err := json.Marshal(errResp)
			if err != nil {
				fmt.Println("error marshalling credits")
			}
			w.Write([]byte(resp))
		}
		resp, err := json.Marshal(createdUser)
		if err != nil {
			fmt.Println("error marshalling credits")
		}
		w.Write([]byte(resp))

	}
}
