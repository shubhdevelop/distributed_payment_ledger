// Package repository
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type UserRepo struct {
	db *mongo.Database
}

type User struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name      string             `bson:"name" json:"name"`
	Credits   int                `bson:"credits" json:"credits"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at" json:"updated_at"`
}

func NewUser(name string, credits int) *User {
	return &User{
		Name:      name,
		Credits:   credits,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func NewUserRepo(db *mongo.Client) *UserRepo {
	return &UserRepo{
		db: db.Database("user"),
	}
}

func (u *UserRepo) getFilter(userID string) (bson.M, error) {
	if objID, err := primitive.ObjectIDFromHex(userID); err == nil {
		return bson.M{"_id": objID}, nil
	}
	return bson.M{"_id": userID}, nil
}

func (u *UserRepo) GetUserByID(ctx context.Context, userID string) (*User, error) {
	filter, _ := u.getFilter(userID)
	userCollection := u.db.Collection("user")
	var user User
	err := userCollection.FindOne(ctx, filter).Decode(&user)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("user not found: %s", userID)
		}
		return nil, err
	}
	return &user, nil
}

func (u *UserRepo) GetUsersCreditsByID(ctx context.Context, userID string) (int, error) {
	filter, _ := u.getFilter(userID)
	userCollection := u.db.Collection("user")
	var res struct {
		Credits int `bson:"credits"`
	}
	err := userCollection.FindOne(ctx, filter).Decode(&res)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return -1, fmt.Errorf("user not found: %s", userID)
		}
		return -1, err
	}
	return res.Credits, nil
}

func (u *UserRepo) CreateUser(ctx context.Context, name string, credits int) (*User, error) {
	userCollection := u.db.Collection("user")
	user := NewUser(name, credits)

	result, err := userCollection.InsertOne(ctx, user)
	if err != nil {
		return nil, err
	}
	user.ID = result.InsertedID.(primitive.ObjectID)

	return user, nil
}

func (u *UserRepo) BatchUpdateUserCredits(ctx context.Context, userBalance *map[string]int) error {
	userCollection := u.db.Collection("user")

	var models []mongo.WriteModel

	for userIDStr, amount := range *userBalance {
		filter, _ := u.getFilter(userIDStr)

		update := bson.M{
			"$inc": bson.M{
				"credits": amount,
			},
		}

		model := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update)

		models = append(models, model)
	}

	opts := options.BulkWrite().SetOrdered(false)

	_, err := userCollection.BulkWrite(ctx, models, opts)
	if err != nil {
		if bwe, ok := err.(mongo.BulkWriteException); ok {
			for _, writeErr := range bwe.WriteErrors {
				fmt.Println("Failed index:", writeErr.Index)
			}
		}
		return err
	}

	return nil
}
