// Package repository
package repository

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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
		db: db.Database("UserCredit"),
	}
}

func (u *UserRepo) GetUserByID(ctx context.Context, userID string) (*User, error) {
	userCollection := u.db.Collection("userCredit")
	var user User
	err := userCollection.FindOne(ctx, bson.M{"_id": userID}).Decode(&user)
	if err != nil {
		return nil, err
	}
	return &User{}, nil
}

func (u *UserRepo) CreateUser(ctx context.Context, name string) (*User, error) {
	userCollection := u.db.Collection("userCredit")
	user := NewUser(name, 1000)

	result, err := userCollection.InsertOne(ctx, user)
	if err != nil {
		return nil, err
	}
	user.ID = result.InsertedID.(primitive.ObjectID)

	return &User{}, nil
}
