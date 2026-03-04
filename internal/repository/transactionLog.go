// Package repository
package repository

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type transactionLogRepo struct {
	db *mongo.Database
}

type TransactionLog struct {
	ID         primitive.ObjectID
	SenderID   string
	RecieverID string
	amount     int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewTransactionLog(senderID, recieverID string, amount int) *TransactionLog {
	return &TransactionLog{
		SenderID:   senderID,
		RecieverID: recieverID,
		amount:     amount,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

func NewTransactionLogRepo(db *mongo.Client) *transactionLogRepo {
	return &transactionLogRepo{
		db: db.Database("TransactionLog"),
	}
}

func (u *UserRepo) GetTransactionByID(ctx context.Context, transactionID string) (*TransactionLog, error) {
	userCollection := u.db.Collection("transactionLog")
	var transactionLog TransactionLog
	err := userCollection.FindOne(ctx, bson.M{"_id": transactionID}).Decode(transactionLog)
	if err != nil {
		return nil, err
	}
	return &transactionLog, nil
}

func (u *UserRepo) CreateTransactionLog(ctx context.Context, senderID, recieverID string, amount int) (*TransactionLog, error) {
	transactionCollection := u.db.Collection("transactionLog")
	transactionLog := NewTransactionLog(senderID, recieverID, amount)

	result, err := transactionCollection.InsertOne(ctx, transactionLog)
	if err != nil {
		return nil, err
	}
	transactionLog.ID = result.InsertedID.(primitive.ObjectID)

	return &TransactionLog{}, nil
}
