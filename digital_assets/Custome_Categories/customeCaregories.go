package store

import (
	"context"
	"net/http"
)


type TableName string

const (
	Assets TableName = "fundamentalslatest"
	NFTs TableName = "nftdatalatest"
	Cat TableName = "categories_fundamentals"
)

type RequestFields struct {
	TableName string `json:"table_name"`
	Assets []string `json:"assets"`
	CategoryName string `json:"category_name"`
	Limit int `json:"limit"`
	Sort int `json:"sort"`
	Value int `json:"value"`
	Condition int `json:"condition"`
	ConditionValue int `json:"condition_value"`

}

/*
	- BuildRequest checker function
	- BuildQueryBuilder function
	- BuildMap function to build the object
	- BuildJson response function
*/

func RequestChecker(ctx context.Context, fields RequestFields) error {
	if fields.CategoryName == "" {

	}
}