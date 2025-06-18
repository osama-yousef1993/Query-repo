package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/go-tools/log"
)

// Bigquery utils contains functions that helps interact with Bigquery databases
type BigqueryUtils interface {
	GetBigQueryClient() (*bigquery.Client, error) //creates a bigquery client
}

type bigqueryUtils struct {
	bqClient     *bigquery.Client
	BQClientOnce sync.Once
	projectID    string
}

// creates a new bigquery utils object
func NewBigqueryUtils(projectID string) BigqueryUtils {
	return &bigqueryUtils{projectID: projectID}
}

// creating BQ client and sync it using sync.Once instead of creating it everyTime we call the function
func (b *bigqueryUtils) GetBigQueryClient() (*bigquery.Client, error) {
	if b.bqClient == nil {
		b.BQClientOnce.Do(func() {
			client, err := bigquery.NewClient(context.Background(), b.projectID)
			if err != nil {
				log.Error("%s", err)
			}
			b.bqClient = client
		})
	}
	return b.bqClient, nil
}
