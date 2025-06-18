package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/go-tools/log"
)

type BigQueryUtils interface {
	GetBigQueryClient() (*bigquery.Client, error) // Get the BiqQuery Client
}

// Object for the bigQueryUtils
type bigQueryUtils struct {
	bqClient     *bigquery.Client
	BQClientOnce sync.Once
	projectID    string
}

// NewBigQueryUtils 
// Takes projectID and returns BigQueryUtils interface
// Allow us to access the function fot BQ connection.
func NewBigQueryUtils(projectID string) BigQueryUtils {
	return &bigQueryUtils{projectID: projectID}
}

// GetBigQueryClient
// Create the BQ connection Client
// Returns *bigquery.Client to use it for the connection and  error if the process failed
func (b *bigQueryUtils) GetBigQueryClient() (*bigquery.Client, error) {
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
