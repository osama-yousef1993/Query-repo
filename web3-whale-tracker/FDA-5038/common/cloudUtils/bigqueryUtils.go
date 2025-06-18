package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/go-tools/log"
)

type BigQueryUtils interface {
	GetBigQueryClient() (*bigquery.Client, error)
}

type bigQueryUtils struct {
	bqClient     *bigquery.Client
	BQClientOnce sync.Once
	projectID    string
}

func NewBigQueryUtils(projectID string) BigQueryUtils {
	return &bigQueryUtils{projectID: projectID}
}

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
