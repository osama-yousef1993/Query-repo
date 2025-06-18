package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
)

// Bigquery utils contains functions that helps interact with Bigquery databases
type FirestoreUtils interface {
	GetFirestoreClient() *firestore.Client //creates a bigquery client
}

type firestoreUtils struct {
	firestoreClient    *firestore.Client
	firstoreClientOnce sync.Once
}

// creates a new bigquery utils object
func NewFirestoreUtils() FirestoreUtils {
	return &firestoreUtils{}
}

// creates a firestore client and sync it using sync.Once instead of creating it everytime we call the function
func (f *firestoreUtils) GetFirestoreClient() *firestore.Client {
	if f.firestoreClient == nil {
		f.firstoreClientOnce.Do(func() {
			fsClient, err := firestore.NewClient(context.Background(), "digital-assets-301018")
			if err != nil {
				log.Error("%s", err)
			}
			f.firestoreClient = fsClient
		})
	}

	return f.firestoreClient
}
