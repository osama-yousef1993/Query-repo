package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
)

type FireStoreUtils interface {
	GetFirestoreClient() (*firestore.Client, error)
}

type fireStoreUtils struct {
	fsClient     *firestore.Client
	fsClientOnce sync.Once
	projectId    string
}

func NewFirestoreUtils(projectId string) FireStoreUtils {
	return &fireStoreUtils{projectId: projectId}
}

func (f *fireStoreUtils) GetFirestoreClient() (*firestore.Client, error) {
	if f.fsClient == nil {
		f.fsClientOnce.Do(func() {
			fsClient, err := firestore.NewClient(context.Background(), f.projectId)
			if err != nil {
				log.Error("%s", err)
			}
			f.fsClient = fsClient
		})
	}
	return f.fsClient, nil
}
