// firestore utils provides functions that allow for easy client creation
//
// It also provides usefule generic functions for accessing and reading collections
package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	"google.golang.org/api/iterator"
)

// Firestore utils contains functions that helps interact with firestore databases
type FirestoreUtils interface {
	GetFirestoreClient() *firestore.Client //creates a firestore client
}

type firestoreUtils struct {
	firestoreClient    *firestore.Client
	firstoreClientOnce sync.Once
	projectID          string
}

// creates a new firestore utils object
func NewFirestoreUtils(projectID string) FirestoreUtils {
	return &firestoreUtils{projectID: projectID}
}

// Greates a new firestore client
func (f *firestoreUtils) GetFirestoreClient() *firestore.Client {
	if f.firestoreClient == nil {
		f.firstoreClientOnce.Do(func() {
			fsClient, err := firestore.NewClient(context.Background(), f.projectID)
			if err != nil {
				log.Error("%s", err)
			}
			f.firestoreClient = fsClient
		})
	}

	return f.firestoreClient
}

// ReadDocumentsToArray reads data from a collection and returns all objects in an array
//
// Takes any type, name of a collection, a an object that implements the Firestore utils interface.
// This is to enforce that only this can only be called if a firestore client has been created.
// Defines an array of T
// Reads all documents in the collection.
// attempts to parse the document to type T and appends it to the array
// Returns the array
func ReadDocumentsToArray[T interface{}](collectionName string, f FirestoreUtils, ctx context.Context) ([]T, error) {

	db := f.GetFirestoreClient()

	iter := db.Collection(collectionName).Documents(ctx)

	var collectionInfo []T

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return collectionInfo, err
		}
		var document T
		err = doc.DataTo(&document)
		if err != nil {
			return collectionInfo, err
		}

		collectionInfo = append(collectionInfo, document)
	}

	return collectionInfo, nil

}
