package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/Forbes-Media/go-tools/log"
)

type PubSubUtils interface {
	GetPubSubClient() (*pubsub.Client, error) // Get the PubSub Client
}

// Object for the pubSubUtils
type pubSubUtils struct {
	pubsubClient     *pubsub.Client
	pubsubClientOnce sync.Once
	projectID        string
}

// NewPubSubUtils
// Takes projectID and returns PubSubUtils interface
// Allow us to access the function fot BQ connection.
func NewPubSubUtils(projectID string) PubSubUtils {
	return &pubSubUtils{projectID: projectID}
}

// GetPubSubClient
// Create the PubSub connection Client
// Returns *pubsub.Client to use it for the connection and  error if the process failed
func (p *pubSubUtils) GetPubSubClient() (*pubsub.Client, error) {
	if p.pubsubClient == nil {
		p.pubsubClientOnce.Do(func() {
			client, err := pubsub.NewClient(context.Background(), p.projectID)
			if err != nil {
				log.Error("%s", err)
			}
			p.pubsubClient = client
		})
	}

	return p.pubsubClient, nil
}
