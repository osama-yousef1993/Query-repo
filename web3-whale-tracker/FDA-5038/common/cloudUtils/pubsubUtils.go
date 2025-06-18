package cloudUtils

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/Forbes-Media/go-tools/log"
)

type PubSubUtils interface {
	GetPubSubClient() (*pubsub.Client, error)
}

type pubSubUtils struct {
	pubsubClient     *pubsub.Client
	pubsubClientOnce sync.Once
	projectID        string
}

func NewPubSubUtils(projectID string) PubSubUtils {
	return &pubSubUtils{projectID: projectID}
}

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
