package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/dghubble/oauth1"
	"go.opentelemetry.io/otel/codes"
)

type TwitterServices interface {
	PublishTwitterPost(ctx context.Context, body datastruct.RequestBody) ([]byte, error)
}

type twitterServices struct {
	dao repository.DAO
}

func NewTwitterServices(dao repository.DAO) TwitterServices {
	return &twitterServices{dao: dao}
}

func (t *twitterServices) PublishTwitterPost(ctx context.Context, body datastruct.RequestBody) ([]byte, error) {
	span, labels := common.GenerateSpan("PublishTwitterPost", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "PublishTwitterPost"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "PublishTwitterPost"))
	queryMGR := t.dao.NewTwitterQuery()

	// OAuth1 authentication setup
	// we will use forbes ConsumerKey and ConsumerSecret
	config := oauth1.NewConfig(datastruct.ConsumerKey, datastruct.ConsumerSecret)
	// token := oauth1.NewToken(datastruct.AccessToken, datastruct.AccessSecret)
	token := oauth1.NewToken(datastruct.AccessToken, datastruct.AccessSecret)

	httpClient := config.Client(oauth1.NoContext, token)

	// filePath, err := queryMGR.DownloadImage(ctx, datastruct.ImageURL)
	filePath, err := queryMGR.DownloadImage(ctx, body.ImageURL)
	if err != nil {
		log.Error("Failed to download image: %v", err)
	}
	defer os.Remove(filePath)

	// Step 2: Upload the image
	// mediaID, err := queryMGR.UploadMedia(ctx, httpClient, filePath, datastruct.AccessToken)
	mediaID, err := queryMGR.UploadMedia(ctx, httpClient, filePath, body.AccessToken)
	if err != nil {
		log.Error("Failed to upload media: %v", err)
		return nil, err
	}

	// tweetText := "Here is an image"
	// response, err := queryMGR.PostTweet(ctx, httpClient, tweetText, mediaID)
	response, err := queryMGR.PostTweet(ctx, httpClient, body.Text, mediaID)
	if err != nil {
		log.Error("Failed to upload media: %v", err)
		return nil, err
	}
	res, err := json.Marshal(response)
	if err != nil {
		log.Error("Failed to upload media: %v", err)
		return nil, err
	}

	log.EndTimeL(labels, "UploadMedia", startTime, nil)
	span.SetStatus(codes.Ok, "UploadMedia")
	return res, nil

}
