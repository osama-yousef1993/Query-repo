package repository

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/dto"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type EditorsPickQuery interface {
	GetEditorsPick(ctx context.Context) (*datastruct.EditorsPick, error)                                              // Get EditorsPick Data from FS
	BuildEditorsPicksData(ctx context.Context) (*datastruct.EditorsPick, error)                                       // Build EditorsPick Data from ForbesAPI
	SaveEditorsPicks(ctx context.Context, editorsPicksData interface{}) error                                         // Save EditorsPick Data to FS
	GetAuthorGroup(ctx context.Context, primaryAuthorId string, publicationId string) (datastruct.AuthorGroup, error) // Get AuthorGroup Data From BQ
	FetchEditorsPicks(ctx context.Context) (*datastruct.NewsFeedPayload, error)                                       // Get EditorsPick Data From BQ
}

type editorsPickQuery struct{}

// GetEditorsPick Gets all content for EditorsPick
// Takes a context
// Returns (*datastruct.EditorsPick, error)
//
// Gets the EditorsPick data from firestore
// Returns the EditorsPick content and no error if successful
func (e *editorsPickQuery) GetEditorsPick(ctx context.Context) (*datastruct.EditorsPick, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 EditorsPickQuery.GetEditorsPick", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetEditorsPick"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetEditorsPick"))

	var editorsPicksData datastruct.EditorsPick

	dbSnap, dataSnapErr := fs.Collection(datastruct.EditorsPickCollection).Doc("editorsPick_data").Get(ctx)

	if dataSnapErr != nil {
		log.Error("Error V2 EditorsPickQuery.GetEditorsPick Getting Data from FS: %s", dataSnapErr)
		return nil, dataSnapErr
	}

	if err := dbSnap.DataTo(&editorsPicksData); err != nil {
		log.Error("Error V2 EditorsPickQuery.GetEditorsPick Mapping Data from FS: %s", err)
		return nil, err
	}
	log.EndTimeL(labels, "V2 EditorsPickQuery.GetEditorsPick", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EditorsPickQuery.GetEditorsPick")
	return &editorsPicksData, nil

}

// BuildEditorsPicksData Gets all content for EditorsPick from ForbesAPI
// Takes a context
// Returns (*datastruct.EditorsPick, error)
//
// Gets the EditorsPick data from ForbesAPI
// Returns the EditorsPick content and no error if successful
func (e *editorsPickQuery) BuildEditorsPicksData(ctx context.Context) (*datastruct.EditorsPick, error) {
	span, labels := common.GenerateSpan("V2 EditorsPickQuery.BuildEditorsPicksData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.BuildEditorsPicksData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.BuildEditorsPicksData"))

	var editorsPicksData *datastruct.EditorsPick

	url := fmt.Sprintf("%s%s", dto.ApiURL, "source/more.json?limit=15&source=stream&sourceType=channelEditorsPick&sourceValue=channel_115")

	editorsPicksData, err := common.MakeForbesAPIRequest[datastruct.EditorsPick](ctx, url, "GET")
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.BuildEditorsPicksData Getting Data from ForbesAPI: %s", err)
		return nil, err
	}

	e.SaveEditorsPicks(ctx, editorsPicksData)

	log.EndTimeL(labels, "V2 EditorsPickQuery.BuildEditorsPicksData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EditorsPickQuery.BuildEditorsPicksData")

	return editorsPicksData, nil
}

// SaveEditorsPicks save EditorsPick content to FS
// Takes a context and editorsPicks data
// Returns  error
//
// Save the EditorsPick data to FS
// Returns error if the save process failed and nil if the save process successful
func (e *editorsPickQuery) SaveEditorsPicks(ctx context.Context, editorsPicksData interface{}) error {
	span, labels := common.GenerateSpan("V2 EditorsPickQuery.SaveEditorsPicks", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.SaveEditorsPicks"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.SaveEditorsPicks"))

	client := fsUtils.GetFirestoreClient()

	client.Collection(datastruct.EditorsPickCollection).Doc("editorsPick_data").Set(ctx, editorsPicksData)
	log.EndTimeL(labels, "V2 EditorsPickQuery.SaveEditorsPicks", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EditorsPickQuery.SaveEditorsPicks")
	return nil
}

// GetAuthorGroup Get AuthorGroup from BQ
// Takes a context, primaryAuthorId and publicationId
// Returns  (datastruct.AuthorGroup, error)
//
// Get AuthorGroup Data frm BQ
// Returns datastruct.AuthorGroup and no error if successful
func (e *editorsPickQuery) GetAuthorGroup(ctx context.Context, primaryAuthorId string, publicationId string) (datastruct.AuthorGroup, error) {
	span, labels := common.GenerateSpan("V2 EditorsPickQuery.GetAuthorGroup", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetAuthorGroup"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetAuthorGroup"))

	var authorGroup datastruct.AuthorGroup
	var err error
	authorGroup.PrimaryAuthor, err = e.GetPrimaryAuthor(ctx, primaryAuthorId)
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.GetAuthorGroup Getting PrimaryAuthor Data from FS: %s", err)
		return authorGroup, err
	}

	authorGroup.Publication, err = e.GetPublication(ctx, publicationId)
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.GetAuthorGroup Getting Publication Data from FS: %s", err)
		return authorGroup, err
	}
	log.EndTimeL(labels, "V2 EditorsPickQuery.GetAuthorGroup", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EditorsPickQuery.GetAuthorGroup")
	return authorGroup, nil
}

// GetPrimaryAuthor Get PrimaryAuthor from BQ
// Takes a context and primaryAuthorId
// Returns  (datastruct.PrimaryAuthor, error)
//
// Get PrimaryAuthor Data frm BQ
// Returns datastruct.PrimaryAuthor and no error if successful
func (e *editorsPickQuery) GetPrimaryAuthor(ctx context.Context, primaryAuthorId string) (datastruct.PrimaryAuthor, error) {
	span, labels := common.GenerateSpan("V2 EditorsPickQuery.GetPrimaryAuthor", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetPrimaryAuthor"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetPrimaryAuthor"))

	var primaryAuthor datastruct.PrimaryAuthor
	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.GetPrimaryAuthor Open Connection To BQ: %s", err)
		return primaryAuthor, err
	}

	primaryAuthorQueryResult := client.Query(datastruct.PrimaryAuthorQuery)
	primaryAuthorQueryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "id",
			Value: primaryAuthorId,
		},
	}

	primaryAuthorQueryIT, err := primaryAuthorQueryResult.Read(ctx)
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.GetPrimaryAuthor Getting PrimaryAuthor Data from BQ: %s", err)
		return primaryAuthor, err
	}
	for {
		err := primaryAuthorQueryIT.Next(&primaryAuthor)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error V2 EditorsPickQuery.GetPrimaryAuthor Mapping PrimaryAuthor Data from BQ: %s", err)
			return primaryAuthor, err
		}

	}
	log.EndTimeL(labels, "V2 EditorsPickQuery.GetPrimaryAuthor", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EditorsPickQuery.GetPrimaryAuthor")
	return primaryAuthor, nil
}

// GetPublication Get Publication from BQ
// Takes a context and publicationId
// Returns  (datastruct.Publication, error)
//
// Get Publication Data frm BQ
// Returns datastruct.Publication and no error if successful
func (e *editorsPickQuery) GetPublication(ctx context.Context, publicationId string) (datastruct.Publication, error) {

	span, labels := common.GenerateSpan("V2 EditorsPickQuery.GetPublication", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetPublication"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EditorsPickQuery.GetPublication"))

	var publication datastruct.Publication

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.GetPublication Open Connection To BQ: %s", err)
		return publication, err
	}

	publicationQueryResult := client.Query(datastruct.PublicationQuery)

	publicationQueryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "id",
			Value: publicationId,
		},
	}

	publicationQueryIT, err := publicationQueryResult.Read(ctx)
	if err != nil {
		log.Error("Error V2 EditorsPickQuery.GetPublication Getting Publication Data from BQ: %s", err)
		return publication, err
	}
	for {
		err := publicationQueryIT.Next(&publication)

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error V2 EditorsPickQuery.GetPublication Mapping Publication Data from BQ: %s", err)
			return publication, err
		}
	}
	log.EndTimeL(labels, "V2 EditorsPickQuery.GetPublication", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EditorsPickQuery.GetPublication")
	return publication, nil
}

// FetchEditorsPicks Get EditorsPick from BQ
// Takes a context
// Returns  (*datastruct.NewsFeedPayload, error)
//
// Get EditorsPick Data frm BQ
// Returns datastruct.NewsFeedPayload and no error if successful
func (e *editorsPickQuery) FetchEditorsPicks(ctx context.Context) (*datastruct.NewsFeedPayload, error) {

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		return nil, err
	}

	q := client.Query(datastruct.EditorsPickQuery)

	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	var newsFeedPayload datastruct.NewsFeedPayload
	for {
		var newsFeedItem datastruct.NewsFeedItem
		var newsFeedResponse datastruct.NewsFeedResponse
		err := it.Next(&newsFeedItem)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		newsFeedResponse.Date = newsFeedItem.Date.Timestamp
		newsFeedResponse.Image = newsFeedItem.Image.StringVal
		newsFeedResponse.URI = newsFeedItem.URI.StringVal
		newsFeedResponse.Title = newsFeedItem.Title.StringVal
		newsFeedResponse.PrimaryAuthor = newsFeedItem.PrimaryAuthor.StringVal
		newsFeedResponse.Publication = newsFeedItem.Publication.StringVal
		newsFeedResponse.Author.Name = newsFeedItem.Author.StringVal
		newsFeedResponse.Author.AuthorType = newsFeedItem.AuthorType.StringVal
		newsFeedResponse.Author.Type = newsFeedItem.AuthType.StringVal
		newsFeedResponse.Author.SeniorContributor = newsFeedItem.SeniorContributor.Bool
		newsFeedResponse.Author.Slug = newsFeedItem.AuthorSlug.StringVal

		newsFeedPayload.NewsFeedItems = append(newsFeedPayload.NewsFeedItems, newsFeedResponse)
	}
	return &newsFeedPayload, nil

}
