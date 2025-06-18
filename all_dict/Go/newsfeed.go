package services

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/model"
	"google.golang.org/api/iterator"
)

// query the latest news about crypto and blockchain in the last 7 days
const query = `
SELECT
  c.title,
  c.type,
  c.date date,
  c.description,
  c.templateType,
  c.templateSubType,
  c.image,
  c.author,
  c.authorType,
  aut.type authType,
  aut.inactive disabled,
  aut.seniorContributor,
  c.siteSlug authorSlug,
  REPLACE(c.uri, "http://", "https://") AS uri,
FROM
  api-project-901373404215.Content.mv_content_latest c,
  UNNEST(c.channelSection) as channelSection
LEFT JOIN
  api-project-901373404215.Content.v_author_latest aut
ON
  c.authorNaturalId = aut.naturalId
LEFT JOIN
  api-project-901373404215.Content.channelsection cs
ON
  c.primaryChannelId = cs.channelId
  AND c.primarySectionId = cs.sectionId
WHERE
  c.visible = TRUE
  AND c.preview = FALSE
  AND c.date <= CURRENT_TIMESTAMP()
  AND c.timestamp <= CURRENT_TIMESTAMP()
  AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
  AND "all" NOT IN UNNEST(spikeFrom)
  AND ( 
    channelSection in ('channel_115') 
    or
    c.primaryChannelId= "channel_115" #Forbes Digital Assets
   )
  # AND c.primarySectionId = "section_1095" #crypto & blockchain
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  cs.sectionName
ORDER BY
  date DESC,
  c.title DESC
LIMIT
  21
`

type NewsFeedPayload struct {
	NewsFeedItems []NewsFeedResponse `json:"newsFeedItems" firestore:"newsFeedItems"`
}

type NewsFeedResponse struct {
	Title         string    `json:"title" firestore:"title"`
	URI           string    `json:"uri" firestore:"uri"`
	Date          time.Time `json:"date" firestore:"date"`
	Description   string    `json:"description" firestore:"description"`
	Image         string    `json:"image" firestore:"image"`
	PrimaryAuthor string    `json:"primaryAuthor" firestore:"primaryAuthor"`
	Publication   string    `json:"publication" firestore:"publication"`
	Author        struct {
		AuthorType        string `json:"authorType" firestore:"authorType"`
		Slug              string `json:"slug" firestore:"slug"`
		Type              string `json:"type" firestore:"type"`
		SeniorContributor bool   `json:"seniorContributor" firestore:"seniorContributor"`
		Name              string `json:"name" firestore:"name"`
		Disabled          bool   `json:"disabled" firestore:"disabled"`
	} `json:"author" firestore:"author"`
}

type NewsFeedItem struct {
	Title             bigquery.NullString    `json:"title" bigquery:"title"`
	URI               bigquery.NullString    `json:"uri" bigquery:"uri"`
	Date              bigquery.NullTimestamp `json:"date" bigquery:"date"`
	Description       bigquery.NullString    `json:"description" bigquery:"description"`
	Author            bigquery.NullString    `json:"author" bigquery:"author"`
	Image             bigquery.NullString    `json:"image" bigquery:"image"`
	AuthorType        bigquery.NullString    `json:"authorType" bigquery:"authorType"`
	AuthorSlug        bigquery.NullString    `json:"authorSlug" bigquery:"authorSlug"`
	AuthType          bigquery.NullString    `json:"authType" bigquery:"authType"`
	SeniorContributor bigquery.NullBool      `json:"seniorContributor" bigquery:"seniorContributor"`
	Disabled          bigquery.NullBool      `json:"disabled" bigquery:"disabled"`
	PrimaryAuthor     bigquery.NullString    `json:"primaryAuthor" bigquery:"primaryAuthor"`
	Publication       bigquery.NullString    `json:"publication" bigquery:"publication"`
}

// gets the latest news and save it to firestore
func UpdateNewsFeed() error {
	newsFeed, err := GetNewsFeed()
	if err != nil {
		return err
	}

	err = SaveNewsFeed(newsFeed)
	if err != nil {
		return err
	}

	return nil
}

// gets the latest news from BQ
func GetNewsFeed() (*NewsFeedPayload, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(context.Background(), "api-project-901373404215")
	if err != nil {
		return nil, err
	}

	q := client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	var newsFeedPayload NewsFeedPayload
	for {
		var newsFeedItem NewsFeedItem
		var newsFeedResponse NewsFeedResponse
		err := it.Next(&newsFeedItem)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		newsFeedResponse.Date = newsFeedItem.Date.Timestamp
		newsFeedResponse.Image = newsFeedItem.Image.StringVal
		newsFeedResponse.Description = newsFeedItem.Description.StringVal
		newsFeedResponse.URI = newsFeedItem.URI.StringVal
		newsFeedResponse.Title = newsFeedItem.Title.StringVal
		newsFeedResponse.Author.Name = newsFeedItem.Author.StringVal
		newsFeedResponse.Author.AuthorType = newsFeedItem.AuthorType.StringVal
		newsFeedResponse.Author.Type = newsFeedItem.AuthType.StringVal
		newsFeedResponse.Author.SeniorContributor = newsFeedItem.SeniorContributor.Bool
		newsFeedResponse.Author.Slug = newsFeedItem.AuthorSlug.StringVal

		newsFeedPayload.NewsFeedItems = append(newsFeedPayload.NewsFeedItems, newsFeedResponse)
	}
	return &newsFeedPayload, nil
}

// gets latests news from firestore
func GetCachedNewsFeed() (*NewsFeedPayload, error) {
	var newsFeedPayload NewsFeedPayload
	ctx := context.Background()
	fsClient, err := firestore.NewClient(ctx, "digital-assets-301018")
	if err != nil {
		return &newsFeedPayload, err
	}

	collectionName := fmt.Sprintf("pagedata%s", os.Getenv("DATA_NAMESPACE"))
	ds, err := fsClient.Collection(collectionName).Doc("newsfeed").Get(ctx)

	if err != nil {
		return &newsFeedPayload, err
	}

	err = ds.DataTo(&newsFeedPayload)
	if err != nil {
		return &newsFeedPayload, err
	}

	return &newsFeedPayload, nil
}

// saves latest news to firestore
func SaveNewsFeed(newsFeed *NewsFeedPayload) error {
	ctx := context.Background()
	fsClient, err := firestore.NewClient(ctx, "digital-assets-301018")
	if err != nil {
		return err
	}

	collectionName := fmt.Sprintf("pagedata%s", os.Getenv("DATA_NAMESPACE"))
	_, err = fsClient.Collection(collectionName).Doc("newsfeed").Set(ctx, newsFeed)
	if err != nil {
		return err
	}
	return nil
}

const editorsPickQuery = `
SELECT
	c.title,
	c.type,
	c.date date,
	c.templateType,
	c.templateSubType,
	c.image,
	c.author,
	c.authorType,
	aut.type authType,
	aut.inactive disabled,
	aut.seniorContributor,
	c.siteSlug authorSlug,
	c.authorGroup.primaryAuthor,
	c.authorGroup.publication,
	REPLACE(c.uri, "http://", "https://") AS uri,
FROM
	api-project-901373404215.Content.mv_content_latest c,
	UNNEST(c.bertieBadges) as bertieBadge
LEFT JOIN
	api-project-901373404215.Content.v_author_latest aut
ON
	c.authorNaturalId = aut.naturalId
LEFT JOIN
	api-project-901373404215.Content.channelsection cs
ON
	c.primaryChannelId = cs.channelId
	AND c.primarySectionId = cs.sectionId
WHERE
	c.visible = TRUE
	AND c.preview = FALSE
	AND c.date <= CURRENT_TIMESTAMP()
	AND c.timestamp <= CURRENT_TIMESTAMP()
	AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
	AND "all" NOT IN UNNEST(spikeFrom)
	AND bertieBadge in ("Editors' Pick")
	AND c.primaryChannelId= "channel_115" #Forbes Digital Assets
GROUP BY
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	11,
	12,
	13,
	14,
	15,
	cs.sectionName
ORDER BY
	date DESC,
	c.title DESC
LIMIT
	15
`

func FetchEditorsPicks(ctx context.Context) (*NewsFeedPayload, error) {

	client, err := bigquery.NewClient(ctx, "api-project-901373404215")
	if err != nil {
		return nil, err
	}

	q := client.Query(editorsPickQuery)

	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	var newsFeedPayload NewsFeedPayload
	for {
		var newsFeedItem NewsFeedItem
		var newsFeedResponse NewsFeedResponse
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

func SaveEditorsPicks(ctx context.Context, newsFeed interface{}) error {

	client, err := firestore.NewClient(ctx, "digital-assets-301018")
	if err != nil {
		return err
	}

	editorsPickCollection := fmt.Sprintf("editorsPicks%s", os.Getenv("DATA_NAMESPACE"))

	_, err = client.Collection(editorsPickCollection).Doc("editorsPick_data").Set(ctx, newsFeed)
	if err != nil {
		return err
	}

	return nil
}

const primaryAuthorQuery = `
SELECT 
	id, 
	name, 
	naturalId, 
	avatars, 
	url, 
	type, 
	profileUrl, 
	twitterName, 
	authorType, 
	linkedIn,
	email, 
	blog,
	CAST(UNIX_SECONDS(timestamp) AS INT64 ) as timestamp,
	shortBio,
	blogName,
	description,
	primaryBlogNaturalId,
	slug,
	showNoVestPocket,
	embargo,
	primaryChannelId,
	primarySectionId,
	enableContribContact,
	enableTwitterFeed,
	disableCanonical,
	disableDigest,
	inactive,
	division,
	allowEmail,
	seniorContributor,
	disabled
FROM 
	api-project-901373404215.Content.v_author_latest 
where 
	id = @id 
LIMIT 
	1
`

const publicationQuery = `
SELECT 
	id,
	naturalId,
	name,
	avatars,
	url,
	type,
	authorType,
	blog,
	CAST(UNIX_SECONDS(timestamp) AS INT64 ) as timestamp,
	primaryContributor,
	primaryContributorData,
	contributorSince,
	slug,
	showNoVestPocket,
	primaryChannelId,
	primarySectionId,
	enableContribContact,
	enableTwitterFeed,
	disableCanonical,
	disableDigest,
	allowEmail
FROM 
	api-project-901373404215.Content.author_stage
where 
	id = @id 
	AND primaryChannelId = 'channel_115' 
LIMIT 
	1
`

func GetAuthorGroup(ctx context.Context, primaryAuthorId string, publicationId string) (model.AuthorGroup, error) {

	var authorGroup model.AuthorGroup
	var err error
	authorGroup.PrimaryAuthor, err = GetPrimaryAuthor(ctx, primaryAuthorId)
	if err != nil {
		return authorGroup, err
	}

	authorGroup.Publication, err = GetPublication(ctx, publicationId)
	if err != nil {
		return authorGroup, err
	}

	return authorGroup, nil
}

func GetPrimaryAuthor(ctx context.Context, primaryAuthorId string) (model.PrimaryAuthor, error) {
	var primaryAuthor model.PrimaryAuthor
	client, err := bigquery.NewClient(ctx, "api-project-901373404215")
	if err != nil {
		return primaryAuthor, err
	}

	primaryAuthorQueryResult := client.Query(primaryAuthorQuery)
	primaryAuthorQueryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "id",
			Value: primaryAuthorId,
		},
	}

	primaryAuthorQueryIT, err := primaryAuthorQueryResult.Read(ctx)
	if err != nil {
		return primaryAuthor, err
	}
	for {
		err := primaryAuthorQueryIT.Next(&primaryAuthor)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return primaryAuthor, err
		}

	}

	return primaryAuthor, nil
}

func GetPublication(ctx context.Context, publicationId string) (model.Publication, error) {

	var publication model.Publication

	client, err := bigquery.NewClient(ctx, "api-project-901373404215")
	if err != nil {
		return publication, err
	}

	publicationQueryResult := client.Query(publicationQuery)

	publicationQueryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "id",
			Value: publicationId,
		},
	}

	publicationQueryIT, err := publicationQueryResult.Read(ctx)
	if err != nil {
		return publication, err
	}
	for {
		err := publicationQueryIT.Next(&publication)

		if err == iterator.Done {
			break
		}
		if err != nil {
			return publication, err
		}
	}
	return publication, nil
}

