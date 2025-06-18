
x := `SELECT
c.contentPaywall,
c.naturalId,
c.id,
c.title,
c.date date,
c.description,
c.image,
c.author,
c.authorType author_type,
aut.type type,
aut.inactive disabled,
aut.seniorContributor senior_contributor,
aut.bylineFormat byline_format,
REPLACE(c.uri, "http://", "https://") AS link,
REPLACE(aut.url, "http://", "https://") AS author_link,
bertieTag
FROM
api-project-901373404215.Content.mv_content_latest c,
UNNEST(c.channelSection) AS channelSection,
UNNEST(c.bertieBadges) AS bertieTag
LEFT JOIN
api-project-901373404215.Content.v_author_latest aut
ON
c.authorNaturalId = aut.naturalId
WHERE
c.visible = TRUE
AND c.preview = FALSE
-- AND c.date <= CURRENT_TIMESTAMP()
-- AND c.timestamp <= CURRENT_TIMESTAMP()
-- AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
AND "all" NOT IN UNNEST(spikeFrom)
AND ( 
	  c.primaryChannelId = "channel_115"
	  OR 
	  channelSection = "channel_115"
	)
  And contentPaywall = 'premium'
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
16
order by c.date desc


`
// https://staging-a.forbesapi.forbes.com/forbesapi/contents/stream/channel_115.json?limit=12


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// firestore
// Add Premium Articles with all its data to FS
func SaveRecommendedPremiumArticles(ctx0 context.Context, articles []services.EducationArticle) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveRecommendedPremiumArticles")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "premium_articles")
		for _, article := range articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			doc["naturalid"] = article.NaturalID
			//if there is no natural id dont store the article
			if article.NaturalID != "" {
				fs.Collection(collectionName).Doc(strings.ReplaceAll(article.NaturalID, "/", "_")).Set(ctx, doc, firestore.MergeAll)
			}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// premium articles 
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

const premiumArticlesQuery = `
	SELECT distinct
		c.id,
		c.title,
		c.date date,
		c.description,
		c.image,
		c.author,
		c.authorType author_type,
		c.naturalId,
		aut.type type,
		aut.inactive disabled,
		aut.seniorContributor senior_contributor,
		aut.bylineFormat byline_format,
		REPLACE(c.uri, "http://", "https://") AS link,
		REPLACE(aut.url, "http://", "https://") AS author_link
	FROM
		api-project-901373404215.Content.mv_content_latest c,
		UNNEST(c.channelSection) AS channelSection,
		UNNEST(c.bertieBadges) AS bertieTag
	LEFT JOIN
		api-project-901373404215.Content.v_author_latest aut
	ON
		c.authorNaturalId = aut.naturalId
	WHERE
		c.visible = TRUE
		AND c.preview = FALSE
		AND "all" NOT IN UNNEST(spikeFrom)
		AND ( 
			c.primaryChannelId = "channel_115"
			OR 
			channelSection = "channel_115"
			)
		And contentPaywall = 'premium'
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
		14
	order by 
		c.date desc
`

type ContentList struct {
	PremiumArticles []PremiumArticles `json:"contentList" firestore:"id"`

}
type PremiumArticles struct {
	Id                         string                     `json:"id" firestore:"id"`
	Title                      string                     `json:"title" firestore:"title"`
	NaturalID                  string                     `json:"naturalId" firestore:"naturalid"`
	Image                      string                     `json:"image" firestore:"image"`
	ArticleURL                 string                     `json:"uri" firestore:"articleURL"`
	Description                string                     `json:"description" firestore:"description"`
	PublishDate                time.Time                  `json:"date" firestore:"publishDate"`
	PremiumArticlesAuthorGroup PremiumArticlesAuthorGroup `json:"authorGroup" firestore:"authorGroup"`
}

type PremiumArticlesAuthorGroup struct {
	PrimaryAuthor PrimaryAuthor `json:"primaryAuthor" firestore:"primaryAuthor"`
}

// Get all premium articles from BQ and Build Articles Array to be inserted to FS
func BuildRecommendedPremiumArticles(ctx0 context.Context) ([]EducationArticle, error) {

	client := GetBQClient()

	ctx, span := tracer.Start(ctx0, "BuildRecommendedPremiumArticles")

	defer span.End()

	queryResult := client.Query(premiumArticlesQuery)

	it, err := queryResult.Read(ctx)

	if err != nil {
		log.Error("Error Getting Premium Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Premium Articles Data from BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var premiumArticles []EducationArticle
	span.AddEvent("Start Map Premium Articles Data")

	imageDomain := os.Getenv("ARTICLES_IMAGE_DOMAIN")

	for {
		var article EducationArticle
		var articleFromBQ EducationArticleFromBQ

		err := it.Next(&articleFromBQ)

		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Map Premium Articles Data to Struct: %s", err)
			span.AddEvent(fmt.Sprintf("Error Map Premium Articles Data to Struct: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		if articleFromBQ.Id.Valid {
			article.Id = articleFromBQ.Id.StringVal
		}
		if articleFromBQ.NaturalID.Valid {
			article.NaturalID = articleFromBQ.NaturalID.StringVal
		}
		if articleFromBQ.Title.Valid {
			article.Title = articleFromBQ.Title.StringVal
		}
		if articleFromBQ.Image.Valid {
			article.Image = imageDomain + articleFromBQ.Image.StringVal
		}
		if articleFromBQ.Author.Valid {
			article.Author = articleFromBQ.Author.StringVal
		}
		if articleFromBQ.AuthorLink.Valid {
			article.AuthorLink = articleFromBQ.AuthorLink.StringVal
		}
		if articleFromBQ.AuthorType.Valid {
			article.AuthorType = articleFromBQ.AuthorType.StringVal
		}
		if articleFromBQ.Description.Valid {
			article.Description = articleFromBQ.Description.StringVal
		}
		if articleFromBQ.ArticleURL.Valid {
			article.ArticleURL = articleFromBQ.ArticleURL.StringVal
		}
		if articleFromBQ.Type.Valid {
			article.Type = articleFromBQ.Type.StringVal
		}
		if articleFromBQ.Disabled.Valid {
			article.Disabled = articleFromBQ.Disabled.Bool
		}
		if articleFromBQ.SeniorContributor.Valid {
			article.SeniorContributor = articleFromBQ.SeniorContributor.Bool
		}
		if articleFromBQ.BertieTag.Valid {
			article.BertieTag = articleFromBQ.BertieTag.StringVal
		}
		if articleFromBQ.BylineFormat.Valid {
			article.BylineFormat = &articleFromBQ.BylineFormat.Int64
		} else {
			article.BylineFormat = nil
		}
		article.PublishDate = articleFromBQ.PublishDate

		premiumArticles = append(premiumArticles, article)

	}

	return premiumArticles, nil
}

func GetRecommendedPremiumArticles(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "GetRecommendedPremiumArticles")

	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "premium_articles")

	dbSnap := fs.Collection(collectionName).OrderBy("publishDate", firestore.Desc).Documents(ctx)

	span.AddEvent("Start Get Recommended Premium Articles Data from FS")

	var premiumArticles []EducationArticle
	for {
		var premiumArticle EducationArticle

		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&premiumArticle); err != nil {
			log.Error("Error Getting Recommended Premium Articles from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Recommended Premium Articles Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		premiumArticles = append(premiumArticles, premiumArticle)
	}

	result, err := json.Marshal(premiumArticles[0:12])
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil

}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Education
type EducationArticleFromBQ struct {
	Id                bigquery.NullString `bigquery:"id" json:"id" firestore:"id"`
	Title             bigquery.NullString `bigquery:"title" json:"title" firestore:"title"`
	Image             bigquery.NullString `bigquery:"image" json:"image" firestore:"image"`
	ArticleURL        bigquery.NullString `bigquery:"link" json:"articleURL" firestore:"articleURL"`
	Author            bigquery.NullString `bigquery:"author" json:"author" firestore:"author"`
	AuthorType        bigquery.NullString `bigquery:"author_type" json:"authorType" firestore:"authorType"`
	Type              bigquery.NullString `bigquery:"type" json:"type" firestore:"type"`
	AuthorLink        bigquery.NullString `bigquery:"author_link" json:"authorLink" firestore:"authorLink"`
	Description       bigquery.NullString `bigquery:"description" json:"description" firestore:"description"`
	PublishDate       time.Time           `bigquery:"date" json:"publishDate" firestore:"publishDate"`
	Disabled          bigquery.NullBool   `bigquery:"disabled" json:"disabled" firestore:"disabled"`
	SeniorContributor bigquery.NullBool   `bigquery:"senior_contributor" json:"seniorContributor" firestore:"seniorContributor"`
	BylineFormat      bigquery.NullInt64  `bigquery:"byline_format" json:"bylineFormat" firestore:"bylineFormat"`
	BertieTag         bigquery.NullString `bigquery:"bertieTag" json:"bertieTag" firestore:"bertieTag"`
	NaturalID         bigquery.NullString `bigquery:"naturalId" json:"naturalId" firestore:"naturalId"`
}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


r.Handle("/build-premium-articles", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildPremiumArticles))).Methods(http.MethodPost)



// Build Premium articles from BQ each 5 min
func BuildPremiumArticles(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "BuildPremiumArticles")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildPremiumArticles"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Build Premium Articles")

	result, err := services.BuildRecommendedPremiumArticles(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	store.SaveRecommendedPremiumArticles(ctx, result)

	log.EndTimeL(labels, "Build Premium Articles", startTime, nil)
	span.SetStatus(codes.Ok, "Build Premium Articles")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// get the latest premium articles from FS
func GetPremiumArticles(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetPremiumArticles")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetPremiumArticles"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Premium Articles")

	_, err := services.GetPremiumArticlesAPI(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "Get Premium Articles", startTime, nil)
	span.SetStatus(codes.Ok, "Get Premium Articles")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}





// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

forbesURL       = "https://forbesapi.forbes.com/forbesapi/"

// get all premium articles from forbes API using FDA channel
func GetPremiumArticlesAPI(ctx context.Context) (*[]ContentList, error) {

	url := fmt.Sprintf("%s%s", forbesURL, "content/all.json?limit=100&queryfilters=%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22primaryChannelId%22%3A%5B%22channel_115%22%5D%7D%5D&retrievedfields=title,date,description,image,author,authorGroup,naturalId,primaryChannelId,type,uri")

	ContentList, err := MakeForbesAPIRequest[[]ContentList](ctx, url, "GET")
	if err != nil {
		return nil, err
	}
	return ContentList, nil
}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// firestore.go 
// Add Premium Articles with all its data to FS
func SaveRecommendedPremiumArticles(ctx0 context.Context, articles []services.EducationArticle) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveRecommendedPremiumArticles")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "premium_articles")
		for _, article := range articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			doc["naturalid"] = article.NaturalID
			//if there is no natural id dont store the article
			if article.NaturalID != "" {
				fs.Collection(collectionName).Doc(strings.ReplaceAll(article.NaturalID, "/", "_")).Set(ctx, doc, firestore.MergeAll)
			}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// forbesAPI.go


type ForbesAPIArticles struct {
	PremiumArticles []PremiumArticles `json:"contentList" firestore:"id"`
}
type PremiumArticles struct {
	Id                         string                     `json:"id" firestore:"id"`
	Title                      string                     `json:"title" firestore:"title"`
	NaturalID                  string                     `json:"naturalId" firestore:"naturalid"`
	Image                      string                     `json:"image" firestore:"image"`
	ArticleURL                 string                     `json:"uri" firestore:"articleURL"`
	Description                string                     `json:"description" firestore:"description"`
	PublishDate                int64                      `json:"date" firestore:"publishDate"`
	ContentPayWall             string                     `json:"contentPaywall" firestore:"contentPaywall"`
	PremiumArticlesAuthorGroup PremiumArticlesAuthorGroup `json:"authorGroup" firestore:"authorGroup"`
}

type PremiumArticlesAuthorGroup struct {
	PrimaryAuthor PrimaryAuthor `json:"primaryAuthor" firestore:"primaryAuthor"`
}

// get all premium articles from Forbes API using FDA channel
func GetPremiumArticlesDataFromForbesAPI(ctx context.Context) (*ForbesAPIArticles, error) {

	_, span := tracer.Start(ctx, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()

	span.AddEvent("Start Getting Premium Articles Data")

	/*
		QueryFilter that ForbesAPI Accept To got the premium Articles.
		- limit ---> the response limit.
		- queryfilters ---> it will be in this format [{"contentPaywall":["premium"]}, {"primaryChannelId":["channel_115"]}].
			- contentPaywall ---> we will use it because we need all the Premium Articles and it take value "premium".
			- primaryChannelId ---> we will use it to determine the channel Id data will return from it in our case it will be FDA channel "channel_115".
		- retrievedfields ---> it will present the fields will be included in response.
			- title --> article Title.
			- date --> article PublishDate.
			- description --> article Description.
			- image --> article Image.
			- author --> article author.
			- authorGroup --> authorGroup it will contains all author data that we need.
				- AuthorType --> author Type.
				- Badges --> Bertie Badges.
				- Name --> author Name.
				- SeniorContributor --> author SeniorContributor.
				- Disabled
				- BylineFormat
				- AuthorLink --> author AuthorLink.
				- Type --> author Type.
			- naturalId --> article NaturalId we need it to use it as unique Key.
			- primaryChannelId --> article PrimaryChannelId to check if the data back from FDA channel.
			- type --> article Type.
			- uri --> article URL.
			- contentPaywall --> article contentPaywall to check if this article is premium articles.
	*/
	url := fmt.Sprintf("%s%s", forbesURL, "content/all.json?limit=100&queryfilters=%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22primaryChannelId%22%3A%5B%22channel_115%22%5D%7D%5D&retrievedfields=title,date,description,image,author,authorGroup,naturalId,primaryChannelId,type,uri,contentPaywall")

	ContentList, err := MakeForbesAPIRequest[ForbesAPIArticles](ctx, url, "GET")
	if err != nil {
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	return ContentList, nil
}

// Build Premium Articles
// first got the Premium Articles fro ForbesAPI then map it to Articles Struct
func BuildRecommendedPremiumArticles(ctx0 context.Context) ([]EducationArticle, error) {
	ctx, span := tracer.Start(ctx0, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Build Premium Articles Data")

	data, err := GetPremiumArticlesDataFromForbesAPI(ctx)
	if err != nil {
		log.Error("Error Getting Premium Articles Data from ForbesAPI: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Premium Articles Data from ForbesAPI: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	result := MapPremiumArticlesDataFromForbesAPI(ctx, *data)
	return result, nil
}

// Map Premium Articles from ForbesAPI to Articles Struct
func MapPremiumArticlesDataFromForbesAPI(ctx0 context.Context, articles ForbesAPIArticles) []EducationArticle {

	_, span := tracer.Start(ctx0, "MapPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Map Premium Articles Data")
	var premiumArticles []EducationArticle

	for _, article := range articles.PremiumArticles {
		var premiumArticle EducationArticle
		premiumArticle.Id = article.Id
		premiumArticle.Title = article.Title
		premiumArticle.NaturalID = article.NaturalID
		premiumArticle.Image = article.Image
		premiumArticle.ArticleURL = article.ArticleURL
		premiumArticle.Description = article.Description
		premiumArticle.PublishDate = time.Unix(0, article.PublishDate*int64(time.Millisecond))
		premiumArticle.AuthorType = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorType
		premiumArticle.Author = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Name
		premiumArticle.SeniorContributor = article.PremiumArticlesAuthorGroup.PrimaryAuthor.SeniorContributor
		premiumArticle.Disabled = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Disabled
		premiumArticle.BylineFormat = &article.PremiumArticlesAuthorGroup.PrimaryAuthor.BylineFormat
		premiumArticle.AuthorLink = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorLink
		premiumArticle.Type = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Type
		premiumArticles = append(premiumArticles, premiumArticle)
	}

	span.AddEvent("Finished Map Premium Articles Data")
	return premiumArticles
}

func GetRecommendedPremiumArticles(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "GetRecommendedPremiumArticles")

	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "premium_articles")

	dbSnap := fs.Collection(collectionName).OrderBy("publishDate", firestore.Desc).Documents(ctx)

	span.AddEvent("Start Get Recommended Premium Articles Data from FS")

	var premiumArticles []EducationArticle
	for {
		var premiumArticle EducationArticle

		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&premiumArticle); err != nil {
			log.Error("Error Getting Recommended Premium Articles from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Recommended Premium Articles Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		premiumArticles = append(premiumArticles, premiumArticle)
	}

	result, err := json.Marshal(premiumArticles[0:12])
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil

}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// education.go
NaturalID         bigquery.NullString `bigquery:"naturalId" json:"naturalId" firestore:"naturalId"`
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// main.go
r.Handle("/build-premium-articles", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildPremiumArticles))).Methods(http.MethodPost)
v1.HandleFunc("/premium-articles", GetPremiumArticles).Methods(http.MethodGet, http.MethodOptions)


// Build Premium articles from BQ each 5 min
func BuildPremiumArticles(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "BuildPremiumArticles")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildPremiumArticles"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Build Premium Articles")

	result, err := services.BuildRecommendedPremiumArticles(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	store.SaveRecommendedPremiumArticles(ctx, result)

	log.EndTimeL(labels, "Build Premium Articles", startTime, nil)
	span.SetStatus(codes.Ok, "Build Premium Articles")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// get the latest premium articles from FS
func GetPremiumArticles(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetPremiumArticles")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetPremiumArticles"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Premium Articles")

	result, err := services.GetRecommendedPremiumArticles(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "Get Premium Articles", startTime, nil)
	span.SetStatus(codes.Ok, "Get Premium Articles")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}


x :=`
SELECT
c.contentPaywall,
bertieTag,
c.naturalId,
c.id,
c.title,
c.date date,
c.description,
c.image,
c.author,
c.authorType author_type,
aut.type type,
aut.inactive disabled,
aut.seniorContributor senior_contributor,
aut.bylineFormat byline_format,
REPLACE(c.uri, "http://", "https://") AS link,
REPLACE(aut.url, "http://", "https://") AS author_link

FROM
api-project-901373404215.Content.content c,
UNNEST(c.channelSection) AS channelSection,
UNNEST(c.bertieBadges) AS bertieTag
LEFT JOIN
api-project-901373404215.Content.v_author_latest aut
ON
c.authorNaturalId = aut.naturalId
WHERE
c.visible = TRUE
AND c.preview = FALSE
-- AND c.date <= CURRENT_TIMESTAMP()
-- AND c.timestamp <= CURRENT_TIMESTAMP()
-- AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
AND "all" NOT IN UNNEST(spikeFrom)
AND ( 
	  c.primaryChannelId = "channel_115"
	  OR 
	  channelSection = "channel_115"
	)
  And contentPaywall = 'premium'
  and c.naturalId = 'blogAndPostId/blog/post/7442-653ab7ddd648f14e94532e26'
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
16
order by c.date desc`


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
func BuildTagRecommendedPremiumArticles(ctx0 context.Context) (map[string][]EducationArticle, error) {
	ctx, span := tracer.Start(ctx0, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Build Premium Articles Data")

	// Get the Premium Articles from ForbesAPI with queryFiler that we need.
	data, err := GetPremiumArticlesDataFromForbesAPI(ctx)
	if err != nil {
		log.Error("Error Getting Premium Articles Data from ForbesAPI: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Premium Articles Data from ForbesAPI: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.AddEvent("Start Map Premium Articles Data")

	// The response is not structured as we need so once the data has been obtained, it needs to be mapped into EducationArticle objects.
	// So we created MapPremiumArticlesDataFromForbesAPI to map the response to our struct so we can store it in Rowy table.
	result := BuildPremiumArticlesCategories(ctx, *data)

	return result, nil
}

func MapPremiumArticleDataFromForbesAPI(ctx0 context.Context, article PremiumArticles) *EducationArticle {

	_, span := tracer.Start(ctx0, "MapPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Map Premium Articles Data")
	// loop over the ForbesPremiumArticles to map the data that we need from the response
	var premiumArticle EducationArticle
	premiumArticle.Id = article.Id
	premiumArticle.Title = article.Title
	premiumArticle.NaturalID = article.NaturalID
	premiumArticle.Image = article.Image
	premiumArticle.ArticleURL = article.ArticleURL
	premiumArticle.Description = article.Description
	// convert date from Unix format to timestamp format
	premiumArticle.PublishDate = time.Unix(0, article.PublishDate*int64(time.Millisecond))
	premiumArticle.AuthorType = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorType
	premiumArticle.Author = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Name
	premiumArticle.SeniorContributor = article.PremiumArticlesAuthorGroup.PrimaryAuthor.SeniorContributor
	premiumArticle.Disabled = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Disabled
	premiumArticle.BylineFormat = &article.PremiumArticlesAuthorGroup.PrimaryAuthor.BylineFormat
	premiumArticle.AuthorLink = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorLink
	premiumArticle.Type = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Type

	span.AddEvent("Finished Map Premium Articles Data")
	return &premiumArticle
}

func BuildBertieBadge(bertie BertieBadges) string {
	var tag string
	if strings.Contains(bertie.Slug, "-") {
		tag = bertie.DisplayName
	} else {
		tag = bertie.Slug
	}
	return tag

}

func BuildPremiumArticlesCategories(ctx0 context.Context, articles ForbesPremiumArticles) map[string][]EducationArticle {
	ctx, span := tracer.Start(ctx0, "MapPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Map Premium Articles Data")
	premiumArticles := make(map[string][]EducationArticle)

	for _, article := range articles.PremiumArticles {
		for _, tag := range article.BertieBadges {
			res := BuildBertieBadge(tag)
			art := MapPremiumArticleDataFromForbesAPI(ctx, article)
			art.BertieTag = res
			premiumArticles[res] = append(premiumArticles[res], *art)
		}

	}
	file, _ := json.MarshalIndent(premiumArticles, " ", "")
	_ = os.WriteFile("premiumArticles.json", file, 0644)

	return premiumArticles
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/HTTPGateway"
	"github.com/Forbes-Media/forbes-digital-assets/model"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/iterator"
)

const (
	apiURL          = "https://www.forbes.com/forbesapi/"
	userAgentHeader = "Web3 FDA-API/Go"
	forbesURL       = "https://forbesapi.forbes.com/forbesapi/"
)

// gets editors picks data from forbes apis and save it to firestore
func BuildEditorsPicksData(ctx context.Context) (*model.EditorsPick, error) {

	client := GetFirestoreClient()

	var editorsPicksData *model.EditorsPick

	url := fmt.Sprintf("%s%s", apiURL, "source/more.json?limit=15&source=stream&sourceType=channelEditorsPick&sourceValue=channel_115")

	editorsPicksData, err := MakeForbesAPIRequest[model.EditorsPick](ctx, url, "GET")
	if err != nil {
		return nil, err
	}

	editorsPickCollection := fmt.Sprintf("editorsPicks%s", os.Getenv("DATA_NAMESPACE"))

	client.Collection(editorsPickCollection).Doc("editorsPick_data").Set(ctx, editorsPicksData)

	return editorsPicksData, nil
}

// gets editors picks data from firestore
func GetEditorsPick() ([]byte, error) {
	ctx := context.Background()
	client := CreateClient(ctx)

	var editorsPicksData model.EditorsPick

	editorsPickCollection := fmt.Sprintf("editorsPicks%s", os.Getenv("DATA_NAMESPACE"))

	dbSnap, dataSnapErr := client.Collection(editorsPickCollection).Doc("editorsPick_data").Get(ctx)

	if dataSnapErr != nil {
		return nil, dataSnapErr
	}

	if err := dbSnap.DataTo(&editorsPicksData); err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(editorsPicksData)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func UpdateAdsConfig(ctx context.Context) error {

	labels := map[string]string{
		"handler": "UpdateAdsConfig",
	}

	log.InfoL(labels, "Starting UpdateAdsConfig")

	db := GetFirestoreClient()

	var adsConfig interface{}

	url := fmt.Sprintf("%s%s", apiURL, "channelsections/names.json?channelsections=channel_115")
	adsConfig, err := MakeForbesAPIRequest[interface{}](ctx, url, "GET")
	//res, err := http.Get(url)
	if err != nil {
		return err
	}

	adsConfigCollection := fmt.Sprintf("adsConfig%s", os.Getenv("DATA_NAMESPACE"))

	db.Collection(adsConfigCollection).Doc("adsConfig_data").Set(ctx, adsConfig)

	log.InfoL(labels, "Finished UpdateAdsConfig")

	return nil
}

func GetAdsConfig(ctx context.Context) (interface{}, error) {

	client := CreateClient(ctx)

	var adsConfig interface{}

	adsConfigCollection := fmt.Sprintf("adsConfig%s", os.Getenv("DATA_NAMESPACE"))

	dbSnap, dataSnapErr := client.Collection(adsConfigCollection).Doc("adsConfig_data").Get(ctx)

	if dataSnapErr != nil {
		return nil, dataSnapErr
	}

	if err := dbSnap.DataTo(&adsConfig); err != nil {
		return nil, err
	}

	return adsConfig, nil
}

// gets editors picks data from forbes apis and save it to firestore
func BuildVideosList(ctx context.Context, bqVideos map[string]model.BqVideosResults) (*model.NaturalIDContents, error) {

	client := GetFirestoreClient()

	var (
		vids model.NaturalIDContents
	)

	for _, vid := range bqVideos {
		forbesAPIQuery := fmt.Sprintf("video/brightcove/%s,", vid.VideoID.String())

		forbesAPIQuery = strings.TrimRight(forbesAPIQuery, ",")

		url1 := fmt.Sprintf("%s%s", apiURL, fmt.Sprintf("content/naturalids.json?naturalids=%s", url.QueryEscape(forbesAPIQuery)))
		vids, err := MakeForbesAPIRequest[model.NaturalIDContents](ctx, url1, "GET")

		if err != nil {
			return nil, err
		}
		videoCollection := fmt.Sprintf("fda_videos%s", os.Getenv("DATA_NAMESPACE"))

		for _, vid := range vids.Contents {

			var (
				authName = ""
				authSlug = ""
			)
			if len(vid.Authors) > 0 {
				authName = vid.Authors[0].Name
				authSlug = vid.Authors[0].Slug
			}
			//converts date from epoch to time.time
			date := time.Unix(0, vid.Date*int64(time.Millisecond))

			client.Collection(videoCollection).Doc(vid.Video.VideoID).Set(ctx, model.FdaVideos{VideoID: vid.Video.VideoID, Thumbnail: vid.Video.ThumbImage, Poster: vid.Video.StillImage, Title: vid.Title, Date: date, Author: authName, AuthorSlug: authSlug})
		}
	}
	return &vids, nil
}

// GetVidosList returns a list of videos from the FireStore fda_videos table
func GetVideosList(ctx0 context.Context) (*[]byte, error) {

	ctx, span := tracer.Start(ctx0, "GetVideos")
	defer span.End()

	db := GetFirestoreClient()

	collectionName := fmt.Sprintf("fda_videos%s", os.Getenv("DATA_NAMESPACE"))

	iter := db.Collection(collectionName).OrderBy("date", firestore.Desc).Documents(ctx)

	var videos []model.FdaVideos

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		var video model.FdaVideos
		err = doc.DataTo(&video)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		videos = append(videos, video)
	}

	bytes, err := json.Marshal(videos)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return &bytes, nil
}

func MakeForbesAPIRequest[T interface{}](ctx context.Context, host string, httpMethod string) (*T, error) {

	labels := make(map[string]string)
	span := trace.SpanFromContext(ctx)
	defer span.End()

	labels["function"] = "MakeForbesApiRequest"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("start MakeForbesApiRequest")
	var data T
	req, _ := http.NewRequest(httpMethod, host, nil)
	req.Header = make(http.Header)
	req.Header.Add("User-Agent", userAgentHeader)

	resp := HTTPGateway.Process(req)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}
	data, err = HTTPGateway.ConvertResponseToObj[T](body, resp.Header["Content-Type"][0])

	resp.Body.Close()

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err

	}
	span.SetStatus(codes.Ok, "MakeForbesApiRequest")
	return &data, nil
}

type ForbesPremiumArticles struct {
	PremiumArticles []PremiumArticles `json:"contentList" firestore:"id"` // ContentList is a list of Premium Articles from ForbesAPI
}
type PremiumArticles struct {
	Id                         string                     `json:"id" firestore:"id"`                         // Article Id
	Title                      string                     `json:"title" firestore:"title"`                   // Article title
	NaturalID                  string                     `json:"naturalId" firestore:"naturalid"`           // Article naturalId
	Image                      string                     `json:"image" firestore:"image"`                   // Article image
	ArticleURL                 string                     `json:"uri" firestore:"articleURL"`                // Article articleURL
	Description                string                     `json:"description" firestore:"description"`       // Article description
	PublishDate                int64                      `json:"date" firestore:"publishDate"`              // Article publishDate
	ContentPayWall             string                     `json:"contentPaywall" firestore:"contentPaywall"` // Article contentPaywall
	PremiumArticlesAuthorGroup PremiumArticlesAuthorGroup `json:"authorGroup" firestore:"authorGroup"`       // Article authorGroup Data
	BertieBadges               []BertieBadges             `json:"bertieBadges" firestore:"bertieBadges"`     // Article bertieBadges Data
}

type PremiumArticlesAuthorGroup struct {
	PrimaryAuthor PrimaryAuthor `json:"primaryAuthor" firestore:"primaryAuthor"` // contain All author data
}

type BertieBadges struct {
	Id          string `json:"id"`          // BertieBadges Id
	Slug        string `json:"slug"`        // BertieBadges slug
	DisplayName string `json:"displayName"` // BertieBadges displayName
	Status      string `json:"status"`      // BertieBadges status
	Priority    int64  `json:"priority"`    // BertieBadges priority
	StreamUrl   string `json:"streamUrl"`   // BertieBadges streamUrl
	Display     bool   `json:"display"`     // BertieBadges display
}

// get all premium articles from Forbes API using FDA channel and contentPaywall
func GetPremiumArticlesDataFromForbesAPI(ctx context.Context) (*ForbesPremiumArticles, error) {

	_, span := tracer.Start(ctx, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()

	span.AddEvent("Start Getting Premium Articles Data")

	/*
		QueryFilter that ForbesAPI Accept To got the premium Articles.
		- limit ---> the response limit.
		- queryfilters ---> it will be in this format [{"contentPaywall":["premium"]}, {"primaryChannelId":["channel_115"]}].
			- contentPaywall ---> we will use it because we need all the Premium Articles and it take value "premium".
			- primaryChannelId ---> we will use it to determine the channel Id data will return from it in our case it will be FDA channel "channel_115".
		- retrievedfields ---> it will present the fields will be included in response.
		- I f we need any new field to be returned we should add it to retrievedfields and we can see it in the response.
			- title --> article Title.
			- date --> article PublishDate.
			- description --> article Description.
			- image --> article Image.
			- author --> article author.
			- authorGroup --> authorGroup it will contains all author data that we need.
				- AuthorType --> author Type.
				- Badges --> Bertie Badges.
				- Name --> author Name.
				- SeniorContributor --> author SeniorContributor.
				- Disabled
				- BylineFormat
				- AuthorLink --> author AuthorLink.
				- Type --> author Type.
			- naturalId --> article NaturalId we need it to use it as unique Key.
			- primaryChannelId --> article PrimaryChannelId to check if the data back from FDA channel.
			- type --> article Type.
			- uri --> article URL.
			- contentPaywall --> article contentPaywall to check if this article is premium articles.
			- bertieBadges --> article bertieBadges it will contains the tags that article mapped to.
	*/
	url := fmt.Sprintf("%s%s", forbesURL, "content/all.json?limit=100&queryfilters=%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22primaryChannelId%22%3A%5B%22channel_115%22%5D%7D%5D&retrievedfields=id,title,image,uri,author,type,authorGroup,description,date,bertieBadges,naturalId,primaryChannelId,contentPaywall")

	ContentList, err := MakeForbesAPIRequest[ForbesPremiumArticles](ctx, url, "GET")
	if err != nil {
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	return ContentList, nil
}

// Build Premium Articles will include two process:
func BuildRecommendedPremiumArticles(ctx0 context.Context) ([]EducationArticle, error) {
	ctx, span := tracer.Start(ctx0, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Build Premium Articles Data")

	// Get the Premium Articles from ForbesAPI with queryFiler that we need.
	data, err := GetPremiumArticlesDataFromForbesAPI(ctx)
	if err != nil {
		log.Error("Error Getting Premium Articles Data from ForbesAPI: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Premium Articles Data from ForbesAPI: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.AddEvent("Start Map Premium Articles Data")

	// The response is not structured as we need so once the data has been obtained, it needs to be mapped into EducationArticle objects.
	// So we created MapPremiumArticlesDataFromForbesAPI to map the response to our struct so we can store it in Rowy table.
	result := MapPremiumArticlesDataFromForbesAPI(ctx, *data)

	return result, nil
}

// Map Premium Articles from ForbesAPI to Articles Struct
// The data that will return from ForbesAPI are not structured So we need to re-structured the data to our struct
// And convert field if there is any, then build our EducationArticle array to be ready to save.
func MapPremiumArticlesDataFromForbesAPI(ctx0 context.Context, articles ForbesPremiumArticles) []EducationArticle {

	_, span := tracer.Start(ctx0, "MapPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Map Premium Articles Data")
	var premiumArticles []EducationArticle

	// loop over the ForbesPremiumArticles to map the data that we need from the response
	for _, article := range articles.PremiumArticles {
		var premiumArticle EducationArticle
		premiumArticle.Id = article.Id
		premiumArticle.Title = article.Title
		premiumArticle.NaturalID = article.NaturalID
		premiumArticle.Image = article.Image
		premiumArticle.ArticleURL = article.ArticleURL
		premiumArticle.Description = article.Description
		// convert date from Unix format to timestamp format
		premiumArticle.PublishDate = time.Unix(0, article.PublishDate*int64(time.Millisecond))
		premiumArticle.AuthorType = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorType
		premiumArticle.Author = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Name
		premiumArticle.SeniorContributor = article.PremiumArticlesAuthorGroup.PrimaryAuthor.SeniorContributor
		premiumArticle.Disabled = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Disabled
		premiumArticle.BylineFormat = &article.PremiumArticlesAuthorGroup.PrimaryAuthor.BylineFormat
		premiumArticle.AuthorLink = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorLink
		premiumArticle.Type = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Type
		premiumArticle.BertieTag = BuildBertieBadges(article.BertieBadges)
		premiumArticles = append(premiumArticles, premiumArticle)
	}

	span.AddEvent("Finished Map Premium Articles Data")
	return premiumArticles
}

func BuildBertieBadges(bertie []BertieBadges) string {
	var bertieBadges []string

	for _, tag := range bertie {
		if strings.Contains(tag.Slug, "-") {
			bertieBadges = append(bertieBadges, tag.DisplayName)
		} else {
			bertieBadges = append(bertieBadges, tag.Slug)
		}
	}
	result := strings.Join(bertieBadges, ",")
	return result

}

func BuildTagRecommendedPremiumArticles(ctx0 context.Context) (map[string][]EducationArticle, error) {
	ctx, span := tracer.Start(ctx0, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Build Premium Articles Data")

	// Get the Premium Articles from ForbesAPI with queryFiler that we need.
	data, err := GetPremiumArticlesDataFromForbesAPI(ctx)
	if err != nil {
		log.Error("Error Getting Premium Articles Data from ForbesAPI: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Premium Articles Data from ForbesAPI: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.AddEvent("Start Map Premium Articles Data")

	// The response is not structured as we need so once the data has been obtained, it needs to be mapped into EducationArticle objects.
	// So we created MapPremiumArticlesDataFromForbesAPI to map the response to our struct so we can store it in Rowy table.
	result := BuildPremiumArticlesCategories(ctx, *data)

	return result, nil
}

func MapPremiumArticleDataFromForbesAPI(ctx0 context.Context, article PremiumArticles) *EducationArticle {

	_, span := tracer.Start(ctx0, "MapPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Map Premium Articles Data")
	// loop over the ForbesPremiumArticles to map the data that we need from the response
	var premiumArticle EducationArticle
	premiumArticle.Id = article.Id
	premiumArticle.Title = article.Title
	premiumArticle.NaturalID = article.NaturalID
	premiumArticle.Image = article.Image
	premiumArticle.ArticleURL = article.ArticleURL
	premiumArticle.Description = article.Description
	// convert date from Unix format to timestamp format
	premiumArticle.PublishDate = time.Unix(0, article.PublishDate*int64(time.Millisecond))
	premiumArticle.AuthorType = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorType
	premiumArticle.Author = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Name
	premiumArticle.SeniorContributor = article.PremiumArticlesAuthorGroup.PrimaryAuthor.SeniorContributor
	premiumArticle.Disabled = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Disabled
	premiumArticle.BylineFormat = &article.PremiumArticlesAuthorGroup.PrimaryAuthor.BylineFormat
	premiumArticle.AuthorLink = article.PremiumArticlesAuthorGroup.PrimaryAuthor.AuthorLink
	premiumArticle.Type = article.PremiumArticlesAuthorGroup.PrimaryAuthor.Type

	span.AddEvent("Finished Map Premium Articles Data")
	return &premiumArticle
}

func BuildBertieBadge(bertie BertieBadges) string {
	var tag string
	if strings.Contains(bertie.Slug, "-") {
		tag = bertie.DisplayName
	} else {
		tag = bertie.Slug
	}
	return tag

}

func BuildPremiumArticlesCategories(ctx0 context.Context, articles ForbesPremiumArticles) map[string][]EducationArticle {
	ctx, span := tracer.Start(ctx0, "MapPremiumArticlesDataFromForbesAPI")

	defer span.End()
	span.AddEvent("Start Map Premium Articles Data")
	premiumArticles := make(map[string][]EducationArticle)

	for _, article := range articles.PremiumArticles {
		for _, tag := range article.BertieBadges {
			res := BuildBertieBadge(tag)
			art := MapPremiumArticleDataFromForbesAPI(ctx, article)
			art.BertieTag = res
			premiumArticles[res] = append(premiumArticles[res], *art)
		}

	}
	file, _ := json.MarshalIndent(premiumArticles, " ", "")
	_ = os.WriteFile("premiumArticles.json", file, 0644)

	return premiumArticles
}

// Get Recommended Premium Articles from FS
// Before we return the premium articles we need to insure the articles sorted then we will take the top 12 articles (latest) from it and return it.
func GetRecommendedPremiumArticles(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "GetRecommendedPremiumArticles")

	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "premium_articles")

	dbSnap := fs.Collection(collectionName).OrderBy("publishDate", firestore.Desc).Documents(ctx)

	span.AddEvent("Start Get Recommended Premium Articles Data from FS")

	var premiumArticles []EducationArticle
	for {
		var premiumArticle EducationArticle

		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		// map the articles from Rowy table to premiumArticle struct
		if err := doc.DataTo(&premiumArticle); err != nil {
			log.Error("Error Getting Recommended Premium Articles from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Recommended Premium Articles Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		premiumArticles = append(premiumArticles, premiumArticle)
	}

	// return Top 12 latest Premium Articles
	result, err := json.Marshal(premiumArticles[0:12])
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil

}
