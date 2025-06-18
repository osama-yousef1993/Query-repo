// main
r.Handle("/build-featured-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildFeaturedCategories))).Methods(http.MethodPost)
v1.HandleFunc("/landing-page-categories", GetLandingPageCategories).Methods(http.MethodGet, http.MethodOptions)

func BuildFeaturedCategories(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["function"] = "BuildEducation"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Education Data")

	result, err := services.BuildLandingPageCategoriesArticles(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	store.SaveFeaturedCategories(r.Context(), result)

	log.EndTimeL(labels, "Build Education Data ", startTime, nil)
	span.SetStatus(codes.Ok, "Build Education Data")
	w.WriteHeader(200)
	w.Write([]byte("ok"))
}

func GetLandingPageCategories(w http.ResponseWriter, r *http.Request) {
	// update each 30 sec
	setResponseHeaders(w, 30)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetFeaturedCategories"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Featured Categories")

	categories := html.EscapeString(r.URL.Query().Get("categories"))

	categoriesList := strings.Split(categories, ",")
	for index, ele := range categoriesList {
		categoriesList[index] = strings.TrimSpace(ele)
	}

	var (
		result []byte
		err    error
	)
	if len(categoriesList) > 0 && categories != "" {
		result, err = services.GetLandingPageCategories(r.Context(), categoriesList)
	} else {
		result, err = services.GetLandingPageCategories(r.Context(), nil)
	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	log.EndTimeL(labels, "Featured Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Featured Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}


// firestore
func SaveFeaturedCategories(ctx context.Context, featuredCategories []services.FeaturedCategory) {
	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "education")
	for _, category := range featuredCategories {
		fs.Collection(collectionName).Doc(category.ID).Set(ctx, map[string]interface{}{
			"categoryId":    category.ID,
			"categoryName":  category.Name,
			"isFeatured":    category.IsFeatured,
			"categoryOrder": category.Order,
			"link":   category.Link,
		}, firestore.MergeAll)
		for _, article := range category.Articles {
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
			doc["lastUpdated"] = article.LastUpdated
			if article.DocId != "" {
				fs.Collection(collectionName).Doc(category.ID).Collection("articles").Doc(article.DocId).Set(ctx, doc, firestore.MergeAll)
			} else {
				fs.Collection(collectionName).Doc(category.ID).Collection("articles").NewDoc().Set(ctx, doc, firestore.MergeAll)
			}
		}
	}
}

// categories.go
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type CategoriesArticle struct {
	DocId             string                 `json:"doc_id,omitempty" firestore:"doc_id"`
	Id                string                 `json:"id" firestore:"id"`
	Title             string                 `json:"title" firestore:"title"`
	Image             string                 `json:"image" firestore:"image"`
	ArticleURL        string                 `json:"articleURL" firestore:"articleURL"`
	Author            string                 `json:"author" firestore:"author"`
	Type              string                 `json:"type" firestore:"type"`
	AuthorType        string                 `json:"authorType" firestore:"authorType"`
	AuthorLink        string                 `json:"authorLink" firestore:"authorLink"`
	Description       string                 `json:"description" firestore:"description"`
	PublishDate       time.Time              `json:"publishDate" firestore:"publishDate"`
	Disabled          bool                   `json:"disabled" firestore:"disabled"`
	SeniorContributor bool                   `json:"seniorContributor" firestore:"seniorContributor"`
	BylineFormat      *int64                 `json:"bylineFormat" firestore:"bylineFormat"`
	BertieTag         string                 `json:"bertieTag" firestore:"bertieTag"`
	Order             int64                  `json:"order" firestore:"order"`
	UpdatedAt         map[string]interface{} `json:"-" firestore:"_updatedBy,omitempty"`
	LastUpdated       time.Time              `json:"lastUpdated" firestore:"lastUpdated"`
}

type CategoriesArticleBQ struct {
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
}

type FeaturedCategory struct {
	ID         string              `json:"category_id" firestore:"categoryId"`       // The Feature category id will use it in Search Traded Assets Tags
	Name       string              `json:"category_name" firestore:"categoryName"`   // The Feature category Name will use it to be displayed on Category Carousel
	Order      int                 `json:"category_order" firestore:"categoryOrder"` // The Feature category Order will use it to be determine the category order
	IsFeatured bool                `json:"isFeatured" firestore:"isFeatured"`        // The Feature category IsFeatured will use it to choose which Marked as Featured Category
	Link       string              `json:"link" firestore:"categoryLink"`            // The Feature category Link will use to lead to the Category page that is part of the News Page feature.
	Articles   []CategoriesArticle `json:"articles" firestore:"articles"`            // The Category Article will contains all articles related to specific Category
}

type LandingPageResult struct {
	// FeaturedCategory []FeaturedCategory  `json:"FeaturedCategory" firestore:"FeaturedCategory"`
	RecentArticles []CategoriesArticle `json:"articles" firestore:"articles"`
}

const categoriesArticlesQuery = `
SELECT
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
  AND c.date <= CURRENT_TIMESTAMP()
  AND c.timestamp <= CURRENT_TIMESTAMP()
  AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
  AND "all" NOT IN UNNEST(spikeFrom)
  AND ( 
		c.primaryChannelId = "channel_115"
    	OR 
		channelSection = "channel_115"
	  )
	AND bertieTag in UNNEST(@bertieTag)
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
  `

// get all categories articles using Categories Id's from BQ
func GetLandingPageCategoriesArticles(ctx0 context.Context, categoriesId []string, contentDataSet string) ([]CategoriesArticle, error) {

	client := GetBQClient()

	ctx, span := tracer.Start(ctx0, "GetLandingPageCategoriesArticles")
	defer span.End()

	queryResult := client.Query(categoriesArticlesQuery)
	queryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "categoriesId",
			Value: categoriesId,
		},
	}

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.Error("Error Getting Categories Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Categories Articles Data from BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var imageDomain string
	if contentDataSet == "mv_content_latest" {
		imageDomain = ""
	} else {
		imageDomain = os.Getenv("ARTICLES_IMAGE_DOMAIN")
	}
	var categoriesArticles []CategoriesArticle
	span.AddEvent("Start Map Articles Data")

	for {
		var categoryArticle CategoriesArticle
		var categoryArticleBQ CategoriesArticleBQ
		err := it.Next(&categoryArticleBQ)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Categories Articles Data from BQ: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Categories Articles Data from BQ: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		if categoryArticleBQ.Id.Valid {
			categoryArticle.Id = categoryArticleBQ.Id.StringVal
		}
		if categoryArticleBQ.Title.Valid {
			categoryArticle.Title = categoryArticleBQ.Title.StringVal
		}
		if categoryArticleBQ.Image.Valid {
			categoryArticle.Image = imageDomain + categoryArticleBQ.Image.StringVal
		}
		if categoryArticleBQ.Author.Valid {
			categoryArticle.Author = categoryArticleBQ.Author.StringVal
		}
		if categoryArticleBQ.AuthorLink.Valid {
			categoryArticle.AuthorLink = categoryArticleBQ.AuthorLink.StringVal
		}
		if categoryArticleBQ.AuthorType.Valid {
			categoryArticle.AuthorType = categoryArticleBQ.AuthorType.StringVal
		}
		if categoryArticleBQ.Description.Valid {
			categoryArticle.Description = categoryArticleBQ.Description.StringVal
		}
		if categoryArticleBQ.ArticleURL.Valid {
			categoryArticle.ArticleURL = categoryArticleBQ.ArticleURL.StringVal
		}
		if categoryArticleBQ.Type.Valid {
			categoryArticle.Type = categoryArticleBQ.Type.StringVal
		}
		if categoryArticleBQ.Disabled.Valid {
			categoryArticle.Disabled = categoryArticleBQ.Disabled.Bool
		}
		if categoryArticleBQ.SeniorContributor.Valid {
			categoryArticle.SeniorContributor = categoryArticleBQ.SeniorContributor.Bool
		}
		if categoryArticleBQ.BertieTag.Valid {
			categoryArticle.BertieTag = categoryArticleBQ.BertieTag.StringVal
		}
		if categoryArticleBQ.BylineFormat.Valid {
			categoryArticle.BylineFormat = &categoryArticleBQ.BylineFormat.Int64
		} else {
			categoryArticle.BylineFormat = nil
		}
		categoryArticle.PublishDate = categoryArticleBQ.PublishDate

		categoriesArticles = append(categoriesArticles, categoryArticle)

	}
	return categoriesArticles, nil
}

// Build Featured Categories Articles from BQ and store it IN Rowy Table
func BuildLandingPageCategoriesArticles(ctx0 context.Context) ([]FeaturedCategory, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "BuildLandingPageCategoriesArticles")
	defer span.End()
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")

	dbSnap := fs.Collection(collectionName).Documents(ctx)

	span.AddEvent("Start Get FeaturedCategory Data from FS")

	var (
		categoriesIds      []string
		featuredCategories []FeaturedCategory
	)

	for {
		var (
			featuredCategory   FeaturedCategory
			categoriesArticles []CategoriesArticle
		)

		doc, err := dbSnap.Next()
		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&featuredCategory); err != nil {
			log.Error("Error Getting Featured Category Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Featured Category Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		db := fs.Collection(collectionName).Doc(featuredCategory.ID).Collection("articles").Documents(ctx)

		for {
			var categoryArticles CategoriesArticle

			doc, err := db.Next()

			if err == iterator.Done {
				break
			}
			if err := doc.DataTo(&categoryArticles); err != nil {
				log.Error("Error Getting Category Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Category Article Data from FS: %s", err))
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			categoryArticles.DocId = doc.Ref.ID
			if categoryArticles.UpdatedAt != nil {
				categoryArticles.LastUpdated = categoryArticles.UpdatedAt["timestamp"].(time.Time)
			}

			categoriesArticles = append(categoriesArticles, categoryArticles)

		}

		categoriesIds = append(categoriesIds, featuredCategory.ID)

		featuredCategory.Articles = categoriesArticles
		featuredCategories = append(featuredCategories, featuredCategory)
	}

	categoriesArticles, err := GetLandingPageCategoriesArticles(ctx, categoriesIds, "mv_content_latest")

	if err != nil {
		log.Error("Error Getting Categories Articles from Bertie BQ: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Categories Articles from Bertie BQ: %s", err))
		return nil, err
	}
	categories, err := MapArticlesToLandingPageCategories(ctx, featuredCategories, categoriesArticles)

	if err != nil {
		log.Error("Error Map Articles to Featured Categories: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Map Articles to Featured Categories: %s", err))
		return nil, err
	}

	return categories, nil
}

// Map articles to each category for landing page
func MapArticlesToLandingPageCategories(ctx0 context.Context, categories []FeaturedCategory, articles []CategoriesArticle) ([]FeaturedCategory, error) {
	var featuredCategories []FeaturedCategory

	for _, category := range categories {
		var categoriesArticles []CategoriesArticle
		for _, article := range articles {
			if category.ID == article.BertieTag {
				for _, categoryArticle := range category.Articles {
					// if article exist in section map the new value article to it
					if categoryArticle.Title == article.Title {
						article.DocId = categoryArticle.DocId
						article.Order = categoryArticle.Order
						article.LastUpdated = categoryArticle.LastUpdated
						goto ADDArticles
					}
				}
			ADDArticles:
				categoriesArticles = append(categoriesArticles, article)
			}
		}
		category.Articles = categoriesArticles
		featuredCategories = append(featuredCategories, category)
	}
	return featuredCategories, nil

}

func GetLandingPageCategories(ctx0 context.Context, categories []string) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetLandingPageCategories")
	defer span.End()
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")

	span.AddEvent("Start Getting Categories Data fro FS")
	var (
		db                 *firestore.DocumentIterator
		featuredCategories []FeaturedCategory
	)
	if categories != nil {
		db = fs.Collection(collectionName).Where("isFeatured", "==", true).Where("categoryOrder", "!=", 0).Where("categoryId", "in", categories).OrderBy("categoryOrder", firestore.Asc).Documents(ctx)
	} else {
		db = fs.Collection(collectionName).Where("isFeatured", "==", true).Where("categoryOrder", "!=", 0).OrderBy("categoryOrder", firestore.Asc).Documents(ctx)
	}
	for {
		var featuredCategory FeaturedCategory
		var categoriesArticles []CategoriesArticle

		doc, err := db.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&featuredCategory); err != nil {
			log.Error("Error Getting Categories FS: %s", err)
			span.SetStatus(codes.Error, err.Error())
			span.AddEvent(fmt.Sprintf("Error Getting Categories FS: %s", err))
			return nil, err
		}

		dbSnap := fs.Collection(collectionName).Doc(featuredCategory.ID).Collection("articles").Documents(ctx)

		for {
			var categoryArticles CategoriesArticle
			doc, err := dbSnap.Next()
			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&categoryArticles); err != nil {
				log.Error("Error Getting Categories Articles FS: %s", err)
				span.SetStatus(codes.Error, err.Error())
				span.AddEvent(fmt.Sprintf("Error Getting Categories Articles FS: %s", err))
				return nil, err
			}
			categoriesArticles = append(categoriesArticles, categoryArticles)
		}
		featuredCategory.Articles = categoriesArticles
		featuredCategories = append(featuredCategories, featuredCategory)

	}

	latestArticles := GetLatest12Articles(featuredCategories, categories)
	resp := LandingPageResult{RecentArticles: latestArticles}
	result, err := json.Marshal(resp)

	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil
}

func SortCAtegoriesArticles(categoriesArticles []CategoriesArticle) {
	sort.Slice(categoriesArticles, func(i, j int) bool {
		return categoriesArticles[i].PublishDate.After(categoriesArticles[j].PublishDate)
	})
}

func GetLatest12Articles(categoriesArticles []FeaturedCategory, categories []string) []CategoriesArticle {
	categoriesLen := len(categoriesArticles)
	var articles []CategoriesArticle
	var length int
	minLen, maxLen, index := GetMinMaxValue(categoriesArticles)
	if categoriesLen > 1 {
		if categories != nil {
			// Start append only one Article from each section for each loop
			for i := 0; i < minLen; i++ {
				for j := 0; j < categoriesLen; j++ {
					articles = append(articles, categoriesArticles[j].Articles[i])
					if len(articles) >= 12 {
						goto END
					}
				}
			}

		} else {
			artLen := 2
			for i := 0; i < artLen; i++ {
				for j := 0; j < categoriesLen; j++ {
					SortCAtegoriesArticles(categoriesArticles[j].Articles)
					if len(categoriesArticles[j].Articles) > 0 {
						if len(categoriesArticles[j].Articles) < artLen {
							articles = append(articles, categoriesArticles[j].Articles[0])
						} else {
							articles = append(articles, categoriesArticles[j].Articles[i])
						}
					}
					if len(articles) >= 12 {
						goto END
					}
				}
			}
		}
		// if the latest articles not equals 12 append articles to be 12 latest articles
		for i := minLen; i < maxLen; i++ {
			articles = append(articles, categoriesArticles[index].Articles[i])
			if len(articles) >= 12 {
				goto END
			}
		}
	} else {
		SortCAtegoriesArticles(categoriesArticles[0].Articles)
		articlesLength := len(categoriesArticles[0].Articles)
		if articlesLength > 12 {
			length = 12
		} else {
			length = articlesLength
		}
		articles = append(articles, categoriesArticles[0].Articles[0:length]...)
	}
END:
	return articles
}

func GetMinMaxValue(categoriesArticles []FeaturedCategory) (int, int, int) {
	minLength := len(categoriesArticles[0].Articles)
	maxLength := len(categoriesArticles[0].Articles)
	maxLengthIndex := 0
	categoriesLen := len(categoriesArticles)

	for i := 1; i < categoriesLen; i++ {
		artLength := len(categoriesArticles[i].Articles)
		if artLength > 0 {
			if artLength < minLength {
				minLength = artLength
			} else if artLength > maxLength {
				maxLength = artLength
				maxLengthIndex = i
			}
		}
	}
	return minLength, maxLength, maxLengthIndex
}
