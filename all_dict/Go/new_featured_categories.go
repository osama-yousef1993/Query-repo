// categories.go
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type LandingPageResult struct {
	// Topic []Topic  `json:"Topic" firestore:"Topic"`
	RecentArticles []EducationArticle `json:"articles" firestore:"articles"`
}

func GetLandingPageFeaturedCategoriesArticles(ctx0 context.Context, categories []string) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetLandingPageCategories")
	defer span.End()
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	span.AddEvent("Start Getting Categories Data fro FS")
	var (
		db                 *firestore.DocumentIterator
		featuredCategories []Topic
		latestArticles     []EducationArticle
	)
	if categories != nil {
		db = fs.Collection(collectionName).Where("isFeaturedHome", "==", true).Where("topicOrder", "!=", 0).Where("slug", "in", categories).OrderBy("topicOrder", firestore.Asc).Documents(ctx)
	} else {
		db = fs.Collection(collectionName).Where("isFeaturedHome", "==", true).Where("topicOrder", "!=", 0).OrderBy("topicOrder", firestore.Asc).Documents(ctx)
	}
	for {
		var topic Topic
		var articles []EducationArticle

		doc, err := db.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting Categories FS: %s", err)
			span.SetStatus(codes.Error, err.Error())
			span.AddEvent(fmt.Sprintf("Error Getting Categories FS: %s", err))
			return nil, err
		}

		dbSnap := fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").Documents(ctx)

		for {
			var categoryArticles EducationArticle
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
			articles = append(articles, categoryArticles)
		}
		SortEducationArticles(articles)
		topic.Articles = articles
		featuredCategories = append(featuredCategories, topic)

	}
	if len(featuredCategories) > 0 {
		latestArticles = GetLatest12Articles(featuredCategories, categories)
	}
	SortEducationArticles(latestArticles)
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

func SortEducationArticles(articles []EducationArticle) {
	sort.Slice(articles, func(i, j int) bool {
		return articles[i].PublishDate.After(articles[j].PublishDate)
	})
}

func GetLatest12Articles(featuredCategories []Topic, categories []string) []EducationArticle {
	categoriesLen := len(featuredCategories)
	var articles []EducationArticle
	var length int
	minLen, maxLen, index := GetMinMaxValue(featuredCategories)
	if categoriesLen > 1 {
		if categories != nil {
			// Start append only one Article from each section for each loop
			for i := 0; i < minLen; i++ {
				for j := 0; j < categoriesLen; j++ {
					articles = append(articles, featuredCategories[j].Articles[i])
					if len(articles) >= 12 {
						goto END
					}
				}
			}

		} else {
			artLen := 2
			for i := 0; i < artLen; i++ {
				for j := 0; j < categoriesLen; j++ {
					SortEducationArticles(featuredCategories[j].Articles)
					if len(featuredCategories[j].Articles) > 0 {
						if len(featuredCategories[j].Articles) < artLen {
							articles = append(articles, featuredCategories[j].Articles[0])
						} else {
							articles = append(articles, featuredCategories[j].Articles[i])
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
			articles = append(articles, featuredCategories[index].Articles[i])
			if len(articles) >= 12 {
				goto END
			}
		}
	} else {
		SortEducationArticles(featuredCategories[0].Articles)
		articlesLength := len(featuredCategories[0].Articles)
		if articlesLength > 12 {
			length = 12
		} else {
			length = articlesLength
		}
		articles = append(articles, featuredCategories[0].Articles[0:length]...)
	}
END:
	return articles
}

func GetMinMaxValue(articles []Topic) (int, int, int) {
	minLength := len(articles[0].Articles)
	maxLength := len(articles[0].Articles)
	maxLengthIndex := 0
	categoriesLen := len(articles)

	for i := 1; i < categoriesLen; i++ {
		artLength := len(articles[i].Articles)
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


// firestore.go

type LandingPageCategories struct {
	Slug      string `json:"slug" firestore:"slug"`
	TopicName string `json:"topicName" firestore:"topicName"`
}

// func DeleteCategories(ctx context.Context, featuredCategories []services.FeaturedCategory) {
// 	fs := GetFirestoreClient()
// 	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "education")
// 	for _, category := range featuredCategories {
// 		fs.Collection(collectionName).Doc(category.ID).Delete(ctx)
// 	}
// }

// Get All Landing Page Featured Categories from FS
func GetLandingPageCategories(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetLandingPageCategories")
	defer span.End()

	var featuresCategories []LandingPageCategories

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	// Get Featured Categories and order it by category order
	iter := fs.Collection(collectionName).Where("isFeaturedHome", "==", true).Where("topicOrder", "!=", 0).OrderBy("topicOrder", firestore.Asc).Documents(ctx)
	span.AddEvent("Start Getting Landing Page Categories Data from FS")

	for {
		var featuresCategory LandingPageCategories

		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Landing Page Categories Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&featuresCategory)
		if err != nil {
			log.Error("Error Getting Landing Page Categories Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		featuresCategories = append(featuresCategories, featuresCategory)

	}
	jsonData, err := BuildJsonResponse(ctx, featuresCategories, "Crypto Landing Page Categories Data")

	if err != nil {
		log.Error("Error Converting Landing Page Categories to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}


// main.go

// Landing Page Featured Categories
landingPage := v1.PathPrefix("/landing-page").Subrouter()
landingPage.HandleFunc("/featured-categories-content/", GetLandingPageFeaturedCategoriesArticles).Methods(http.MethodGet, http.MethodOptions)
landingPage.HandleFunc("/featured-categories/", GetLandingPageCategories).Methods(http.MethodGet, http.MethodOptions)
func GetLandingPageFeaturedCategoriesArticles(w http.ResponseWriter, r *http.Request) {
	// update each 30 sec
	setResponseHeaders(w, 30)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetLandingPageFeaturedCategoriesArticles"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Landing Page Featured Categories")

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
		result, err = services.GetLandingPageFeaturedCategoriesArticles(r.Context(), categoriesList)
	} else {
		result, err = services.GetLandingPageFeaturedCategoriesArticles(r.Context(), nil)
	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	log.EndTimeL(labels, "Landing Page Featured Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Landing Page Featured Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}

// Will fetch Featured Categories from FS
func GetLandingPageCategories(w http.ResponseWriter, r *http.Request) {
	// update each 30 sec
	setResponseHeaders(w, 30)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetLandingPageCategories"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Crypto Featured Categories")

	// Will returns the ID and name for all Featured Categories
	result, err := store.GetLandingPageCategories(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	log.EndTimeL(labels, "Landing Page Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Landing Page Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}