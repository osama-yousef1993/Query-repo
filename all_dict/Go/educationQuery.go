package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type EducationQuery interface {
	GetEducation(ctx context.Context, categories []string) (*datastruct.Education, error)            // Gets Education Data
	BuildEducation(ctx context.Context) ([]datastruct.Section, []datastruct.EducationArticle, error) // Gets Build Education data
	SaveEducationSection(ctx context.Context, sections []datastruct.Section) error                   // Save Education data to FS
}

type educationQuery struct{}

// GetEducation Gets all content for Education
// Takes a (ctx context.Context, categories []string)
// Returns (*datastruct.Education, Error)
//
// Gets the Education data from firestore
// Returns the Education data and no error if successful
func (e *educationQuery) GetEducation(ctx context.Context, categories []string) (*datastruct.Education, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 EducationQuery.GetEducation", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EducationQuery.GetEducation"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EducationQuery.GetEducation"))

	var (
		dbSnap           *firestore.DocumentIterator
		education        datastruct.Education
		sectionEducation []datastruct.Section
	)
	// If categories exist, we need to get the section and its articles to selected categories.
	// Else it will return the top 12 articles from all sections.
	if categories != nil {
		dbSnap = fs.Collection(datastruct.EducationCollectionName).Where("sectionOrder", "!=", 0).Where("name", "in", categories).OrderBy("sectionOrder", firestore.Asc).Documents(ctx)

	} else {
		dbSnap = fs.Collection(datastruct.EducationCollectionName).Where("sectionOrder", "!=", 0).OrderBy("sectionOrder", firestore.Asc).Documents(ctx)
	}

	for {
		var section datastruct.Section
		var articles []datastruct.EducationArticle
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&section); err != nil {
			log.Error("Error V2 EducationQuery.GetEducation from FS: %s", err)
			return nil, err
		}
		subCollection := fs.Collection(datastruct.EducationCollectionName).Doc(doc.Ref.ID).Collection("articles").Where("order", "!=", 0).OrderBy("order", firestore.Asc).Documents(ctx)

		for {
			var article datastruct.EducationArticle
			do, err := subCollection.Next()

			if err == iterator.Done {
				break
			}

			if err := do.DataTo(&article); err != nil {
				log.Error("Error V2 EducationQuery.GetEducation Data from FS: %s", err)
				return nil, err
			}
			articles = append(articles, article)
		}
		section.Articles = articles
		sectionEducation = append(sectionEducation, section)
	}
	education.Section = sectionEducation
	log.EndTimeL(labels, "V2 EducationQuery.GetEducation", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EducationQuery.GetEducation")
	return &education, nil
}

// GetPremiumArticles Gets all content for PremiumArticles
// Takes a context
// Returns (*datastruct.PremiumArticles, Error)
//
// Gets the PremiumArticles data from firestore
// Returns the PremiumArticles and no error if successful
func (e *educationQuery) BuildEducation(ctx context.Context) ([]datastruct.Section, []datastruct.EducationArticle, error) {
	span, labels := common.GenerateSpan("V2 EducationQuery.BuildEducation", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EducationQuery.BuildEducation"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EducationQuery.BuildEducation"))
	fs := fsUtils.GetFirestoreClient()

	dbSnap := fs.Collection(datastruct.EducationCollectionName).Documents(ctx)
	span.AddEvent("Start Get Section Data from FS")

	var (
		sectionEducation []datastruct.Section
		bertieTag        []string
	)

	for {
		var (
			section  datastruct.Section
			articles []datastruct.EducationArticle
		)
		doc, err := dbSnap.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&section); err != nil {
			log.Error("Error Getting LearnSection Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting LearnSection Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, nil, err
		}
		db := fs.Collection(datastruct.EducationCollectionName).Doc(doc.Ref.ID).Collection("articles").Documents(ctx)

		for {
			var article datastruct.EducationArticle
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&article); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(codes.Error, err.Error())
				return nil, nil, err
			}
			article.DocId = doc.Ref.ID
			if article.UpdatedAt != nil {
				article.LastUpdated = article.UpdatedAt["timestamp"].(time.Time)
			}

			articles = append(articles, article)
		}
		bertieTag = append(bertieTag, section.BertieTag)

		section.DocId = doc.Ref.ID
		section.Articles = articles
		sectionEducation = append(sectionEducation, section)
	}

	articles, err := e.GetEducationContentFromBertie(ctx, bertieTag, "mv_content_latest")

	if err != nil {
		log.Error("Error Getting Articles from Bertie BQ: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		return nil, nil, err
	}
	log.EndTimeL(labels, "V2 EducationQuery.BuildEducation", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EducationQuery.BuildEducation")
	return sectionEducation, articles, nil
}

func (e *educationQuery) GetEducationContentFromBertie(ctx context.Context, bertieTag []string, contentDataSet string) ([]datastruct.EducationArticle, error) {
	span, labels := common.GenerateSpan("V2 EducationQuery.GetEducationContentFromBertie", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EducationQuery.GetEducationContentFromBertie"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EducationQuery.GetEducationContentFromBertie"))
	client, err := NewBQStore()
	if err != nil {
		log.Error("Error V2 EducationQuery.GetEducation Data from FS: %s", err)
		return nil, err
	}

	queryResult := client.Query(datastruct.ArticlesQuery)
	queryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "bertieTag",
			Value: bertieTag,
		},
		{
			Name:  "learnTag",
			Value: "FDA Learn",
		},
	}

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.Error("Error Getting Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Articles Data from BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var imageDomain string
	if contentDataSet == "mv_content_latest" {
		imageDomain = ""
	} else {
		imageDomain = os.Getenv("ARTICLES_IMAGE_DOMAIN")
	}
	var educationArticle []datastruct.EducationArticle
	span.AddEvent("Start Map Articles Data")

	for {
		var article datastruct.EducationArticle
		var articleFromBQ datastruct.EducationArticleFromBQ
		err := it.Next(&articleFromBQ)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Map Articles Data to Struct: %s", err)
			span.AddEvent(fmt.Sprintf("Error Map Articles Data to Struct: %s", err))
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

		educationArticle = append(educationArticle, article)
	}
	log.EndTimeL(labels, "V2 EducationQuery.GetEducationContentFromBertie", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EducationQuery.GetEducationContentFromBertie")
	return educationArticle, nil
}

func (e *educationQuery) SaveEducationSection(ctx context.Context, sections []datastruct.Section) error {

	span, labels := common.GenerateSpan("V2 EducationQuery.SaveEducationSection", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EducationQuery.SaveEducationSection"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EducationQuery.SaveEducationSection"))
	fs := fsUtils.GetFirestoreClient()

	for _, section := range sections {
		fs.Collection(datastruct.EducationCollectionName).Doc(section.DocId).Set(ctx, map[string]interface{}{
			"name":         section.Name,
			"bertieTag":    section.BertieTag,
			"description":  section.Description,
			"sectionOrder": section.SectionOrder,
			"sectionImage": section.SectionImage,
		}, firestore.MergeAll)
		for _, article := range section.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["naturalid"] = article.NaturalID
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
			if article.NaturalID != "" {
				fs.Collection(datastruct.EducationCollectionName).Doc(section.DocId).Collection("articles").Doc(strings.ReplaceAll(article.NaturalID, "/", "_")).Set(ctx, doc, firestore.MergeAll)
			}
		}
		err := e.removeArticlesWithOutNaturalID(ctx, datastruct.EducationCollectionName, section.DocId)
		if err != nil {
			log.Error("Error Getting Article Data from FS: %s", err)
			return err
		}
	}
	log.EndTimeL(labels, "V2 EducationQuery.SaveEducationSection", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EducationQuery.SaveEducationSection")
	return nil
}

// This function is to remove all articles without a natural id. This is beacuse we can not match them correctly to incoming articles. The natural id is the primary key
func (e *educationQuery) removeArticlesWithOutNaturalID(ctx context.Context, collectionName string, sectionName string) error {

	span, labels := common.GenerateSpan("V2 EducationQuery.removeArticlesWithOutNaturalID", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EducationQuery.removeArticlesWithOutNaturalID"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EducationQuery.removeArticlesWithOutNaturalID"))
	fs := fsUtils.GetFirestoreClient()

	// get topic data using slug

	//get topic articles
	db := fs.Collection(collectionName).Doc(sectionName).Collection("articles").Documents(ctx)

	for {
		var article datastruct.EducationArticle
		doc, err := db.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&article); err != nil {
			log.Error("Error Getting Article Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		//if the article does not have a natural id delete it
		if article.NaturalID == "" {
			fs.Collection(collectionName).Doc(sectionName).Collection("articles").Doc(doc.Ref.ID).Delete(ctx)
		}
	}

	log.EndTimeL(labels, "V2 EducationQuery.removeArticlesWithOutNaturalID", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EducationQuery.removeArticlesWithOutNaturalID")
	return nil

}
