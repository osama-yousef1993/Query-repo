package repository

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type ResearchQuery interface {
	GetResearchArticles(ctx context.Context) ([]datastruct.Article, error)                 // Get Research Articles data from BQ
	GetResearchAnalysts(ctx context.Context) ([]datastruct.Analyst, error)                 // Get Research Analysts data from FS
	GetResearchArticle(ctx context.Context, articleID string) (*datastruct.Article, error) // Get Research Article data by ArticleId from BQ
	UpdateResearchData(ctx context.Context, research *datastruct.Research) error           // Update Research Data to FS
}

type researchQuery struct{}

func (r *researchQuery) GetResearchArticles(ctx context.Context) ([]datastruct.Article, error) {
	span, labels := common.GenerateSpan("V2 ResearchQuery.GetResearchArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchArticles"))
	bqs, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchArticles Connecting to BigQuery: %s", err)
		return nil, err
	}
	var articles []datastruct.Article

	queryResult := bqs.Query(datastruct.ResearchArticlesQuery)

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchArticles executing the Query from BigQuery: %s", err)
		return nil, err
	}

	for {
		var bqArticle datastruct.EducationArticleFromBQ
		err := it.Next(&bqArticle)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchArticles Mapping data t Article Object: %s", err)
			return nil, err
		}
		var article = datastruct.Article{
			Id:                bqArticle.Id.StringVal,
			Title:             bqArticle.Title.StringVal,
			Image:             bqArticle.Image.StringVal,
			ArticleURL:        bqArticle.ArticleURL.StringVal,
			Author:            bqArticle.Author.StringVal,
			Type:              bqArticle.Type.StringVal,
			AuthorType:        bqArticle.AuthorType.StringVal,
			AuthorLink:        bqArticle.AuthorLink.StringVal,
			Description:       bqArticle.Description.StringVal,
			PublishDate:       bqArticle.PublishDate,
			Disabled:          bqArticle.Disabled.Bool,
			SeniorContributor: bqArticle.SeniorContributor.Bool,
			BylineFormat:      &bqArticle.BylineFormat.Int64,
		}
		articles = append(articles, article)
	}

	log.EndTimeL(labels, "V2 ResearchQuery.GetResearchArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 ResearchQuery.GetResearchArticles")
	return articles, nil

}
func (r *researchQuery) GetResearchAnalysts(ctx context.Context) ([]datastruct.Analyst, error) {
	span, labels := common.GenerateSpan("V2 ResearchQuery.GetResearchAnalysts", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchAnalysts"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchAnalysts"))
	fs := fsUtils.GetFirestoreClient()

	var analysts []datastruct.Analyst

	dbSnap := fs.Collection(datastruct.ResearchAnalystsCollectionName).Documents(ctx)

	for {
		var analyst datastruct.Analyst

		doc, err := dbSnap.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&analyst); err != nil {
			log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchAnalysts Mapping data to analyst Object from FS: %s", err)
			return nil, err
		}
		analysts = append(analysts, analyst)
	}

	log.EndTimeL(labels, "V2 ResearchQuery.GetResearchAnalysts", startTime, nil)
	span.SetStatus(codes.Ok, "V2 ResearchQuery.GetResearchAnalysts")

	return analysts, nil

}
func (r *researchQuery) GetResearchFeaturedArticles(ctx context.Context) (*datastruct.Article, error) {
	span, labels := common.GenerateSpan("V2 ResearchQuery.GetResearchFeaturedArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchFeaturedArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchFeaturedArticles"))
	fs := fsUtils.GetFirestoreClient()

	dbSnap := fs.Collection(datastruct.ResearchFeaturedArticleCollectionName).Documents(ctx)
	var articleID string
	for {

		doc, err := dbSnap.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchFeaturedArticles Mapping data to analyst Object from FS: %s", err)
			return nil, err
		}
		if doc.Ref.ID == "featured" {
			data := doc.Data()
			articleID = data["articleId"].(string)
		}
	}

	if articleID == "" {
		err := "Error V2 ResearchQuery.GetResearchFeaturedArticles No featured research article found"
		log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchFeaturedArticles No featured research article found: %s", err)
		return nil, fmt.Errorf("%s", err)
	}

	article, err := r.GetResearchArticle(ctx, articleID)
	if err != nil {
		log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchFeaturedArticles there is no article with this ID %s in BigQuery: %s", articleID, err)
		return nil, err
	}

	log.EndTimeL(labels, "V2 ResearchQuery.GetResearchFeaturedArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 ResearchQuery.GetResearchFeaturedArticles")

	return article, nil

}

func (r *researchQuery) GetResearchArticle(ctx context.Context, articleID string) (*datastruct.Article, error) {
	span, labels := common.GenerateSpan("V2 ResearchQuery.GetResearchArticle", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchArticle"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 ResearchQuery.GetResearchArticle"))
	bqs, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchArticle Connecting to BigQuery: %s", err)
		return nil, err
	}

	queryResult := bqs.Query(datastruct.ResearchArticleQuery)
	queryResult.Parameters = append(queryResult.Parameters, bigquery.QueryParameter{Name: "articleId", Value: articleID})

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchArticle executing the Query from BigQuery: %s", err)
		return nil, err
	}

	var article *datastruct.Article

	for {
		var bqArticle datastruct.EducationArticleFromBQ
		err := it.Next(&bqArticle)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "Error V2 ResearchQuery.GetResearchArticle Mapping data to Article Object: %s", err)
			return nil, err
		}
		article = &datastruct.Article{
			Id:                bqArticle.Id.StringVal,
			Title:             bqArticle.Title.StringVal,
			Image:             bqArticle.Image.StringVal,
			ArticleURL:        bqArticle.ArticleURL.StringVal,
			Author:            bqArticle.Author.StringVal,
			Type:              bqArticle.Type.StringVal,
			AuthorType:        bqArticle.AuthorType.StringVal,
			AuthorLink:        bqArticle.AuthorLink.StringVal,
			Description:       bqArticle.Description.StringVal,
			PublishDate:       bqArticle.PublishDate,
			Disabled:          bqArticle.Disabled.Bool,
			SeniorContributor: bqArticle.SeniorContributor.Bool,
			BylineFormat:      &bqArticle.BylineFormat.Int64,
		}
	}

	log.EndTimeL(labels, "V2 ResearchQuery.GetResearchArticle", startTime, nil)
	span.SetStatus(codes.Ok, "V2 ResearchQuery.GetResearchArticle")
	return article, nil

}

func (r *researchQuery) UpdateResearchData(ctx context.Context, research *datastruct.Research) error {
	span, labels := common.GenerateSpan("V2 EducationQuery.UpdateResearchData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EducationQuery.UpdateResearchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EducationQuery.UpdateResearchData"))
	fs := fsUtils.GetFirestoreClient()

	_, err := fs.Collection(datastruct.ResearchCollectionName).Doc("research_data").Set(ctx, research)
	if err != nil {
		log.ErrorL(labels, "Error V2 ResearchQuery.UpdateResearchData Saving data to Research Data to FS: %s", err)
		return err
	}
	log.EndTimeL(labels, "V2 EducationQuery.UpdateResearchData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EducationQuery.UpdateResearchData")
	return nil
}
