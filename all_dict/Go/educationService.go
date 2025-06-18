package services

import (
	"context"
	"slices"
	"sort"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type EducationService interface {
	GetEducation(ctx context.Context, categories []string) (*datastruct.LandingPageEducation, error) // Returns all Section data and top latest articles from each section for Education Page
	BuildLearnEducation(ctx context.Context)  error                          // Build All data from Education section with FDA Learn tag
}

// Create object for the service that contains a repository.education interface
type educationService struct {
	dao repository.DAO
}

// NewEducationService Attempts to Get Access to all Education Page functions
// Takes a repository.DAO so we can use our Query functions
// Returns (EducationService)
//
// Takes the dao and return educationService with dao to access all our functions in Education page to get data from our FS
// Returns a EducationService interface for Education Page
func NewEducationService(dao repository.DAO) EducationService {
	return &educationService{dao: dao}
}

// GetEducation Attempts to Get Sections data and Top latest Articles
// Takes a (ctx context.Context, categories []string)
// Returns (*datastruct.LandingPageEducation, error)
//
// Takes the context and array of selected categories(Section Name) and returns Sections data and Top latest Articles
// Returns a *datastruct.LandingPageEducation with all Section  info for Education Page
func (e *educationService) GetEducation(ctx context.Context, categories []string) (*datastruct.LandingPageEducation, error) {
	education, err := e.dao.NewEducationQuery().GetEducation(ctx, categories)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	/*
		- We will return the new object LandingPageEducation with both Sections data and the top Articles from Selected Section.
		- If selected categories exist, it will return the selected section and its top 12 latest articles.
		- If not, it will return all sections and the top 12 articles from all sections.
	*/
	articles := e.GetTop12ArticlesFromLearnSection(education.Section, categories)
	resp := datastruct.LandingPageEducation{Education: *education, LatestArticles: articles}
	return &resp, nil
}

// BuildLearnEducation Attempts to Build Education data
// Takes a context
// Returns (*datastruct.FAQ, error)
//
// Takes the context and get the Frequently Asked Questions data
// Returns a *datastruct.FAQ with all of the FAQ info for Community Page
func (e *educationService) BuildLearnEducation(ctx context.Context) error {
	educationSections, articles, err := e.dao.NewEducationQuery().BuildEducation(ctx)
	if err != nil {
		log.Error("%s", err)
		return err
	}
	sections, err := e.MapArticlesToSection(educationSections, articles)
	if err != nil {
		log.Error("%s", err)
		return err
	}
	saveErr := e.dao.NewEducationQuery().SaveEducationSection(ctx, sections)
	if saveErr != nil {
		log.Error("%s", saveErr)
		return saveErr
	}
	return nil
}

/*
- GetTop12ArticlesFromLearnSection Attempts to get 12 top latest articles from each section
- Takes (sections []datastruct.Section, categories []string)
- returns []datastruct.EducationArticle
- This will ensure that if the selected section contains more than 12 articles, it will return only the top 12 articles from the selected section else it will return all articles.
*/
func (e *educationService) GetTop12ArticlesFromLearnSection(sections []datastruct.Section, categories []string) []datastruct.EducationArticle {
	var latestArticles []datastruct.EducationArticle
	var length int
	sectionsLen := len(sections)
	var uniqueArticles []string

	// if there are more than one section selected
	if sectionsLen > 1 {
		if categories != nil {
			minLength := len(sections[0].Articles)
			maxLength := len(sections[0].Articles)
			maxLengthIndex := 0
			// Getting the minimum length to start with it
			// Getting the maximum length and section index to finished with it
			for index, section := range sections[1:sectionsLen] {
				e.SortArticles(sections[index].Articles, false)
				artLength := len(section.Articles)
				if artLength < minLength {
					minLength = artLength
				} else if artLength > maxLength {
					maxLengthIndex = index
					maxLength = artLength
				}
				e.SortArticles(sections[index].Articles, true)
			}
			// Start append only one Article from each section for each loop
			for i := 0; i < minLength; i++ {
				for j := 0; j < sectionsLen; j++ {
					if e.CheckUniqueArticles(uniqueArticles, sections[j].Articles[i].ArticleURL) {
						uniqueArticles = append(uniqueArticles, sections[j].Articles[i].ArticleURL)
						latestArticles = append(latestArticles, sections[j].Articles[i])
						if len(latestArticles) >= 12 {
							goto END
						}
					}
				}
			}
			// if the latest articles not equals 12 append articles to be 12 latest articles
			for i := minLength; i < maxLength; i++ {
				latestArticles = append(latestArticles, sections[maxLengthIndex].Articles[i])
				if len(latestArticles) >= 12 {
					goto END
				}
			}
		} else {
			// Getting two Articles from each section for default response
			for i := 0; i < 2; i++ {
				for j := 0; j < sectionsLen; j++ {
					e.SortArticles(sections[j].Articles, false)
					if e.CheckUniqueArticles(uniqueArticles, sections[j].Articles[i].ArticleURL) {
						uniqueArticles = append(uniqueArticles, sections[j].Articles[i].ArticleURL)
						latestArticles = append(latestArticles, sections[j].Articles[i])
						if len(latestArticles) >= 12 {
							goto END
						}
					}
					e.SortArticles(sections[j].Articles, true)
				}
			}
		}

	} else {
		// only one section selected
		e.SortArticles(sections[0].Articles, false)
		articlesLength := len(sections[0].Articles)
		if articlesLength > 12 {
			length = 12
		} else {
			length = articlesLength
		}
		latestArticles = append(latestArticles, sections[0].Articles[0:length]...)
	}
END:
	e.SortArticles(latestArticles, false)
	return latestArticles

}

// CheckUniqueArticles Attempts to check if the top latest articles
func (e *educationService) CheckUniqueArticles(uniqueArticles []string, articleUrl string) bool {
	return !slices.Contains(uniqueArticles, articleUrl)
}

/*
- Sort articles by order if it exists. If it does not exist, they will be sorted by date.
- Add a flag to use the sort for both the Learn tab and Learn section.
- If the flag is true, it will sort the articles for the Learn tab.
- If the flag is false, it will sort the articles for the Learn section.
*/
func (e *educationService) SortArticles(articles []datastruct.EducationArticle, flag bool) {
	sort.Slice(articles, func(i, j int) bool {
		if flag {

			if articles[i].Order > 0 || articles[j].Order > 0 {
				if articles[i].Order == articles[j].Order {
					return articles[i].LastUpdated.After(articles[j].LastUpdated)
				}
				return articles[i].Order < articles[j].Order
			} else {
				return articles[i].PublishDate.After(articles[j].PublishDate)
			}
		} else {
			return articles[i].PublishDate.After(articles[j].PublishDate)
		}
	})
}

// map Articles to each LearnSection by Bertie Tag
func (e *educationService) MapArticlesToSection(sections []datastruct.Section, articles []datastruct.EducationArticle) ([]datastruct.Section, error) {
	var educationSection []datastruct.Section
	for _, section := range sections {
		var educationArticles []datastruct.EducationArticle
		for _, article := range articles {
			if section.BertieTag == article.BertieTag {
				for _, sectionArticle := range section.Articles {
					// if article exist in section map the new value article to it
					if sectionArticle.Title == article.Title {
						article.DocId = sectionArticle.DocId
						article.Order = sectionArticle.Order
						article.LastUpdated = sectionArticle.LastUpdated
						article.IsFeaturedArticle = sectionArticle.IsFeaturedArticle
						goto ADDArticles
					}
				}
			ADDArticles:
				educationArticles = append(educationArticles, article)
			}
		}
		e.SortArticles(educationArticles, true)
		section.Articles = educationArticles
		educationSection = append(educationSection, section)
	}
	return educationSection, nil
}
