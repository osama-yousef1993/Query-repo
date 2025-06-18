package app

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// Get GetLandingPageFeaturedCategoriesArticles Data from FS
// GetLandingPageFeaturedCategoriesArticles Returns All Data for Landing Page featured Categories Articles Data
// Returns the output of the call
func (m *Microservices) GetLandingPageFeaturedCategoriesArticles(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetLandingPageFeaturedCategoriesArticles", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 GetLandingPageFeaturedCategoriesArticles"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 GetLandingPageFeaturedCategoriesArticles"))

	// Get the categories from request
	categories := html.EscapeString(r.URL.Query().Get("categories"))

	// Split categories by comma to generate Categories array
	categoriesList := strings.Split(categories, ",")
	for index, ele := range categoriesList {
		categoriesList[index] = strings.TrimSpace(ele)
	}

	var (
		landingPageResponse []datastruct.Article
		err                 error
		result              []byte
	)

	// Check if we have any category in categoriesList
	if len(categoriesList) > 0 && categories != "" {
		// get All Latest Featured categories Articles depends on categoriesList
		landingPageResponse, err = m.landingPageService.GetLandingPageFeaturedCategoriesArticles(r.Context(), categoriesList)
	} else {
		// get All Latest Featured categories Articles for all Featured categories
		landingPageResponse, err = m.landingPageService.GetLandingPageFeaturedCategoriesArticles(r.Context(), nil)
	}

	if err != nil {
		goto ERR
	}

	result, err = json.Marshal(landingPageResponse)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 Landing Page Featured Categories Articles Data", startTime, nil)
	span.SetStatus(codes.Ok, "V2 Landing Page Featured Categories Articles Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	w.WriteHeader(http.StatusInternalServerError)
	span.SetStatus(codes.Error, err.Error())
	return
}

// Will fetch only Featured Categories from FS for Landing Page
// Get GetLandingPageCategories Data from FS
// GetLandingPageCategories Returns All Data for Landing Page featured Categories Data
// Returns the output of the call
func (m *Microservices) GetLandingPageCategories(w http.ResponseWriter, r *http.Request) {
	// updated each 10 min
	common.SetResponseHeaders(w, 600)
	span, labels := common.GenerateSpan("V2 GetLandingPageCategories", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetLandingPageCategories"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetLandingPageCategories"))

	var (
		landingPageCategories *[]datastruct.LandingPageFeaturedCategories
		err                   error
		result                []byte
	)
	// Will returns the ID and name for all Featured Categories
	landingPageCategories, err = m.landingPageService.GetLandingPageCategories(r.Context())

	if err != nil {
		goto ERR
	}
	result, err = json.Marshal(*landingPageCategories)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 Landing Page Featured Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "V2 Landing Page Featured Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
	return
ERR:
	log.ErrorL(labels, "%s", err)
	w.WriteHeader(http.StatusInternalServerError)
	span.SetStatus(codes.Error, err.Error())
	return
}

// Get GetLandingPageFeaturedCategoriesArticles Data from FS
// GetLandingPageFeaturedCategoriesArticles Returns All Data for Landing Page featured Categories Articles Data
// Returns the output of the call
func (m *Microservices) GetLandingPageFeaturedArticles(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetLandingPageFeaturedArticles", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 GetLandingPageFeaturedArticles"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 GetLandingPageFeaturedArticles"))

	// Get the categories from request
	categories := html.EscapeString(r.URL.Query().Get("categories"))

	// Split categories by comma to generate Categories array
	categoriesList := strings.Split(categories, ",")
	for index, ele := range categoriesList {
		categoriesList[index] = strings.TrimSpace(ele)
	}

	var (
		landingPageResponse []datastruct.Article
		err                 error
		result              []byte
	)

	// get All Latest Featured categories Articles depends on categoriesList
	landingPageResponse, err = m.landingPageService.GetLandingPageFeaturedArticles(r.Context(), categoriesList)

	if err != nil {
		goto ERR
	}

	result, err = json.Marshal(landingPageResponse)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 Landing Page Featured Articles Data", startTime, nil)
	span.SetStatus(codes.Ok, "V2 Landing Page Featured Articles Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	w.WriteHeader(http.StatusInternalServerError)
	span.SetStatus(codes.Error, err.Error())
	return
}
