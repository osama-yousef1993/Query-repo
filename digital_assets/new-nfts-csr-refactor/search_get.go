package app

import (
	"context"
	"errors"
	"fmt"
	"html"
	"net/http"
	"strconv"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/dto"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel/codes"
)

// Search: An entry point to the FDSA seach service
func (m *Microservices) Search(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 150)

	span, labels := common.GenerateSpan("V2 SearchCategoriesFundamentals", r.Context())

	span.AddEvent(fmt.Sprintf("Starting %s", "SearchCategoriesFundamentals"))
	defer span.End()

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetAnnouncementsData"))

	searchRequest, err := extractSearchParameters(r.Context(), r)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	jsonB, err := m.searchService.SearchCache(r.Context(), *searchRequest)
	if err == nil && jsonB == nil {
		log.DebugL(labels, "%s", "No Results Found matching query")
		span.SetStatus(codes.Error, "No Results Found matching query")
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.EndTimeL(labels, "V2 SearchCategoriesFundamentals", startTime, nil)
	span.SetStatus(codes.Ok, "V2 SearchCategoriesFundamentals")
	w.Write(*jsonB)
	return

}

// ExtractSearchParameters: Used to extract all parameters from a search request.
//
// Takes a context an an http request.
// Returns a dto.searchrequest object which is all of the parameters wrapped in an object, or an error,
func extractSearchParameters(ctx context.Context, r *http.Request) (*dto.SearchRequest, error) {

	vars := mux.Vars(r)

	dataset := vars["dataset"]

	paginate := dto.Paginate{} //captures the pagination params.
	limit := html.EscapeString(r.URL.Query().Get("limit"))
	query := html.EscapeString(r.URL.Query().Get("query"))
	pageNum := html.EscapeString(r.URL.Query().Get("pageNum"))
	paginate.SortBy = html.EscapeString(r.URL.Query().Get("sortBy"))
	paginate.Direction = html.EscapeString(r.URL.Query().Get("direction"))
	paginate.ChainID = html.EscapeString(r.URL.Query().Get("chain_id"))
	// Will use categoryID if we need to search about specific FT using Tags
	categoryID := html.EscapeString(r.URL.Query().Get("categoryId"))
	var limitError error
	var pageError error
	paginate.Limit, limitError = strconv.Atoi(limit)
	paginate.PageNum, pageError = strconv.Atoi(pageNum)
	isCategoryValid := isValidSearchCategory(dataset)
	if limitError != nil || pageError != nil || !isCategoryValid { //throw an error if pagination args are improper.
		log.Debug("Invalid pagination values")
		return nil, errors.New(fmt.Sprintf("limitError:%s,pageError:%s,isCategoryValid:%v", limitError.Error(), pageError.Error(), isCategoryValid))
	}
	return &dto.SearchRequest{Query: query, CategoryID: categoryID, Category: dto.DictionaryDataSet(dataset), Paginate: paginate}, nil
}

// isValidSearchCategory: Verifies that the passed in object is a valid enum
// Takes a string
// returns a boolean
func isValidSearchCategory(category string) bool {
	switch dto.DictionaryDataSet(category) {
	case dto.Ft, dto.FTCategory, dto.Nft, dto.CategoriesTable, dto.Category, dto.NFTChains:
		return true
	default:
		return false
	}
}
