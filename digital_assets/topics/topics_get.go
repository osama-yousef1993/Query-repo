package app

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel/codes"
)

// Get Trending Topics data from FS
// GetTrendingTopics Returns All Data for Trending Topics
// Returns the output of the call
func (m *Microservices) GetTrendingTopics(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetTrendingTopics", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetTrendingTopics"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetTrendingTopics"))
	var (
		err            error
		trendingTopics []datastruct.TrendingTopics
		jsonB          []byte
	)
	trendingTopics, err = m.topicsService.GetTrendingTopics(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(trendingTopics)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetTrendingTopics", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetTrendingTopics")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// Get News Topic data from FS
// GetNewsTopic Returns All Data for News Topic using the Slug
// Returns the output of the call
func (m *Microservices) GetNewsTopic(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	vars := mux.Vars(r)
	slug := vars["slug"]
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetNewsTopic", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetNewsTopic"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetNewsTopic"))
	var (
		err       error
		newsTopic *datastruct.Topic
		jsonB     []byte
	)
	newsTopic, err = m.topicsService.GetNewsTopic(r.Context(), slug)
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(newsTopic)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetNewsTopic", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetNewsTopic")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// Get News Topics Categories data from FS
// GetNewsTopicCategories Returns All Data for News Topics Categories
// Returns the output of the call
func (m *Microservices) GetNewsTopicCategories(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetNewsTopicCategories", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetNewsTopicCategories"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetNewsTopicCategories"))
	var (
		err              error
		topicsCategories []datastruct.TopicCategories
		jsonB            []byte
	)
	topicsCategories, err = m.topicsService.GetNewsTopicCategories(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(topicsCategories)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetNewsTopicCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetNewsTopicCategories")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// Get Topics Bubbles data from FS
// GetTopicBubbles Returns All Data for Topics Bubbles
// Returns the output of the call
func (m *Microservices) GetTopicBubbles(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetTopicBubbles", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetTopicBubbles"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetTopicBubbles"))
	var (
		err           error
		topicsBubbles []datastruct.TopicsBubbles
		jsonB         []byte
	)
	topicsBubbles, err = m.topicsService.GetTopicBubbles(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(topicsBubbles)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetTopicBubbles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetTopicBubbles")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
