package app

import (
	"fmt"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// Build Topics Data From DysonSphere
// BuildTopicsFromDS Build All articles Data for Topics from DysonSphere
// Returns the output of the call
func (m *Microservices) BuildTopicsFromDS(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildTopicsFromDS", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildTopicsFromDS"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildTopicsFromDS"))
	var (
		err error
	)
	err = m.topicsService.BuildNewsTopics(r.Context())
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildTopicsFromDS", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildTopicsFromDS")
	w.Write([]byte("Ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// Update Trending Topics Data
// UpdateTrendingTopics Trending Topics Data
// Returns the output of the call
func (m *Microservices) UpdateTrendingTopics(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 UpdateTrendingTopics", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpdateTrendingTopics"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpdateTrendingTopics"))
	var (
		err error
	)
	err = m.topicsService.UpdateTrendingTopics(r.Context())
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 UpdateTrendingTopics", startTime, nil)
	span.SetStatus(codes.Ok, "V2 UpdateTrendingTopics")
	w.Write([]byte("Ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// Build News Topics Categories Data
// BuildNewsTopicsCategories  News Topics Categories Data
// Returns the output of the call
func (m *Microservices) BuildNewsTopicsCategories(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildNewsTopicsCategories", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildNewsTopicsCategories"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildNewsTopicsCategories"))

	err := m.topicsService.BuildNewsTopicsCategories(r.Context())
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildNewsTopicsCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildNewsTopicsCategories")
	w.Write([]byte("Ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
