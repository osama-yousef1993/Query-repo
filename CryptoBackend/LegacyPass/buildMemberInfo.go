package app

import (
	"fmt"
	"net/http"

	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// BuildCommunityMembersInfo Build Community Members Data from PG to BQ
//
// Returns Ok if the build process successfully if not it will return 500
func (m *Microservices) BuildCommunityMembersInfo(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("BuildCommunityMembersInfo", r.Context())
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildCommunityMembersInfo"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildCommunityMembersInfo"))

	memberInfo, err := m.memberReportService.GetCommunityMembersInfo(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = m.memberReportService.BQInsertCommunityMembersInfo(r.Context(), memberInfo)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.EndTimeL(labels, "BuildCommunityMembersInfo", startTime, nil)
	span.SetStatus(codes.Ok, "BuildCommunityMembersInfo")
	w.Write([]byte("ok"))
	return
}

// BuildLegacyPassInfo Build LegacyPass Data from MySql into BQ
//
// Returns Ok if the build process successfully if not it will return 500
func (m *Microservices) BuildLegacyPassInfo(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("BuildLegacyPassInfo", r.Context())
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildLegacyPassInfo"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildLegacyPassInfo"))

	memberInfo, err := m.memberReportService.GetLegacyPassInfo(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = m.memberReportService.BQInsertLegacyPassInfo(r.Context(), memberInfo)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.EndTimeL(labels, "BuildLegacyPassInfo", startTime, nil)
	span.SetStatus(codes.Ok, "BuildLegacyPassInfo")
	w.Write([]byte("ok"))
	return
}
