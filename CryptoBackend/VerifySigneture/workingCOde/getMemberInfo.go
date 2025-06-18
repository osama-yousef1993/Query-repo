package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Forbes-Media/crypto-backend-api/dto"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// GetMemberInfo Returns All Data for a wallet user
// Expects a request body that contains a wallet_addr as a parameter
// Returns information about a member
func (m *Microservices) GetMemberInfo(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Cache-Control", "max-age=-1, no-cache, no-store, must-revalidate, public")

	var (
		memberInfo *dto.MemberInfo // Member Info Data
		jsonB      []byte          // Byte array, parse data from memberInfo
		err        error
		body       dto.MemberInfo //parse request body into this object
	)
	span, labels := common.GenerateSpan("GetMemberInfo", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetMemberInfo"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetMemberInfo"))

	results := m.isMemberInfoRequestAuthorized(r)
	if !results.IsDIDValid && !results.IsAuthorized {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if results.JWT != nil {
		body = dto.MemberInfo{WalletAddress: results.WalletAddress, FDAClgnJWT: results.JWT}
	} else {
		body = dto.MemberInfo{WalletAddress: results.WalletAddress}
	}

	memberInfo, err = m.memberInfoService.GetMemberInfo(r.Context(), &body)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	jsonB, err = json.Marshal(*memberInfo)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "GetMemberInfo", startTime, nil)
	w.Header().Add("Content-Type", "application/json")
	span.SetStatus(codes.Ok, "GetMemberInfo")
	w.Write(jsonB)
	return

}

func (m *Microservices) GetMemberInfoTest(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Cache-Control", "max-age=-1, no-cache, no-store, must-revalidate, public")

	var (
		jsonB []byte // Byte array, parse data from memberInfo
		err   error
		body  dto.MemberInfo //parse request body into this object
	)
	span, labels := common.GenerateSpan("GetMemberInfo", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetMemberInfo"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetMemberInfo"))

	results := m.isMemberInfoRequestAuthorized(r)
	if !results.IsDIDValid && !results.IsAuthorized {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if results.JWT != nil {
		body = dto.MemberInfo{WalletAddress: results.WalletAddress, FDAClgnJWT: results.JWT}
	} else {
		body = dto.MemberInfo{WalletAddress: results.WalletAddress}
	}

	jsonB, err = json.Marshal(body)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "GetMemberInfo", startTime, nil)
	w.Header().Add("Content-Type", "application/json")
	span.SetStatus(codes.Ok, "GetMemberInfo")
	w.Write(jsonB)
	return

}

// UpdateMemberInfo Update User data
// Expects a request body that contains a wallet_addr, email_addr and display_name as a parameter
// Returns error if the data not updated successfully or Ok is mean data updated successfully
func (m *Microservices) UpdateMemberInfo(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Cache-Control", "max-age=-1, no-cache, no-store, must-revalidate, public")

	var memberInfo *dto.MemberInfo

	span, labels := common.GenerateSpan("UpdateMemberInfo", r.Context())

	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpdateMemberInfo"))

	body, err := io.ReadAll(r.Body)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := json.Unmarshal(body, &memberInfo); err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	results := m.isMemberInfoRequestAuthorized(r)
	if !results.IsDIDValid && !results.IsAuthorized {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if results.JWT != nil {
		memberInfo.WalletAddress = results.WalletAddress
		memberInfo.FDAClgnJWT = results.JWT
	} else {
		memberInfo.WalletAddress = results.WalletAddress
	}

	// Call UpdateMemberInfo to start our update process
	memberInfo, err = m.memberInfoService.UpdateMemberInfo(r.Context(), memberInfo)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	jsonB, err := json.Marshal(*memberInfo)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "UpdateMemberInfo", startTime, nil)
	w.Header().Add("Content-Type", "application/json")
	span.SetStatus(codes.Ok, "UpdateMemberInfo")
	w.Write(jsonB)
	return
}

// is MemberInfoRequestAuthorized looks for a bearer token If one is found we call the token manager service to see if its valid
//
// Use this in calls where endpoints where user authentication is required.
func (m *Microservices) isMemberInfoRequestAuthorized(r *http.Request) dto.MagicDIDValidationResults {

	results := dto.MagicDIDValidationResults{IsDIDValid: false, IsAuthorized: false}

	authHeader := r.Header.Get("Authorization")
	//if we have no header or the header is not a bearer token return false
	// if the auth is not greater than 7 return false to avoid out of bounds exception
	if authHeader == "" || !strings.Contains(strings.ToLower(authHeader), "bearer") || len(authHeader) < 7 {
		log.Debug(fmt.Sprintf("No Authorization header was provided %s", r.RemoteAddr))
		return results
	}
	did := authHeader[len("Bearer")+1:]
	if did == "" {
		log.Debug(fmt.Sprintf("DID token is required %s", r.RemoteAddr))
		return results
	}

	results, err := m.tokenManagerService.ValidateDID(r.Context(), did)
	if err != nil {
		log.Error(fmt.Sprintf("%s", err))
	}
	if !results.IsDIDValid {
		results, err := m.tokenManagerService.ValidateJWT(r.Context(), did)
		if err != nil {
			log.Error(fmt.Sprintf("%s", err))
			return results
		}
	}

	return results
}
