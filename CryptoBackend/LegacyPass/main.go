package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/Forbes-Media/crypto-backend-api/app"
	"github.com/Forbes-Media/crypto-backend-api/auth"
	"github.com/Forbes-Media/crypto-backend-api/repository"
	"github.com/Forbes-Media/crypto-backend-api/services"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
)

var allowedDomains = []string{"https://devboxmagic.forbes.com", "https://www-staging.forbes.com", "https://www.forbes.com"}
var environment = os.Getenv("ROWY_PREFIX")
var (
	db      = repository.NewDao()
	cordial = repository.NewCordial(
		os.Getenv("CORDIAL_CONTACTS_URL"),
		os.Getenv("CORDIAL_ACCESS"))
	microservices        *app.Microservices
	memberInfoService    = services.NewMemberInfoService(db, cordial)
	tokenManagerService  = services.NewTokenManagerService()
	configurationService = services.NewConfigurationService(db)
	memberReportService  = services.NewMemberReportService(db)
)

/*
Validates that all incoming requests originated from a valid domain.
Provides additional secuirity varifying that requesters have a valid HMAC token
*/
func CORSMethodMiddleware(allowedDomains []string, r *mux.Router) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Allow-Origin", "*")
			if req.Method == http.MethodOptions {
				return
			}
			next.ServeHTTP(w, req)
		})
	}
}

// Helper function to check if the given domain is allowed
func isDomainAllowed(domain string, allowedDomains []string) bool {
	for _, allowedDomain := range allowedDomains {
		if strings.EqualFold(domain, allowedDomain) {
			return true
		}
	}
	return false
}
func main() {
	var err error
	var oidcAuthMiddleware auth.OidcAuthMiddleware

	microservices, err = app.NewMicroservices(
		memberInfoService,
		tokenManagerService,
		configurationService,
		memberReportService)
	if err != nil {
		log.Critical("could not load microservices")
	}
	r := mux.NewRouter()

	r.Use(CORSMethodMiddleware(allowedDomains, r)) //all routes registered after this will have CORS headers set!
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.HandleFunc("/GetUserInfo", microservices.GetMemberInfo).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/UpdateUserInfo", microservices.UpdateMemberInfo).Methods(http.MethodPost, http.MethodOptions)

	r.HandleFunc("/GetUserInfo", microservices.GetMemberInfo).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/UpdateUserInfo", microservices.UpdateMemberInfo).Methods(http.MethodPost, http.MethodOptions)

	memberReport := v1.PathPrefix("/reports").Subrouter()
	memberReport.Handle("/BuildCommunityMembersInfoBQ", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildCommunityMembersInfo))).Methods(http.MethodPost, http.MethodOptions)
	memberReport.Handle("/BuildLegacyPassInfo", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildLegacyPassInfo))).Methods(http.MethodPost, http.MethodOptions)

	rowyTrigger := v1.PathPrefix("/rowy-trigger").Subrouter()
	rowyTrigger.HandleFunc("/updategrantconfiguration", microservices.UpdateGrantConfigurations).Methods(http.MethodPost)

	initServer(r)
}
