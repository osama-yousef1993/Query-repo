package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"time"

	"net/http"
	"os"

	"github.com/Forbes-Media/forbes-digital-assets/auth" // Note: Keep this Module import at the top. It loads os.GetEnv env variables.
	"github.com/Forbes-Media/forbes-digital-assets/model"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/app"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	rfServices "github.com/Forbes-Media/forbes-digital-assets/refactored/services"
	"github.com/Forbes-Media/forbes-digital-assets/services"
	"github.com/Forbes-Media/forbes-digital-assets/services/content"
	"github.com/Forbes-Media/forbes-digital-assets/store"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/goji/httpauth"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// Create the Datbase Access Object. This contains all of our query logic NOTE (10/20/2023 not all queries have been migrated)
var (
	db                        = repository.NewDao()
	microservices             *app.Microservices
	watchlistService          = rfServices.NewWatchlistService(db)
	communityPageService      = rfServices.NewCommunityPageService(db)
	portfolioService          = rfServices.NewPortfolioService(db)
	landingPageService        = rfServices.NewLandingPageService(db)
	cryptoPriceService        = rfServices.NewCryptoPriceService(db)
	exchangeService           = rfServices.NewExchangeService(db)
	educationService          = rfServices.NewEducationService(db)
	videoService              = rfServices.NewVideoService(db)
	searchService             = rfServices.NewSearchService(db)
	nftService                = rfServices.NewNftService(db)
	chartService              = rfServices.NewChartServices(db)
	newsFeedService           = rfServices.NewNewsFeedService(db)
	editorsPickService        = rfServices.NewEditorsPickService(db)
	topicsService             = rfServices.NewTopicsService(db)
	carouselService           = rfServices.NewCarouselService(db)
	dynamicDescriptionService = rfServices.NewDynamicDescriptionService(db)
	profilesService           = rfServices.NewProfilesService(db)
	eventsService             = rfServices.NewEventsService(db)
	researchService           = rfServices.NewResearchService(db)
	customCategoryService     = rfServices.NewCustomCategoryService(db)
	// Map that contains expiration times for "expires" headers. To Sync endpoints to use the same
	// expiration time use this map.
	// Ex: serivies using the /nfts path uses the key app.NFT_path to get the shared epiration time.
	expirationHeaders = common.NewConcurrentMap[app.EndpointPath, time.Time]()
)
var (
	completedNewCasing = false
	areIdsMigrated     bool
	backFillDateAdded  bool
)
var (
	mu     sync.Mutex
	config model.FDAConfig
)

func init() {
	// on init set cache expiration to 5 minutes, After all updates to the cache time will be
	// in build fundamentals process.
	expirationHeaders.AddValue(app.NFT_path, time.Now().Add(5*time.Minute))
	var err error

	microservices, err = app.NewMicroservices(
		expirationHeaders,
		watchlistService,
		communityPageService,
		portfolioService,
		landingPageService,
		cryptoPriceService,
		exchangeService,
		educationService,
		videoService,
		searchService,
		nftService,
		chartService,
		newsFeedService,
		editorsPickService,
		topicsService,
		carouselService,
		dynamicDescriptionService,
		researchService,
		eventsService,
		profilesService,
		customCategoryService)
	if err != nil {
		log.Critical("could not load micro services")
	}

	config, err = services.LoadFDAConfig()
	if err != nil {
		log.Error("%s", err)
	}

	areIdsMigrated, err = strconv.ParseBool(os.Getenv("AREIDSMIGRATED"))
	if err != nil {
		log.Alert("failed to parse AREIDSMIGRATED: %v", err)
		log.Info("setting AREIDSMIGRATED to true")
		areIdsMigrated = true
	}

	backFillDateAdded, err = strconv.ParseBool(os.Getenv("BACKFILL_FT_DATEADDED"))
	if err != nil {
		log.Alert("failed to parse Backfill_FT_DateAdded: %v", err)
		log.Info("setting BACKFILL_FT_DATEADDED to true")
		backFillDateAdded = true
	}

}

func CORSMethodMiddleware(r *mux.Router) mux.MiddlewareFunc {
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

// tracer is only used when adding new spans to the trace in this main package
// Used only for not HTTP starts
var tracer = otel.Tracer("github.com/Forbes-Media/forbes-digital-assets/main")

func main() {

	var oidcAuthMiddleware auth.OidcAuthMiddleware
	r := mux.NewRouter()

	if otelEnabled {

		tp, err := initTracer(context.Background())
		if err != nil {
			log.Alert("%s", err)
		}

		defer func() {
			tp.ForceFlush(context.Background())

			if err := tp.Shutdown(context.Background()); err != nil {
				log.Critical("Error shutting down tracer provider: %v", err)
			}
		}()

		go initMetrics()

		r.Use(otelmux.Middleware("github.com/Forbes-Media/forbes-digital-assets/main"))
	}

	//scheduler triggers, protected with OIDC validating middleware
	r.Handle("/cache-allProfiles", oidcAuthMiddleware.Middleware(http.HandlerFunc(CacheBertieProfiles))).Methods(http.MethodPost)
	r.Handle("/build-chart/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildChartHandler))).Methods(http.MethodPost)
	r.Handle("/build-fundamentals-cg/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(AcceptBuildFundamentalsRequest))).Methods(http.MethodPost)
	r.Handle("/build-forbes-token-metadata", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildForbesTokenMetadata))).Methods(http.MethodPost)
	r.Handle("/build-forbes-chatbot-data", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildAssetsChatbotData))).Methods(http.MethodPost)
	r.Handle("/build-NFTfundamentals-cg/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildNFTFundamentalsCGHandler))).Methods(http.MethodPost)
	r.Handle("/build-exchange-fundamentals/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildExchangeFundamentalsHandler))).Methods(http.MethodPost)
	r.Handle("/build-content", oidcAuthMiddleware.Middleware(http.HandlerFunc(GetDataFromSpreadsheet))).Methods(http.MethodPost)
	r.Handle("/build-newsfeed", oidcAuthMiddleware.Middleware(http.HandlerFunc(UpdateNewsFeedHandler))).Methods(http.MethodPost)
	r.Handle("/rebalance-index", oidcAuthMiddleware.Middleware(http.HandlerFunc(RebalanceIndex))).Methods(http.MethodPost)
	r.Handle("/update-index", oidcAuthMiddleware.Middleware(http.HandlerFunc(UpdateIndex))).Methods(http.MethodPost)
	r.Handle("/build-index-chart/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildIndexChart))).Methods(http.MethodPost)
	r.Handle("/build-events-content", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildEventsData))).Methods(http.MethodPost)
	r.Handle("/build-research-content", oidcAuthMiddleware.Middleware(http.HandlerFunc(GetResearchData))).Methods(http.MethodPost)
	r.Handle("/build-editorsPick-content", oidcAuthMiddleware.Middleware(http.HandlerFunc(GetEditorsPickData))).Methods(http.MethodPost)
	r.Handle("/update-ads-config", oidcAuthMiddleware.Middleware(http.HandlerFunc(UpdateAdsConfig))).Methods(http.MethodPost)
	r.Handle("/update-portfolio-prices", oidcAuthMiddleware.Middleware(http.HandlerFunc(UpdatePortfolioPrices))).Methods(http.MethodPost)
	r.Handle("/update-portfolio-config", oidcAuthMiddleware.Middleware(http.HandlerFunc(UpdatePortfolioConfig))).Methods(http.MethodPost)
	r.Handle("/update-config", oidcAuthMiddleware.Middleware(http.HandlerFunc(UpdateConfig))).Methods(http.MethodPost)
	r.Handle("/build-education", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildEducation))).Methods(http.MethodPost)
	r.Handle("/build-learn-education", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildEducation))).Methods(http.MethodPost)
	r.Handle("/build-videos", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildVideos))).Methods(http.MethodPost)
	r.Handle("/build-topics", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildTopics))).Methods(http.MethodPost)
	r.Handle("/build-topics-ds", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildTopicsFromDS))).Methods(http.MethodPost)
	r.Handle("/update-trending-topics", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildTrendingTopics))).Methods(http.MethodPost)
	r.Handle("/build-topics-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildTopicsCategories))).Methods(http.MethodPost)
	r.Handle("/build-premium-articles", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildPremiumArticles))).Methods(http.MethodPost)
	r.Handle("/build-historical-categories-Data", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildCategoriesHistoricalData))).Methods(http.MethodPost)

	r.Use(CORSMethodMiddleware(r)) //all routes registered after this will have CORS headers set!

	//GET data endpoints
	r.HandleFunc("/healthcheck", healthcheck).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/fundamentals/{period}/{symbol}", GetFundamentalsHandler).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/related-cryptos/{period}/{exchange}", GetRelatedCryptosForExchange).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/content/crypto/{slug}", GetAssetsProfilesHandler).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/content/exchange/{slug}", GetExchangeProfilesHandler).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/leaders-laggards", GetLeadersLaggards).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/dashboard-content", GetDashboardContent).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/carousel", microservices.GetCarouselData).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/tradedAssets", GetSearchAssets).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/indices/{slug}", GetIndexData).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/indices-table", GetIndexTableData).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/price-explanation", GetExplanationData).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/ads-config", GetAdsConfig).Methods(http.MethodGet, http.MethodOptions)

	r.PathPrefix("/docs").Handler(httpauth.SimpleBasicAuth(os.Getenv("DOCS_USERNAME"), os.Getenv("DOCS_PASSWORD"))(http.StripPrefix("/docs", http.FileServer(http.Dir("./docs"))))).Methods(http.MethodGet, http.MethodOptions)

	// API Versioning Setup
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.HandleFunc("/chart/{period}/{symbol}", microservices.GetChartData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/fundamentals/{period}/{symbol}", GetFundamentalsHandler).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/related-cryptos/{period}/{exchange}", GetRelatedCryptosForExchange).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/content/crypto/{slug}", GetAssetsProfilesHandler).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/content/exchange/{slug}", GetExchangeProfilesHandler).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/newsfeed", microservices.GetNewsFeedData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/leaders-laggards", GetLeadersLaggards).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/dashboard-content", GetDashboardContent).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/carousel", microservices.GetCarouselData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/tradedAssets", GetSearchAssets).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/indices/{slug}", GetIndexData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/indices-table", GetIndexTableData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/price-explanation", GetExplanationData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/events", microservices.GetEventsData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/research", microservices.GetResearchData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/editorsPick", microservices.GetEditorsPickData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/allProfiles", microservices.GetAllProfiles).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/ads-config", GetAdsConfig).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/calculator-assets", GetCalculatorAssetsData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/education/", microservices.GetEducation).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/dynamic-description", microservices.GetDynamicDescriptionData).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/categories", GetCategories).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/featured-categories", GetFeaturedCategories).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/crypto-list-section", GetListsSection).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/videos", GetVideos).Methods(http.MethodGet, http.MethodOptions)

	// news topics endpoints
	v1.HandleFunc("/trending-topics/", microservices.GetTrendingTopics).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/news-topic/{slug}/", microservices.GetNewsTopic).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/topics-categories/", microservices.GetNewsTopicCategories).Methods(http.MethodGet, http.MethodOptions)
	v1.HandleFunc("/news-topic-bubbles/", microservices.GetTopicBubbles).Methods(http.MethodGet, http.MethodOptions)

	// nfts page endpoints
	nfts := v1.PathPrefix("/nfts").Subrouter()
	nfts.HandleFunc("/chains", microservices.GetNFTChains).Methods(http.MethodGet, http.MethodOptions)
	nfts.HandleFunc("/prices", GetNFTPrices).Methods(http.MethodGet, http.MethodOptions)

	// Community Page endpoints
	community := v1.PathPrefix("/community").Subrouter()
	community.HandleFunc("/announcements", microservices.GetAnnouncementsData).Methods(http.MethodGet, http.MethodOptions)
	community.HandleFunc("/premium-articles", microservices.GetPremiumArticles).Methods(http.MethodGet, http.MethodOptions)
	community.HandleFunc("/faq", microservices.GetFrequentlyAskedQuestions).Methods(http.MethodGet, http.MethodOptions)

	v1.HandleFunc("/tweets", GetTweets).Methods(http.MethodGet, http.MethodOptions)

	// beta endpoints -- deprecated
	beta := v1.PathPrefix("/beta").Subrouter()
	beta.Handle("/register-user", http.HandlerFunc(RegisterBetaUser)).Methods(http.MethodPost)
	beta.Handle("/verify-user", http.HandlerFunc(VerifyBetaUser)).Methods(http.MethodGet)

	// /v2 API Versioning Setup
	v2 := r.PathPrefix("/v2").Subrouter()

	// V2 Build Api any new build api that will be under the new structure we will add it here.
	build := v2.PathPrefix("/build").Subrouter()
	build.Handle("/newsfeed", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildNewsFeedData))).Methods(http.MethodPost)
	build.Handle("/editorsPick", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildEditorsPickData))).Methods(http.MethodPost)
	build.Handle("/topics-ds", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildTopicsFromDS))).Methods(http.MethodPost)
	build.Handle("/update-trending-topics", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.UpdateTrendingTopics))).Methods(http.MethodPost)
	build.Handle("/topics-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildNewsTopicsCategories))).Methods(http.MethodPost)
	build.Handle("/research", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildResearchData))).Methods(http.MethodPost)
	build.Handle("/events", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildEventsData))).Methods(http.MethodPost)
	build.Handle("/all-profiles", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildAllProfiles))).Methods(http.MethodPost)
	build.Handle("/custom-nft-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildNFTCustomCategoriesData))).Methods(http.MethodPost)
	build.Handle("/custom-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildCustomCategoriesData))).Methods(http.MethodPost)

	v2.HandleFunc("/tradedAssets", GetSearchAssets).Methods(http.MethodGet, http.MethodOptions)
	v2.HandleFunc("/education/", microservices.GetEducation).Methods(http.MethodGet, http.MethodOptions)
	v2.HandleFunc("/chart/{period}/{symbol}", microservices.GetChartData).Methods(http.MethodGet, http.MethodOptions)
	v2.HandleFunc("/dynamic-description", microservices.GetDynamicDescriptionData).Methods(http.MethodGet, http.MethodOptions)
	v2.HandleFunc("/extendedprofile", HandleExtendedProfileRequest).Methods(http.MethodPost, http.MethodOptions)

	v2.HandleFunc("/watch", microservices.AddToWatchlist).Methods(http.MethodPost, http.MethodOptions)
	v2.HandleFunc("/unwatch", microservices.RemoveAssetFromWatchlist).Methods(http.MethodPost, http.MethodOptions)
	v2.HandleFunc("/watchlist", microservices.GetWatchlist).Methods(http.MethodGet, http.MethodOptions)
	v2.HandleFunc("/portfolios", microservices.GetPortfolio).Methods(http.MethodGet, http.MethodOptions)

	v2.HandleFunc("/search/{dataset}", microservices.Search).Methods(http.MethodGet, http.MethodOptions)

	build.HandleFunc("/videos", microservices.BuildVideos).Methods(http.MethodGet, http.MethodOptions)

	// V2 NFT related endpoints
	nftRouter := v2.PathPrefix("/nft").Subrouter()
	nftRouter.HandleFunc("/collection/{slug}", microservices.GetNftCollection).Methods(http.MethodGet, http.MethodOptions)
	nftRouter.HandleFunc("/chains", microservices.GetNFTChains).Methods(http.MethodGet, http.MethodOptions)
	nftRouter.HandleFunc("/search/{dataset}", microservices.Search).Methods(http.MethodGet, http.MethodOptions)

	// V2 get Content Endpoints
	content := v2.PathPrefix("/content").Subrouter()
	content.HandleFunc("/newsfeed", microservices.GetNewsFeedData).Methods(http.MethodGet, http.MethodOptions)
	content.HandleFunc("/editorsPick", microservices.GetEditorsPickData).Methods(http.MethodGet, http.MethodOptions)
	content.HandleFunc("/dynamic-description", microservices.GetDynamicDescriptionData).Methods(http.MethodGet, http.MethodOptions)
	content.HandleFunc("/research", microservices.GetResearchData).Methods(http.MethodGet, http.MethodOptions)
	content.HandleFunc("/events", microservices.GetEventsData).Methods(http.MethodGet, http.MethodOptions)
	content.HandleFunc("/allProfiles", microservices.GetAllProfiles).Methods(http.MethodGet, http.MethodOptions)

	// V2 get exchange related endpoints
	exchanges := v2.PathPrefix("/exchanges").Subrouter()
	exchanges.HandleFunc("/slugs", microservices.GetAllExchangeSlugs).Methods(http.MethodGet, http.MethodOptions)

	// V2 Community Page endpoints
	community = v2.PathPrefix("/community").Subrouter()
	community.HandleFunc("/announcements", microservices.GetAnnouncementsData).Methods(http.MethodGet, http.MethodOptions)
	community.HandleFunc("/premium-articles", microservices.GetPremiumArticles).Methods(http.MethodGet, http.MethodOptions)
	community.HandleFunc("/faq", microservices.GetFrequentlyAskedQuestions).Methods(http.MethodGet, http.MethodOptions)

	// Landing Page Featured Categories
	landingPage := v2.PathPrefix("/landing-page").Subrouter()
	landingPage.HandleFunc("/featured-categories-content/", microservices.GetLandingPageFeaturedCategoriesArticles).Methods(http.MethodGet, http.MethodOptions)
	landingPage.HandleFunc("/articles-content/", microservices.GetLandingPageArticles).Methods(http.MethodGet, http.MethodOptions)
	landingPage.HandleFunc("/featured-categories/", microservices.GetLandingPageCategories).Methods(http.MethodGet, http.MethodOptions)

	// Crypto Price Page Categories and Featured Categories
	cryptoPrice := v2.PathPrefix("/crypto").Subrouter()
	cryptoPrice.HandleFunc("/featured-categories", microservices.GetCryptoFeaturedCategories).Methods(http.MethodGet, http.MethodOptions)
	cryptoPrice.HandleFunc("/categories", microservices.GetCryptoCategories).Methods(http.MethodGet, http.MethodOptions)
	cryptoPrice.HandleFunc("/carousel", microservices.GetCarouselData).Methods(http.MethodGet, http.MethodOptions)

	// V2 Education Learn Tap
	education := v2.PathPrefix("/education").Subrouter()
	education.HandleFunc("/learn/", microservices.GetEducation).Methods(http.MethodGet, http.MethodOptions)

	// V2 Topics
	topics := v2.PathPrefix("/topics").Subrouter()
	topics.HandleFunc("/trending/", microservices.GetTrendingTopics).Methods(http.MethodGet, http.MethodOptions)
	topics.HandleFunc("/news/{slug}/", microservices.GetNewsTopic).Methods(http.MethodGet, http.MethodOptions)
	topics.HandleFunc("/categories/", microservices.GetNewsTopicCategories).Methods(http.MethodGet, http.MethodOptions)
	topics.HandleFunc("/bubbles/", microservices.GetTopicBubbles).Methods(http.MethodGet, http.MethodOptions)
	topics.HandleFunc("/slugs", microservices.GetAllTopicSlugs).Methods(http.MethodGet, http.MethodOptions)

	//Videos v2
	videos := v2.PathPrefix("/videos").Subrouter()
	videos.HandleFunc("/clearCache", microservices.ClearCache).Methods(http.MethodPost, http.MethodOptions)

	// trigger functions for rowy ui
	rowyTrigger := v1.PathPrefix("/rowy-trigger").Subrouter()
	rowyTrigger.Handle("/update-featured-content", http.HandlerFunc(UpdateFeaturedAndPromotedContent)).Methods(http.MethodPost)

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://www.forbes.com/digital-assets/", http.StatusMovedPermanently)
	})

	r.Handle("/favicon.ico", http.NotFoundHandler())
	r.HandleFunc("/version", versionHandler).Methods(http.MethodGet, http.MethodOptions)

	//RefreshMarkets()

	initServer(r)

	closeConnections()

	os.Exit(0)
}

func closeConnections() {
	// Close PG Connection
	store.PGClose()

	// Close BQ Connection
	store.BQClose()

	// Close Firestore Connection
	store.FSClose()
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"version": "` + version + `"}`))
}

// max-age in Cache-Control takes time in second we can calculate the time by using the equation (second * minute * hour)
// Example we need max-age value for one Hour the equation (60 * 60) = 3600
func setResponseHeaders(w http.ResponseWriter, cacheTime int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	cacheValue := fmt.Sprintf("max-age=%v, public", cacheTime)
	w.Header().Set("Cache-Control", cacheValue)
}

/*
	healthcheck: returns a 200 status. Used by devops to check that the instance
	is functioning.
*/

func healthcheck(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "healthcheck"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("Start healthcheck")
	startTime := log.StartTimeL(labels, "healthcheck")

	log.EndTime("healthcheck", startTime, nil)

	span.SetStatus(codes.Ok, "healthcheck completed successfully")

	w.Write([]byte("ok"))
}

/*



/*
	This endpoint takes a session id and passes it to zephr.
	That session Id is then looked up to see if it belongs to a user
	If it does belong to a user then we register mark the user as a beta user
	in zephr if the request was made within our enrollment period, and we did not hit
	the max user limit. (The enrolment period, and user limit can be adjusted in firestore)
*/

func RegisterBetaUser(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "RegisterBetaUser"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("Start RegisterBetaUser")
	startTime := log.StartTimeL(labels, "RegisterBetaUser")

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var reqBody model.BetaUserRequest
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	isRegistered, err := services.RegisterBetaUser(r.Context(), reqBody.Zephr_SessionID)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := model.RegisterBetaUserResponse{WasRegistered: isRegistered}

	log.EndTime("RegisterBetaUser", startTime, nil)

	span.SetStatus(codes.Ok, "request completed successfully")

	respBody, err := json.Marshal(resp)
	w.WriteHeader(200)
	w.Write(respBody)
}

/*
This endpoint takes a session id and passes it to zephr.
That session Id is then looked up to see if it belongs to a user
If it does belong to a user then we look up the user attributes to
see if they have the isfdabetauser equal to true
if so return true, else it returns false
*/
func VerifyBetaUser(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	//services.Test()
	sid := r.URL.Query().Get("sessionID")
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "VerifyBetaUser"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("start VerifyBetaUser")
	startTime := log.StartTimeL(labels, "VerifyBetaUser")

	isRegistered, err := services.VerifyBetaUser(r.Context(), sid)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := model.VerifyBetaUserResponse{IsBetaUser: isRegistered}

	log.EndTime("VerifyBetaUser", startTime, nil)

	span.SetStatus(codes.Ok, "request completed successfully")

	respBody, err := json.Marshal(resp)
	w.WriteHeader(200)
	w.Write(respBody)
}

// Todo we need to remove this function because we move to the new structure CSR
func UpdateNewsFeedHandler(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Update News Feed Handler")

	err := services.UpdateNewsFeed()
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTime("Update News Feed Handler", startTime, nil)
}

func GetFundamentalsHandler(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Get Fundamentals Data")
	// updated each 35 minute
	setResponseHeaders(w, 30)
	vars := mux.Vars(r)
	sym := vars["symbol"]
	period := vars["period"]

	// gets span from the context (middleware created)
	span := trace.SpanFromContext(r.Context())
	defer span.End()
	span.SetAttributes(attribute.String("symbol", sym), attribute.String("period", period))

	result, err := store.GetFundamentals(r.Context(), sym, period)

	if result == nil && err == nil {
		span.SetStatus(codes.Error, "No Data Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	span.SetStatus(codes.Ok, "request completed successfully")

	log.EndTime("Get Fundamentals", startTime, nil)

	w.Write(result)
}

func GetRelatedCryptosForExchange(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 2100)
	vars := mux.Vars(r)
	slug := vars["exchange"]

	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "GetRelatedCryptosForExchange"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Get RelatedCryptos For Exchange")

	exchangeProfile, exchangeErr := store.GetExchangeProfilePG(r.Context(), slug)
	if exchangeErr != nil {
		log.ErrorL(labels, "%s", exchangeErr)
		span.SetStatus(codes.Error, exchangeErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if exchangeProfile == nil {
		log.ErrorL(labels, "%s Exchange not found", slug)
		span.SetStatus(codes.Error, "No Exchange Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	result, err := store.GetRelatedAssetsForExchangePG(labels, r.Context(), exchangeProfile.Name)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "Get RelatedCryptos For Exchange", startTime, nil)
	span.SetStatus(codes.Ok, "Success")
	w.WriteHeader(200)
	w.Write(result)
}

func BuildChartHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	var wg sync.WaitGroup
	period := vars["period"]
	setResponseHeaders(w, 60)
	labels := make(map[string]string)

	labels["period"] = period
	labels["function"] = "BuildChartHandler"
	labels["UUID"] = uuid.New().String()

	startTime := log.StartTimeL(labels, "Get Index Data")
	log.DebugL(labels, "Chart Data Build Process Started at :: %s for Period :: %s", startTime, period)

	var result []store.TimeSeriesResultPG
	var qErr error

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// get chart data fro BQ by Interval
	switch period {
	case "24h":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		// This section we add it here to build the charts data for categories.
		// This process will get the data for 24 hour interval
		// It fetch the data from BG and then build the data we need for the Charts.
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Category, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "7d":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_SevenDay, store.ResSeconds_14400, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_SevenDay, store.ResSeconds_14400, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		// This section we add it here to build the charts data for categories.
		// This process will get the data for 7 days interval
		// It fetch the data from BG and then build the data we need for the Charts.
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_SevenDay, store.ResSeconds_14400, labels["UUID"], store.Category, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "30d":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_ThirtyDay, store.ResSeconds_43200, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_ThirtyDay, store.ResSeconds_43200, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		// This section we add it here to build the charts data for categories.
		// This process will get the data for 30 days interval
		// It fetch the data from BG and then build the data we need for the Charts.
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_ThirtyDay, store.ResSeconds_43200, labels["UUID"], store.Category, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "1y":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneYear, store.ResSeconds_432000, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneYear, store.ResSeconds_432000, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		// This section we add it here to build the charts data for categories.
		// This process will get the data for 365 days interval
		// It fetch the data from BG and then build the data we need for the Charts.
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneYear, store.ResSeconds_432000, labels["UUID"], store.Category, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "max":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_Max, store.ResSeconds_1296000, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_Max, store.ResSeconds_1296000, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		// This section we add it here to build the charts data for categories.
		// This process will get the data for max interval
		// It fetch the data from BG and then build the data we need for the Charts.
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_Max, store.ResSeconds_1296000, labels["UUID"], store.Category, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	wg.Wait()
	if qErr != nil {
		log.Error("%s", qErr)
		w.WriteHeader(http.StatusInternalServerError)
	}

	err = store.InsertNomicsChartData(r.Context(), period, result)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "Chart Data Build ", startTime, nil)

	w.Write([]byte("ok"))

}

func BuildExchangeFundamentalsHandler(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 0)
	vars := mux.Vars(r)
	period := vars["period"]
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "BuildExchangeFundamentalsHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "Build Exchange Fundamentals Data ")

	g, ctx := errgroup.WithContext(r.Context())

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Go Routine 1
	// Get The Exchange Metadata elements needed for the Exchanges Fundamentals
	// this will get all exchanges metadata
	var exchangesMetaData []model.CoingeckoExchangeMetadata
	g.Go(func() error {
		results, err := store.GetExchangeMetaDataWithoutLimit(ctx, labels)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Metadata CG from PG: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Metadata CG %d results from PG", len(results))

		exchangesMetaData = results
		fmt.Println(len(exchangesMetaData))
		return nil

	})

	// Go Routine 2
	// Get The Exchanges Tickers needed for the Exchanges Fundamentals
	exchangeResults := make(map[string]store.ExchangeResults)
	g.Go(func() error {
		results, err := bqs.ExchangeFundamentalsCG(ctx, labels["UUID"])
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Tickers Fundamentals CG from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Tickers Fundamentals CG %d results from BQ", len(results))

		exchangeResults = results
		fmt.Println(len(exchangeResults))

		return nil

	})

	// Results from Go Routine 3
	// List of exchangesProfiles in Map of [Name]ExchangeProfile
	exchangesProfiles := make(map[string]model.ExchangeProfile)

	// Go Routine 3
	// Get all Exchange profiles from FS (rowy tables)
	g.Go(func() error {

		e, err := store.GetExchanges(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting exchanges from rowy: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from FS Exchanges", len(e))

		exchangesProfiles = e
		fmt.Println(len(exchangesProfiles))

		return nil
	})

	span.AddEvent("Waiting for Go Routines to finish")
	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Starts new WaitGroup for the next set of go routines - which don't need to return an error
	var (
		wg sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 10)
	)

	for _, v := range exchangesMetaData {

		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, v model.CoingeckoExchangeMetadata, uuid string) {
			ctx, span := tracer.Start(ctxO, "Go Routine BuildExchangeFundamentalsHandler")
			defer span.End()
			label := make(map[string]string)
			label["symbol"] = v.Name
			span.SetAttributes(attribute.String("exchange", v.Name))
			label["period"] = period
			span.SetAttributes(attribute.String("period", period))
			label["uuid"] = uuid
			span.SetAttributes(attribute.String("uuid", uuid))
			label["spanID"] = span.SpanContext().SpanID().String()
			label["traceID"] = span.SpanContext().TraceID().String()
			// check if the exchange metadata exist in exchange tickers
			if exchangeDataFromCG, ok := exchangeResults[v.ID]; ok {

				// map the exchange metadata to exchanges tickers to build exchange
				e, err := store.CombineExchanges(ctx, v, exchangeDataFromCG, exchangesProfiles)

				if err != nil {
					log.ErrorL(label, "Error combining Exchange Fundamentals for %s: %s", v.ID, err.Error())
					goto waitReturn // If there is an error, skip to the end of the go routine
				}

				// Save the Exchanges Fundamentals to PG
				err = store.InsertExchangeFundamentals(ctx, e, label)
				if err != nil {
					log.ErrorL(label, "Error saving Exchange Fundamentals %s", err)
				}
				// Save the latest Exchanges Fundamentals to PG
				store.InsertExchangeFundamentalsLatest(ctx, e, label)
			}

		waitReturn:
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()

		}(r.Context(), v, labels["UUID"])

	}

	wg.Wait()
	log.EndTimeL(labels, "Exchange Fundamentals CG Build ", startTime, nil)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
	span.SetStatus(codes.Ok, "Exchange Fundamentals CG Built")

}

func RebalanceIndex(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Rebalance Index ")
	status := r.URL.Query().Get("status")
	res, rebalancingTime, err := store.GetIndexContent()
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	indexErr := store.MapIndexData(res, rebalancingTime, status)

	if indexErr != nil {
		log.Error("%s", err)
	}

	log.EndTime("Rebalance Index ", startTime, nil)
}

func UpdateIndex(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Update Index")
	err := store.UpdateIndexContentData()
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTime("Update Index ", startTime, nil)
}

func GetDataFromSpreadsheet(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Get Data From Spreadsheet")
	var sheetSource services.SheetsSource
	sheetSource.Read(os.Getenv("CONTENT_SHEET_ID"))

	w.WriteHeader(200)
	log.EndTime("Get Data From Spreadsheet ", startTime, nil)

}

// New landing page related content in one common API. This contains featured Content, Promoted Content, Hero text & Tip of the day section (today's highlights).
func GetDashboardContent(w http.ResponseWriter, r *http.Request) {
	// updated each 1 minute
	setResponseHeaders(w, 60)

	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels := make(map[string]string)
	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetTodayHighlights"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "Get Dashboard Content")

	data, dashboardErr := content.GetDashboard(r.Context())

	// Returns the data (name, description & link) for today's highlights article.
	tipOfTheDay, tipErr := store.GetTodayHighlights(r.Context())

	if (data == nil && dashboardErr == nil) || // If there is no data returned from GetDashboard
		dashboardErr != nil || // If there is an error returned from GetDashboard
		tipErr != nil { // If there is an error returned from GetTodayHighlights
		var err error
		if data == nil && dashboardErr == nil {
			w.WriteHeader(http.StatusInternalServerError)
			err = fmt.Errorf("no data returned from GetDashboard")
		} else if dashboardErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			err = dashboardErr
		} else if tipErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			err = tipErr
		}
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	data.HeroText = config.HeroText
	data.TipOfTheDay = *tipOfTheDay

	res, err := json.Marshal(data)

	log.EndTimeL(labels, "Get Dashboard Content", startTime, err)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Get Dashboard Content")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

// Return an asset's metadata profile (from PG), along with the metadata description from Firestore.
func GetAssetsProfilesHandler(w http.ResponseWriter, r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels := make(map[string]string)
	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetTodayHighlights"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	startTime := log.StartTimeL(labels, "Get Assets Profiles Data")

	// updated each 1 minute
	setResponseHeaders(w, 60)
	vars := mux.Vars(r)
	slug := vars["slug"]

	// var sheetSource services.SheetsSource

	// data, err := sheetSource.GetAssetsData(slug)
	var (
		data *model.AssetProfile
		err  error
	)
	data, err = store.GetCyptoContent(r.Context(), slug, config)

	if data == nil && err == nil {
		span.SetStatus(codes.Error, "Not Found")
		log.EndTimeL(labels, "Get Asset Profiles Not found", startTime, nil)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.EndTimeL(labels, "Get Asset Profiles", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	token, err := store.GetForbesTokenMetadata(r.Context(), data.Symbol)
	if token != nil && (*token).MetadataDescription != "" {
		data.ForbesMetaDataDescription = &(*token).MetadataDescription
	}

	log.EndTimeL(labels, "Get Assets Profile", startTime, nil)
	res, err := json.Marshal(data)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(res)
}

func GetLeadersLaggards(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Get Leaders Laggards Data")
	// updated each 35 minute
	setResponseHeaders(w, 2100)

	response, err := store.GetLeadersAndLaggards(r.Context())

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTime("Get Leaders Laggards", startTime, nil)
	w.Write(response)
}

func GetExchangeProfilesHandler(w http.ResponseWriter, r *http.Request) {
	var results []byte
	setResponseHeaders(w, 60)
	vars := mux.Vars(r)
	slug := vars["slug"]

	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "GetExchangeProfiles"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Get Exchange Profiles")

	data, err := store.GetExchangeProfilePG(r.Context(), slug)

	if data == nil && err == nil {
		log.EndTimeL(labels, "Get Exchange Profiles", startTime, nil)
		span.SetStatus(codes.Error, "Not Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err == nil {
		results, err = json.Marshal(*data)
	}

	log.EndTimeL(labels, "Get Exchange Profiles", startTime, err)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		span.SetStatus(codes.Ok, "Success")
		w.Write(results)
	}
}

func GetIndexData(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Get Index Data")
	// updated each 1 minute
	setResponseHeaders(w, 60)
	vars := mux.Vars(r)
	slug := vars["slug"]

	var sheetSource services.SheetsSource

	data, err := sheetSource.GetIndexData(slug)

	if data == nil && err == nil {
		w.WriteHeader(http.StatusNotFound)
	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		log.EndTime("Get Index Data ", startTime, nil)
		w.Write(data)
	}
}

func GetIndexTableData(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Get Index Table Data ")
	// updated each 15 minute
	setResponseHeaders(w, 900)

	data, err := store.GetIndexData()

	if data == nil && err == nil {
		w.WriteHeader(http.StatusNotFound)
	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		log.EndTime("Get Index Table Data", startTime, nil)
		w.Write(data)
	}
}

func BuildIndexChart(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Build Index Chart")
	vars := mux.Vars(r)
	period := vars["period"]

	bq, err := store.NewBQStore()
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	var qErr error
	var result store.TimeSeriesResult

	switch period {
	case "24h":
		result, qErr = bq.QueryIndexDaily()
	case "7d":
		result, qErr = bq.QueryIndex7Days()
	case "30d":
		result, qErr = bq.QueryIndex30Days()
	case "1y":
		result, qErr = bq.QueryIndex1Year()
	case "max":
		result, qErr = bq.QueryIndexMax()
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if qErr != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	fs := store.GetFirestoreClient()
	ctx := context.Background()

	collectionName := fmt.Sprintf("chart_data%s", os.Getenv("DATA_NAMESPACE"))

	docName := fmt.Sprintf("%s_%s", result.Symbol, period)
	fs.Collection(collectionName).Doc(docName).Set(ctx, result)
	log.EndTime("Build Index Chart ", startTime, nil)

	w.WriteHeader(200)
}

func GetExplanationData(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Get Explanation Data")
	// updated each 1 minute
	setResponseHeaders(w, 60)

	response, err := services.GetExplanations(r.Context())

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.EndTime("Get Explanation Data ", startTime, nil)
	w.Write(response)
}

func BuildEventsData(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Build Events Data")
	var sheetSource services.SheetsSource
	sheetSource.ReadEventsData(os.Getenv("EVENTS_SHEET_ID"))
	log.EndTime("Build Events Data ", startTime, nil)
	w.WriteHeader(200)
}

func GetResearchData(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Get Research Data")

	err := services.UpdateResearchData(r.Context())
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.EndTime("Get Research Data ", startTime, nil)
	w.WriteHeader(200)
}

func GetEditorsPickData(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Get Editors Pick Data")
	editorsPicksData, err := services.BuildEditorsPicksData(r.Context())
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if editorsPicksData.PromotedContent.ContentPositions == nil || len(editorsPicksData.PromotedContent.ContentPositions) == 0 {
		feed, err := services.FetchEditorsPicks(r.Context())
		if err != nil {
			log.Error("%s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for i, v := range feed.NewsFeedItems {

			authorGroup, err := services.GetAuthorGroup(r.Context(), v.PrimaryAuthor, v.Publication)
			if err != nil {
				log.Error("%s", err)
			}

			item := model.ContentPositions{
				Position:    i + 1,
				Type:        "",
				Title:       v.Title,
				Image:       v.Image,
				Description: "",
				URI:         v.URI,
				ID:          "",
				Authors: []model.Authors{
					{
						NaturalID:  "",
						Name:       v.Author.Name,
						Avatars:    []model.Avatars{},
						URL:        "",
						Type:       v.Author.Type,
						ProfileURL: "",
						AuthorType: v.Author.AuthorType,
						Blog:       false,
						BlogName:   "",
					},
				},
				Date:                v.Date.Unix(),
				BlogType:            "",
				NaturalID:           "",
				BertieBadges:        []interface{}{},
				Magazine:            model.Magazine{},
				HideDescription:     false,
				FullImage:           false,
				Sponsored:           false,
				RemoveTopPadding:    false,
				RemoveBottomPadding: false,
				AuthorGroup:         authorGroup,
				BlogName:            "",
			}
			editorsPicksData.PromotedContent.ContentPositions = append(editorsPicksData.PromotedContent.ContentPositions, item)
		}

		log.Info("Fetched editors picks from feed - Count %d", len(editorsPicksData.PromotedContent.ContentPositions))

		err = services.SaveEditorsPicks(r.Context(), editorsPicksData)
		if err != nil {
			log.Error("%s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	log.EndTime("Get Editors Pick Data ", startTime, nil)
	w.WriteHeader(200)
}

// Called every 24 hrs
func CacheBertieProfiles(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Cache All Profiles")
	// updated each 24 hours
	setResponseHeaders(w, 86400)

	var assets []*model.BertieProfile

	profiles, err := store.GetBertieProfilesPG(r.Context())
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if profiles == nil && err == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	for _, v := range *profiles {

		asset := &model.BertieProfile{
			Symbol:      strings.ToUpper(v.Symbol),
			Name:        v.Name,
			Slug:        v.Slug,
			Logo:        v.Logo,
			ProfileLink: "https://www.forbes.com/digital-assets/assets/" + v.Slug + "/",
		}

		assets = append(assets, asset)
	}

	data, err := json.Marshal(assets)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		goto end
	}

	err = store.UpdateAllProfilesCache(data)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		goto end
	}

	log.EndTime("Cache All Profiles  ", startTime, nil)

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		goto end
	}

end:
	return
}

func UpdateConfig(w http.ResponseWriter, r *http.Request) {

	newConfig, err := services.LoadFDAConfig()
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	mu.Lock()
	config = newConfig
	mu.Unlock()

	w.WriteHeader(200)

}

func UpdatePortfolioConfig(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Update Portfolio Config")

	err := services.UpdatePortfolioConfigInfo(r.Context())

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)

	}
	log.EndTime("Update Portfolio Config ", startTime, nil)

	w.WriteHeader(200)

}
func UpdatePortfolioPrices(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Update Portfolio Prices")

	var allocations []model.PortfolioAllocation
	var areMarketsOpen = false

	//Loading location "America/New_York" then setting the time.now().in() to that location.
	//Daylight Savings is the worst.
	loc, er := time.LoadLocation("America/New_York")
	if er != nil {
		log.Error("%s", er)
	} else {

		now := time.Now().In(loc)

		marketOpen := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, now.Location())
		marketClose := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, now.Location())

		if now.Weekday() != time.Sunday && now.Weekday() != time.Saturday && now.After(marketOpen) && now.Before(marketClose) {
			areMarketsOpen = true
		} else {
			log.Info("markets are closed")
			log.Debug(now.GoString())
			log.Debug(marketOpen.String())
			log.Debug(marketClose.String())

		}
	}
	allocations, err := services.GetAllocationDataFromCollection(fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "portfolioAllocations"), r.Context())
	for _, v := range allocations {
		//There are some prices for assets that FDA might not have.
		// We Default to "unknown price" if the current price is empty.
		//This way the editor will be able to see that they may have to enter the price manually
		if v.CurrentPrice == "" {
			v.CurrentPrice = "unknown price"
		}
		if v.AssetType == "etf" || v.AssetType == "stock" {
			if areMarketsOpen == true {
				// If we dont have a ticker we dont want to call ignite.
				// But we still want to save updates if there is an unkown price
				// This will serve as an indicator to the rowy user that there is something wrong
				if v.Ticker != "" {
					xigniteReq, err := services.MakeIgniteRequest(v.Ticker)

					if err != nil {
						log.Error("%s", err)
						continue
					}

					if len(xigniteReq) > 0 {
						v.CurrentPrice = fmt.Sprintf("$%f", xigniteReq[0].Last)

					}
				}
			} else {
				continue
			}

		} else if v.AssetType == "crypto" {
			fundamentalsData, err := store.GetPortfolioPricesPG(r.Context(), v.Ticker)
			if fundamentalsData == nil && err == nil {
				log.Info("Ticker " + v.Ticker + " not found")
				continue
			}

			if err != nil {
				log.Error("%s", err)
				log.Error("Ticker " + v.Ticker + " not updated")
				continue
			}

			//When the asset fundamental is found, we update it in the portfolio allocation.
			if fundamentalsData != nil {
				if fundamentalsData.Logo != "" {
					v.Logo = fundamentalsData.Logo
				}
				v.CurrentPrice = fmt.Sprintf("$%f", *fundamentalsData.Price24h)
				v.Symbol = fundamentalsData.Symbol
			}

		}

		v.PriceLastUpdated = time.Now().Local()
		err = services.SavePortfolioAllocation(r.Context(), v)

		if err != nil {
			log.Error("%s%+v", err, v)
		}

	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTime("Update Portfolio Prices  ", startTime, nil)

	w.WriteHeader(200)
}

func UpdateAdsConfig(w http.ResponseWriter, r *http.Request) {
	startTime := log.StartTime("Update Ads Config")
	err := services.UpdateAdsConfig(r.Context())

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTime("Update Ads Config  ", startTime, nil)

	w.WriteHeader(200)
}

func GetAdsConfig(w http.ResponseWriter, r *http.Request) {

	startTime := log.StartTime("Get Ads Config")
	// updated each 5 minute
	setResponseHeaders(w, 300)

	data, err := services.GetAdsConfig(r.Context())

	if data == nil && err == nil {
		w.WriteHeader(http.StatusNotFound)
	}

	dataJson, err := json.Marshal(data)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		log.EndTime("Get Ads Config ", startTime, nil)
		w.Write(dataJson)
	}
}

// get all assets data from fundamentalslatest for Convert Calculator Assets page
func GetCalculatorAssetsData(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)

	// updated each 5 minute
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetAssetsCalculatorData"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Assets Calculator Data")

	result, err := store.GetAssetsData(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	jsonData, err := json.Marshal(result)

	if jsonData == nil && err == nil {
		span.SetStatus(codes.Error, "No Data Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	log.EndTimeL(labels, "Assets Calculator Data", startTime, nil)
	span.SetStatus(codes.Ok, "Assets Calculator Data")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}

// Adds the metadata description fields to the BigQuery Chatbot data table for all the assets.
func BuildAssetsChatbotData(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 0)

	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "BuildAssetsChatbotData"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Asset Chatbot data")

	// Go Routine with error group for error handling
	g, ctx := errgroup.WithContext(r.Context())

	// Results from Go Routine 1
	// List of assets for which we want to build the chatbot table.
	var assets *[]store.Fundamentals

	// Go Routine 1
	// Get all assets' relevant fields (eg. name & symbol ) from fundamentalslatest - postgres.
	g.Go(func() error {

		d, err := store.GetAssetsSEOData(ctx, true)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting AssetsSEOData from pg: " + err.Error())
		}

		assets = d
		return nil
	})

	var fsTokens *[]model.ForbesMetadata
	// Go Routine 2
	// Get The Asset Metdata elements needed for the fundamentals
	g.Go(func() error {
		results, err := store.GetAllForbesTokenMetadata(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Forbes token metadata from FS: " + err.Error())
		}
		log.DebugL(labels, "Received a total %d tokens from FS", len(*results))

		fsTokens = results
		return nil

	})

	span.AddEvent("Waiting for Go Routines to finish")
	err := g.Wait() // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	chatbotAssets := store.CalculateChatbotAssets(r.Context(), assets, fsTokens)

	bqs, err := store.NewBQStore()
	err = bqs.UpsertChatbotData(r.Context(), labels["UUID"], chatbotAssets)

	if err != nil {
		log.ErrorL(labels, "Error upserting chatbot data : %s", err.Error())
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		return
	}

	log.EndTimeL(labels, "Assets SEO metadata description ", startTime, nil)
	span.SetStatus(codes.Ok, "BuildForbesTokenMetadata Built")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

// Adds the seo metadata fields to a Rowy table (Forbes token metadata) for all the assets.
func BuildForbesTokenMetadata(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 0)

	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "BuildForbesTokenMetadata"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Asset SEO Metadata")

	// Go Routine with error group for error handling
	g, ctx := errgroup.WithContext(r.Context())

	// Results from Go Routine 1
	// List of assets for which we want to build metadata descriptions.
	var assets *[]store.Fundamentals

	// Go Routine 1
	// Get all assets' relevant fields (eg. name & symbol ) from fundamentalslatest - postgres.
	g.Go(func() error {

		d, err := store.GetAssetsSEOData(ctx, false)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting AssetsSEOData from pg: " + err.Error())
		}

		assets = d
		return nil
	})

	var fsTokens *[]model.ForbesMetadata
	// Go Routine 2
	// Get The Asset Metdata elements needed for the fundamentals
	g.Go(func() error {
		results, err := store.GetAllForbesTokenMetadata(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Forbes token metadata from FS: " + err.Error())
		}
		log.DebugL(labels, "Received a total %d tokens from FS", len(*results))

		fsTokens = results
		return nil

	})

	span.AddEvent("Waiting for Go Routines to finish")
	err := g.Wait() // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Starts new WaitGroup for the next set of go routines - which don't need to return an error
	var (
		wg sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 20)
	)

	for _, asset := range *assets {
		fsToken := model.ForbesMetadata{}
		// find the asset from fsTokens
		for _, tok := range *fsTokens {
			if asset.Symbol == tok.AssetId {
				fsToken = tok
				break
			}
		}

		//We don't want to update the metadata if it already exists
		if fsToken.MetadataDescription != "" {
			continue
		}

		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, uuid string, fsToken model.ForbesMetadata, asset store.Fundamentals) {
			ctx, span := tracer.Start(ctxO, "Go Routine BuildForbesTokenMetadata")
			defer span.End()

			label := make(map[string]string)
			label["symbol"] = asset.Symbol
			span.SetAttributes(attribute.String("symbol", asset.Symbol))
			label["uuid"] = uuid
			span.SetAttributes(attribute.String("uuid", uuid))
			label["spanID"] = span.SpanContext().SpanID().String()
			label["traceID"] = span.SpanContext().TraceID().String()

			fsToken.MetadataDescription = getRandomMetadataDescription(asset)
			fsToken.Symbol = asset.DisplaySymbol
			fsToken.AssetId = asset.Symbol

			// Function creates a new entry in firestore if it doesn't exist. Or Updates an existing entry in firestore if an entry with empty description exists there.

			err := store.UpsertForbesTokenMetadata(ctx, fsToken)

			if err != nil {
				log.ErrorL(label, "Error upserting Token metadata for %s: %s", asset.Symbol, err.Error())
			}

			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()
		}(r.Context(), labels["UUID"], fsToken, asset)
	}

	wg.Wait()
	log.EndTimeL(labels, "Assets SEO metadata description ", startTime, nil)
	span.SetStatus(codes.Ok, "BuildForbesTokenMetadata Built")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

// Returns a random metadata description for the asset
func getRandomMetadataDescription(asset store.Fundamentals) string {
	str1 := []string{
		"View ",
		"Find ",
		"Learn what ",
		"Browse the latest ",
	}
	str2 := []string{
		") cryptocurrency prices and market charts. Stay informed on how much ",
		") cryptocurrency prices, market news, historical data, and financial information. Make informed investment decisions with ",
		") cryptocurrency is and today's market price. Confidently invest in cryptocurrency with current and historical ",
		") cryptocurrency news, research, and analysis. Stay informed on ",
	}
	str3 := []string{
		" is worth and evaluate current and historical price information.",
		" today.",
		" market data.",
		" prices within the cryptocurrency market.",
	}
	random := rand.Intn(4)

	//we want to add the asset name in between the descriptions and concatenate them.
	return str1[random] + asset.Name + " (" + strings.ToUpper(asset.DisplaySymbol) + str2[random] + asset.Name + str3[random]
}

// build fundamentals from coingecko Data
func BuildCategoriesFundamentals(ctx0 context.Context, labels map[string]string, bqs *store.BQStore, categoryMap map[string]store.CategoriesData, assets *[]store.Fundamentals, categoriesHistoricalList map[string]store.CategoryFundamental, chartCategoriesData24hrResults []store.TimeSeriesResultPG) error {

	ctx, span := tracer.Start(ctx0, "PGGetCategories")
	defer span.End()

	labels["subfunction"] = "BuildCategoriesFundamentals"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Categories Fundamentals Data")
	var allFundamentals []store.CategoryFundamental
	var chartData []store.TimeSeriesResultPG

	rowyCategories, err := store.GetFeaturedCategoriesMap(ctx0)
	if err != nil {
		log.ErrorL(labels, "Error BuildCategoriesFundamentals %s", err)
		span.SetStatus(codes.Error, "Category Fundamentals Building failed due to rowy category lookup")
	}

	//Build Newest category fundamentals
	newestAssets, err := store.GetNewestFundamentals(context.Background())
	if err != nil {
		fmt.Println("something went wrong")
	}

	newTokens := store.CategoriesData{ID: "new-cryptocurrencies", Name: "New Crypto Currencies", Inactive: false}

	for _, asset := range *newestAssets {
		newTokens.Markets = append(newTokens.Markets, store.CoinsMarketResult{
			ID:   asset.Symbol,
			Name: asset.Name,
		})
	}

	categoryMap["new-cryptocurrencies"] = newTokens

	for _, category := range categoryMap {
		categoryHistoricalData := categoriesHistoricalList[category.ID]
		categoryFundamental, cd := store.MapCategoryFundamental(ctx, category, assets, categoryHistoricalData, chartCategoriesData24hrResults)

		if cd.Symbol != "" {
			// build the chart for categories
			chartData = append(chartData, cd)
		}

		rowyCategory, exists := rowyCategories[categoryFundamental.ID]
		//Check to see if the category exists in rowy
		if exists {
			//if it does pull user input columns asnd assign them to the category fundamental
			categoryFundamental.ForbesID = rowyCategory.ForbesId
			categoryFundamental.ForbesName = rowyCategory.ForbesName
			categoryFundamental.IsHighlighted = rowyCategory.IsHighlighted
			//assign the rowy link column based on the id if there is a forbes id build a link using that instead ()
			rowyCategory.Link = fmt.Sprintf("/categories/%s/", categoryFundamental.ID)
			if rowyCategory.ForbesId != "" {
				rowyCategory.Link = fmt.Sprintf("/categories/%s/", categoryFundamental.ForbesID)
				categoryFundamental.Slug = rowyCategory.ForbesId
				rowyCategories[categoryFundamental.ID] = rowyCategory
			}
		} else if !categoryFundamental.Inactive {
			//if the category does not exist create a new entry in rowy. This way updates can be made by the seo team
			var newCat = store.FeaturedCategory{ID: categoryFundamental.ID, Name: categoryFundamental.Name, Link: fmt.Sprintf("/categories/%s/", categoryFundamental.ID)}
			rowyCategories[categoryFundamental.ID] = newCat
		}
		allFundamentals = append(allFundamentals, categoryFundamental)
	}
	errUpsert := store.UpsertCategoryFundamentalsPG(ctx, &allFundamentals, labels)
	if errUpsert != nil {
		log.ErrorL(labels, "Error UpsertCategoryFundamentalsPG %s", errUpsert)
		span.SetStatus(codes.Error, "Category Fundamentals Building failed due to insert in BQ failed")
	}
	store.UpdateFeaturedCategories(ctx, rowyCategories)
	// Insert the Charts data after we add the latest marketcap for each category
	err = store.InsertNomicsChartData(ctx, "24h", chartData)
	if err != nil {
		log.Error("%s", err)
	}

	errInsertBQ := bqs.InsertCategoryFundamentalsBQ(ctx, labels["UUID"], &allFundamentals)
	if errInsertBQ != nil {
		log.ErrorL(labels, "Error InsertCategoryFundamentalsBQ %s", errInsertBQ)
		span.SetStatus(codes.Error, "Category Fundamentals Building failed due to insert in BQ failed")
	}

	log.EndTimeL(labels, "Category Fundamentals Build ", startTime, nil)
	if errUpsert == nil && errInsertBQ == nil {
		span.SetStatus(codes.Ok, "Category Fundamentals Built")
	}
	return nil
}

// AcceptBuildFundamentalsRequest Invokes the BuildFundamentals Handler, and sends an accepted response.
func AcceptBuildFundamentalsRequest(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 0)
	go func(*http.Request) {
		ctx, _ := context.WithTimeout(context.Background(), (time.Duration(10) * time.Minute))

		BuildFundamentalsCGHandler(ctx, r)
	}(r)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Accepted"))
}

// build fundamentals from coingecko Data
func BuildFundamentalsCGHandler(ctx context.Context, r *http.Request) {

	vars := mux.Vars(r)
	period := vars["period"]
	labels := make(map[string]string)

	span := trace.SpanFromContext(ctx)
	defer span.End()

	labels["period"] = period
	labels["function"] = "BuildFundamentalsCGHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Fundamentals CG Data")

	// Go Routine with error group for error handling
	g, ctx1 := errgroup.WithContext(ctx)

	// Results from Go Routine 1
	// List of Tickers in Map of [Symbol]CoinsMarketData

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	tickers := make(map[string]store.CoinsMarketResult)
	// GO Routine 1
	// Get all Markets Data from CoinGecko
	g.Go(func() error {
		t, err := bqs.GetCoinsMarketData(ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting tickers from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received %d Markets from BQ", len(t))

		tickers = t

		return nil
	})

	var highLowResults []store.PGFundamentalsResult

	// Go Routine 2
	// Get HighLow Fundamentals for All Assets in BQ
	g.Go(func() error {
		results, err := bqs.BuildHighLowFundamentalsCG(ctx1, labels["UUID"])

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting High/Lows Fundamentals CG from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received Fundamentals CG %d results from BQ", len(results))

		highLowResults = results

		return nil

	})

	// Results from Go Routine 3
	// List of chartData open & close prices for all assets.
	var openClosePrices []store.OpenCloseAsset

	// Go Routine 3
	// Get all assets' open & close prices from the chart_data
	g.Go(func() error {

		d, err := store.GetOpenClosePrice(labels, ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting openClose from pg: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from GetOpenClosePrice", len(d))

		openClosePrices = d
		return nil

	})

	metaDataMap := make(map[string]store.AssetMetaData)
	// Go Routine 4
	// Get The Asset Metdata elements needed for the fundamentals
	g.Go(func() error {
		//esults, err := bqs.GetMarketCapNewAndOldValue(ctx, labels["UUID"])
		results, err := store.GetCoinGeckoMetaData(ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting High/Lows Fundamentals CG from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received Fundamentals CG %d results from BQ", len(results))

		metaDataMap = results
		fmt.Println(len(metaDataMap))

		return nil

	})

	// Go Routine 5
	// Get All 24 Hour Chart Data for all assets from BQ
	var chartData24hrResults []store.TimeSeriesResultPG

	g.Go(func() error {

		// TODO: Use Period from URL
		chartData24hrResults, err = bqs.QueryChartByInterval("24 hour", "900", labels["UUID"], store.Ft, ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting 24hr chart Data  " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges %d results from BQ", len(chartData24hrResults))

		return nil
	})

	// Go Routine 6
	// Get The Exchange Metadata elements needed for the fundamentals
	// And Get the list of Top Exchanges
	exchangesMetaData := make(map[string]model.CoingeckoExchangeMetadata)
	g.Go(func() error {
		results, err := store.GetExchangeMetaData(ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Metadata CG from PG: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Metadata CG %d results from PG", len(results))

		exchangesMetaData = results
		fmt.Println(len(exchangesMetaData))
		return nil

	})

	// Go Routine 7
	// Get The Exchanges Tickers needed for the fundamentals
	exchangeResults := make(map[string][]store.ExchangeBasedFundamentals)
	g.Go(func() error {
		results, err := bqs.ExchangeBasedFundamentalsCG(ctx1, labels["UUID"])
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Tickers Fundamentals CG from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Tickers Fundamentals CG %d results from BQ", len(results))

		exchangeResults = results
		fmt.Println(len(exchangeResults))

		return nil

	})

	// Results from Go Routine 8
	// List of exchangesProfiles in Map of [Name]ExchangeProfile
	exchangesProfiles := make(map[string]model.ExchangeProfile)

	// Go Routine 8
	// Get all Exchange profiles from FS (rowy tables)
	g.Go(func() error {

		e, err := store.GetExchanges(ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting exchanges from rowy: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from FS Exchanges", len(e))

		exchangesProfiles = e
		fmt.Println(len(exchangesProfiles))

		return nil
	})

	// Results from Go Routine 9
	// List of categories to generate their fundamentals
	var categoryMap = make(map[string]store.CategoriesData)

	// Go Routine 9
	// Get all categories list from PG
	g.Go(func() error {

		e, err := store.PGGetCategories(ctx1, true)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting list of categories from PG: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from PG Categories", len(e))

		categoryMap = e
		fmt.Println(len(categoryMap))

		return nil
	})

	// Results from Go Routine 10
	// List of categories historical Data to generate their fundamentals
	categoriesHistoricalList := make(map[string]store.CategoryFundamental)
	// Go Routine 10
	// Get all categories historical data from BQ
	g.Go(func() error {

		e, err := bqs.GetCategoriesHistoricalData(ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting list of categories historical from PG: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from BQ Categories Historical", len(e))

		categoriesHistoricalList = e
		fmt.Println(len(categoriesHistoricalList))

		return nil
	})

	// Go Routine 11
	// Get All 24 Hour Categories Chart Data from BQ
	var chartCategoriesData24hrResults []store.TimeSeriesResultPG

	g.Go(func() error {

		chartCategoriesData24hrResults, err = bqs.QueryChartByInterval("24 hour", "900", labels["UUID"], store.Category, ctx1)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting 24hr chart Data  " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges %d results from BQ", len(chartCategoriesData24hrResults))

		return nil
	})

	// Go Routine 12
	// Get All existing asset fundamentals. This will only populate the symbol and slug fields. All other fields will remain empty as they're not needed.
	// Purpose of this routine is to generate a mapping of slug => symbol, so that we don't produce duplicate slugs for newly added assets.
	var oldFundamentals *[]store.Fundamentals

	g.Go(func() error {

		oldFundamentals, err = store.GetAssetsSEOData(ctx1, false)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting old fundamentals Data  " + err.Error())
		}
		log.DebugL(labels, "Received old fundamentals %d results from PG", len(*oldFundamentals))

		return nil
	})

	span.AddEvent("Waiting for Go Routines to finish")
	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Starts new WaitGroup for the next set of go routines - which don't need to return an error
	var (
		wg sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 20)
		lock         sync.Mutex
		allChartData []store.TimeSeriesResultPG

		allFundamentals []store.Fundamentals
	)

	if backFillDateAdded {

		dateAdded, err := bqs.GetFundamentalsFirstOccuranceTime(context.Background(), labels["UUID"])

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, "Error Getting First Occurance Time: %s", err.Error())

		}
		log.DebugL(labels, "Received First Occurance Time CG %d results from BQ", len(dateAdded))

		backFillDateAdded = false
		store.UpsertFundamentalsDateAdded(ctx, dateAdded, labels)
	}

	allSlugToSymbolMap, oldFundamentalsMap := store.BuildSlugMaps(labels, ctx, oldFundamentals)

	for _, v := range highLowResults {
		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, v store.PGFundamentalsResult, uuid string, period string) {
			ctx, span := tracer.Start(ctxO, "Go Routine BuildFundamentalsPGHandler")
			defer span.End()

			label := make(map[string]string)
			label["symbol"] = v.Symbol
			span.SetAttributes(attribute.String("symbol", v.Symbol))
			label["period"] = period
			span.SetAttributes(attribute.String("period", period))
			label["uuid"] = uuid
			span.SetAttributes(attribute.String("uuid", uuid))
			label["spanID"] = span.SpanContext().SpanID().String()
			label["traceID"] = span.SpanContext().TraceID().String()

			// Function Call merges Ticker Data, High Low Data, and Prices by Exchange together into a single object
			// v.Symbol it is the ID but we need to keep it as it's for now because Nomics After we finish with Nomics we should change it to ID in all Struct that related to fundamentals

			var (
				chartData             []store.TimeSeriesResultPG
				cd                    store.TimeSeriesResultPG
				f                     store.Fundamentals
				exchange_fundamentals []store.ExchangeBasedFundamentals
				coinMarkets           store.CoinsMarketResult
				oldFundamentals       store.Fundamentals
				metadataMap           store.AssetMetaData
			)
			if val, ok := metaDataMap[v.Symbol]; ok {
				metadataMap = val
			}
			if oldFundamentalsMap.GetValue(v.Symbol) != nil {
				oldFundamentals = *oldFundamentalsMap.GetValue(v.Symbol)
			}
			if val, ok := tickers[v.Symbol]; ok {
				coinMarkets = val
			}
			if val, ok := exchangeResults[v.Symbol]; ok {
				exchange_fundamentals = val
			}

			f, err = store.CombineFundamentalsCG(ctx, coinMarkets, v, metadataMap, exchange_fundamentals, allSlugToSymbolMap, oldFundamentals)

			if err != nil {
				log.ErrorL(label, "Error combining fundamentals for %s: %s", v.Symbol, err.Error())
				goto waitReturn // If there is an error, skip to the end of the go routine
			}

			f.Exchanges, f.ForbesTransparencyVolume = store.CombineExchangeDataCG(ctx, f.Exchanges, exchangesMetaData, exchangesProfiles)

			f.Forbes = store.CalculateForbesBasedVolume(ctx, f.Exchanges)
			//Map AssetPrice,Date, and Last Updated based on the latest ticker from charts

			f, cd = store.MapChartDataToFundamental(ctx, chartData24hrResults, f, openClosePrices)
			chartData = append(chartData, cd)

			//insert the chart after the data is saved. this way price and chart match
			//Only do this if we have new chart data
			lock.Lock()
			if chartData != nil && len(chartData[0].Slice) > 0 {
				allChartData = append(allChartData, chartData...)
			}
			if f.Symbol != "" {
				allFundamentals = append(allFundamentals, f)
			}
			lock.Unlock()
			//store.InsertFundamentalLatest(ctx, f, label)

		waitReturn:
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()
		}(context.Background(), v, labels["UUID"], period)
	}

	wg.Wait()

	err = store.InsertNomicsChartData(ctx, "24h", allChartData)
	if err != nil {
		log.Error("%s", err)
	}
	BuildCategoriesFundamentals(ctx, labels, bqs, categoryMap, &allFundamentals, categoriesHistoricalList, chartCategoriesData24hrResults)
	store.UpsertFundamentalsLatest(ctx, allFundamentals, labels)
	store.RebuildCache(ctx, false)
	searchService.RebuildCaches(ctx) // pass a background context so if request terminates the goroutines inside still finish
	log.EndTimeL(labels, "Fundamental CG Build ", startTime, nil)
	span.SetStatus(codes.Ok, "FundamentalsCG Built")
}

func BuildEducation(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["function"] = "BuildEducation"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Education Data")

	result, err := services.BuildEducation(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	store.SaveEducationSection(r.Context(), result)

	log.EndTimeL(labels, "Build Education Data ", startTime, nil)
	span.SetStatus(codes.Ok, "Build Education Data")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

/*
Builds the NFT Fundamental Data,
updates the nft collections 24 hour charts
updates the nftdatalatesttable
*/
func BuildNFTFundamentalsCGHandler(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 0)
	vars := mux.Vars(r)
	period := vars["period"]
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["period"] = period
	labels["function"] = "BuildNFTFundamentalsCGHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build NFT Fundamentals CG Data")

	// Go Routine with error group for error handling
	g, ctx := errgroup.WithContext(r.Context())

	// Results from Go Routine 1
	// List of Tickers in Map of [Symbol]CoinsMarketData

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Go Routine 1
	// Get All 24 Hour Chart Data for all assets from BQ
	var chartData24hrResults []store.TimeSeriesResultPG

	g.Go(func() error {

		// TODO: Use Period from URL
		chartData24hrResults, err = bqs.QueryChartByInterval("24 hour", "900", labels["UUID"], store.Nft, ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting 24hr chart Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFT Charts Data %d results from BQ", len(chartData24hrResults))

		return nil
	})

	// This go function runs aQuery that calculates Sales Data for NFTs over a 90 day period
	var NFTSalesData []store.FundamentalsNFTSalesData
	g.Go(func() error {

		// TODO: Use Period from URL
		NFTSalesData, err = bqs.GetNFTSalesInfo(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting NFTs Sales Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFTs Sales %d results from BQ", len(NFTSalesData))

		return nil
	})

	// This go function runs aQuery that calculates Volume Data and percent calculations for NFTs over a 90 day period
	var NFTPctData map[string]store.FundamentalsNFTSalesData
	g.Go(func() error {

		// TODO: Use Period from URL
		NFTPctData, err = bqs.GetNFTVolumePctInfo(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting  NFTVolumePctChange Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFTs Volume Percent Change %d results from BQ", len(NFTPctData))

		return nil
	})

	// This go function runs aQuery that calculates Volume Data for NFTs over a 90 day period
	var NFTVolumeData map[string]store.FundamentalsNFTSalesData
	g.Go(func() error {

		// TODO: Use Period from URL
		NFTVolumeData, err = bqs.GetNFTVolumeInfo(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting  NFTVolume Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFTs Volume %d results from BQ", len(NFTVolumeData))

		return nil
	})
	// This go function runs aQuery that calculates Floor Price Data for NFTs over all intervals
	var NFTFloorPriceData map[string]store.FundamentalsNFTSalesData
	g.Go(func() error {

		// TODO: Use Period from URL
		NFTFloorPriceData, err = bqs.GetNFTFloorPriceInfo(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting NFTFloorPrice Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFTs Floor Price %d results from BQ", len(NFTFloorPriceData))

		return nil
	})

	// This go function runs aQuery that calculates Floor Price Data for NFTs over all intervals
	var NFTMetaData map[string]store.FundamentalsNFTSalesData
	g.Go(func() error {

		NFTMetaData, err = store.GetNFTMetaData(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting  NFTMetaData Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFTMetaData %d results from PG", len(NFTMetaData))

		return nil
	})

	var nftQuestionsTemplate []store.FSNFTQuestion
	g.Go(func() error {

		nftQuestionsTemplate, err = store.GetNFTsQuestionTemplates(context.Background())
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting NFTs Question Templates Data  " + err.Error())
		}
		log.DebugL(labels, "Received NFTs Question Templates %d results from FS", len(nftQuestionsTemplate))

		return nil
	})

	var coinsData map[string]string
	g.Go(func() error {

		coinsData, err = store.GetCoinsData(context.Background())
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Coins Data Data " + err.Error())
		}
		log.DebugL(labels, "Received Coins Data %d results from PG", len(coinsData))

		return nil
	})

	span.AddEvent("Waiting for Go Routines to finish")
	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Starts new WaitGroup for the next set of go routines - which don't need to return an error
	var (
		wg sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 20)
	)

	for _, v := range chartData24hrResults {
		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, v store.TimeSeriesResultPG, uuid string, period string) {
			ctx, span := tracer.Start(ctxO, "Go Routine BuildNFTFundamentalsCGHandler")
			defer span.End()

			label := make(map[string]string)
			label["symbol"] = v.Symbol
			span.SetAttributes(attribute.String("symbol", v.Symbol))
			label["period"] = period
			span.SetAttributes(attribute.String("period", period))
			label["uuid"] = uuid
			span.SetAttributes(attribute.String("uuid", uuid))
			label["spanID"] = span.SpanContext().SpanID().String()
			label["traceID"] = span.SpanContext().TraceID().String()

			var chartData []store.TimeSeriesResultPG
			chartData = append(chartData, v)

			if err != nil {
				log.ErrorL(label, "Error combining NFTs fundamentals for %s: %s", v.Symbol, err.Error())
				goto waitReturn // If there is an error, skip to the end of the go routine
			}

			//insert the chart after the data is saved. this way price and chart match
			//Only do this if we have new chart data
			if len(chartData) > 0 && len(chartData[0].Slice) > 0 {
				err = store.InsertNomicsChartData(ctx, period, chartData)
			}

		waitReturn:
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()
		}(r.Context(), v, labels["UUID"], period)
	}

	NFTSalesData = store.MapNFTFundamentalsData(ctx, NFTSalesData, NFTVolumeData, NFTFloorPriceData, NFTPctData, NFTMetaData, nftQuestionsTemplate, coinsData)

	wg.Wait()
	store.UpsertNFTSalesData(context.Background(), &NFTSalesData)
	log.EndTimeL(labels, "NFT Fundamental CG Build ", startTime, nil)
	nftService.BuildCache(context.Background())
	microservices.ExpirationHeaders.AddValue(app.NFT_path, time.Now().Add(10*time.Minute))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
	span.SetStatus(codes.Ok, "NFT FundamentalsCG Built")

}

// Searches the traded assets table with the provided query and paginations.
func GetSearchAssets(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "GetSearchAssets"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "Search Assets")

	setResponseHeaders(w, 30)    // Data is fetched from fundamentalsLatest table. This table is updated every 5 minutes.
	paginate := store.Paginate{} //captures the pagination params.
	limit := html.EscapeString(r.URL.Query().Get("limit"))
	query := html.EscapeString(r.URL.Query().Get("query"))
	pageNum := html.EscapeString(r.URL.Query().Get("pageNum"))
	paginate.SortBy = html.EscapeString(r.URL.Query().Get("sortBy"))
	paginate.Direction = html.EscapeString(r.URL.Query().Get("direction"))
	category := html.EscapeString(r.URL.Query().Get("category"))
	// Will use categoryID if we need to search about specific FT using Tags
	categoryID := html.EscapeString(r.URL.Query().Get("categoryId"))
	var limitError error
	var pageError error
	paginate.Limit, limitError = strconv.Atoi(limit)
	paginate.PageNum, pageError = strconv.Atoi(pageNum)
	dictionaryCategory, dictionaryErr := store.GetDictionaryCategoryByString(r.Context(), category)

	if limitError != nil || pageError != nil || dictionaryErr != nil { //throw an error if pagination args are improper.
		log.ErrorL(labels, "Invalid pagination values")
		span.SetStatus(codes.Error, "Invalid pagination values")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var result []byte
	var err error

	if categoryID != "" {
		// If categoryID exists in query params, It will be used to searching for a specific category using FT query.
		// this means the user needs to search for assets using a specific category
		paginate.CategoryID = categoryID
		// The SearchTermByCategory function will build the result using the Markets that exist in the specified category
		result, err = store.SearchTermByCategory(r.Context(), query, dictionaryCategory, paginate)
	} else {
		result, err = store.SearchTerm(r.Context(), query, dictionaryCategory, paginate)
	}

	if err != nil || result == nil {
		if err != nil {
			log.ErrorL(labels, "%s", err)
			span.SetStatus(codes.Error, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			span.SetStatus(codes.Error, "No Data Found")
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}

	log.EndTimeL(labels, "Search Assets", startTime, nil)
	span.SetStatus(codes.Ok, "Search Assets")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}

// Get list of All Categories
func GetCategories(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 30)
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())

	defer span.End()
	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetCategories"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Categories Data")

	result, err := store.PGGetCategories(r.Context(), true)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	jsonData, err := json.Marshal(result)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "Get Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Get Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(jsonData))
}

// Will fetch  Featured Categories from FS
func GetFeaturedCategories(w http.ResponseWriter, r *http.Request) {
	// update each 30 sec
	setResponseHeaders(w, 30)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetFeaturedCategories"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Featured Categories")

	// Will returns the ID and name for all Featured Categories
	result, err := store.GetFeaturedCategories(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	log.EndTimeL(labels, "Featured Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Featured Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}

func GetListsSection(w http.ResponseWriter, r *http.Request) {
	// update each 30 sec
	setResponseHeaders(w, 30)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetListsSection"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Lists Section")

	// Will returns the global description and the lists of crypto section
	result, err := store.GetListsSection(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	log.EndTimeL(labels, "Lists Section Data", startTime, nil)
	span.SetStatus(codes.Ok, "Lists Section Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)
}

func GetVideos(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetVideos")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetVideos"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Assets Calculator Data")

	data, err := services.GetVideosList(ctx)
	if data == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)

	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		span.SetStatus(codes.Ok, "GetVideos")
		log.EndTime("GetVideos", startTime, nil)
		w.Write(*data)
	}
}

// Builds video content
func BuildVideos(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "BuildVideos")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildVideos"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Assets Calculator Data")

	bqs, err := store.NewBQStore()
	if err != nil {
		log.ErrorL(labels, "%s", err)

		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	res, err := bqs.GetBrightCoveVideos(ctx, "")
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	services.BuildVideosList(ctx, res)

	log.EndTimeL(labels, "BuildVideos ", startTime, nil)
	span.SetStatus(codes.Ok, "BuildVideos")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

func GetTweets(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetTweets")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetTweets"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("GetTweets")

	data, err := store.GetRecentTweets(ctx)
	if data == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)

	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		span.SetStatus(codes.Ok, "GetTweets")
		log.EndTime("GetTweets", startTime, nil)
		w.Write(data)
	}
}

// Get NFT Prices Data
// Searches the NFT table with the provided query and pagination.
func GetNFTPrices(w http.ResponseWriter, r *http.Request) {
	if expirationHeaders.DoesKeyExist(app.NFT_path) {
		common.SetExpirationResponseHeaders(w, *microservices.ExpirationHeaders.GetValue(app.NFT_path))
	} // updated each 5 minute
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetNFTPrices")
	defer span.End()

	labels["function"] = "GetNFTPrices"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "NFT Price Table")
	paginate := store.Paginate{} //captures the pagination params.
	limit := html.EscapeString(r.URL.Query().Get("limit"))
	pageNum := html.EscapeString(r.URL.Query().Get("pageNum"))
	paginate.SortBy = html.EscapeString(r.URL.Query().Get("sortBy"))
	paginate.Direction = html.EscapeString(r.URL.Query().Get("direction"))
	category := html.EscapeString(r.URL.Query().Get("category"))
	// We can use if we need to search for specific NFT
	query := html.EscapeString(r.URL.Query().Get("query"))
	// Will use chainID if we need to search about specific NFT using Chains
	chainID := html.EscapeString(r.URL.Query().Get("chain_id"))
	var limitError error
	var pageError error
	paginate.Limit, limitError = strconv.Atoi(limit)
	paginate.PageNum, pageError = strconv.Atoi(pageNum)
	dictionaryCategory, dictionaryErr := store.GetDictionaryCategoryByString(ctx, category)

	if limitError != nil || pageError != nil || dictionaryErr != nil { //throw an error if pagination args are improper.
		log.ErrorL(labels, "Invalid pagination values")
		span.SetStatus(codes.Error, "Invalid pagination values")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var result []byte
	var err error

	if chainID != "" {
		// If chainID exists in query params, It will be used to searching for a specific chain using NFT query.
		// this means the user needs to search for nfts using a specific chain
		paginate.ChainID = chainID
		// The SearchTermByChains function will build the result using the NFTs that exist in the specified chain
		result, err = store.SearchTermByChains(ctx, query, dictionaryCategory, paginate)
	} else {
		// The SearchNFTTerm function will build the result using All NFTs
		result, err = store.SearchNFTTerm(ctx, query, dictionaryCategory, paginate)
	}

	if result == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	reader := bytes.NewReader(result)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	log.EndTime("Get NFT Prices Data", startTime, nil)
	span.SetStatus(codes.Ok, "GetNFTPrices")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

// we will fetch all articles that related to each topic from BQ and Inserted it to FS
func BuildTopics(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["function"] = "BuildTopics"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Topics Data")

	// get all topics data from BQ
	result, err := services.BuildTopics(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// save topics data to FS
	store.SaveNewsTopics(r.Context(), result)

	log.EndTimeL(labels, "Build Topics Data ", startTime, nil)
	span.SetStatus(codes.Ok, "Build Topics Data")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

// Change the trending topic for the day
func BuildTrendingTopics(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["function"] = "BuildTrendingTopics"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Trending Topics Data")

	// get all tending and not trending topics from FS
	topics, oldTopics := services.UpdateTrendingTopics(r.Context())

	// Update the trending topic for 24 hours
	store.UpdateIsTrendingTopics(r.Context(), topics, oldTopics)

	log.EndTimeL(labels, "Build Trending Topics Data ", startTime, nil)
	span.SetStatus(codes.Ok, "Build Trending Topics Data")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

/*
Takes an articleID,CollectionName,and DocumentID

 1. Searches for content relating to article id

 2. updates the document, at the target collection
    with the contents fo the article, and authors

    This function will be called from a rowy action column name "updateContent".
    That column will contain a small amount of logic to call this endpoint.
*/
func UpdateFeaturedAndPromotedContent(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 1)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "UpdateContent")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "UpdateContent"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("UpdateContent")

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var reqBody model.UpdateContentRequest
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	bqs, err := store.NewBQStore()
	if err != nil {
		log.ErrorL(labels, "%s", err)

		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	details, err := bqs.GetArticleContent(ctx, reqBody.ArticleID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	store.SaveFeaturedArticle(ctx, *details, reqBody.Collection, reqBody.Document)
	log.EndTimeL(labels, "UpdateContent ", startTime, nil)
	span.SetStatus(codes.Ok, "UpdateContent")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

// we will fetch all articles that related to each topic from Data Product API and Inserted it to FS
// We will remove this Endpoint after we can get the articles data from BQ
func BuildTopicsFromDS(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["function"] = "BuildTopicsFromDS"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Topics Data")

	// get all topics data from BQ
	result, err := services.BuildNewsTopics(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// save topics data to FS
	store.SaveNewsTopics(r.Context(), result)

	log.EndTimeL(labels, "Build Topics Data ", startTime, nil)
	span.SetStatus(codes.Ok, "Build Topics Data")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

/*
	Responsible for checking what scopes a user has in regards to web3 features,
	and registering them for scopes as well.
	scopes - Scopes reperesent the type of access the user has.
		0 - FDAUser : The user is considered an fda user.
		1 - FDA_BETA_User : The User is part of the Beta program and will recieve special perks.
	action - The action that will be take on the provided scope.
		0 - Verify User Has ALL scopes within the list. If any return false the response will return false.
		1 - Register the user for all scopes in list. If for any reason a scope could not be registered return false.
	zephrSessionID - The session Id of the user
*/

func HandleExtendedProfileRequest(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "HandleExtendedProfileRequest"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("Start HandleExtendedProfileRequest")
	startTime := log.StartTimeL(labels, "HandleExtendedProfileRequest")

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var reqBody model.Web3_ExtendedProfile_Request
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp, err := services.ProcessWeb3ExtendedProfileRequest(r.Context(), reqBody)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTime("HandleExtendedProfileRequest", startTime, nil)

	span.SetStatus(codes.Ok, "request completed successfully")

	respBody, err := json.Marshal(resp)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(200)
	w.Write(respBody)
}

// Build categories With related News Topics
func BuildTopicsCategories(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "BuildTopicsCategories")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildTopicsCategories"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Build News Topics Categories Data")

	result, err := services.BuildNewsTopicsCategories(ctx)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	store.SaveNewsTopicsCategories(ctx, result)

	log.EndTimeL(labels, "Build News Topics Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Build News Topics Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))

}

// Build Premium articles from ForbesAPI each 5 min
func BuildPremiumArticles(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "BuildPremiumArticles")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildPremiumArticles"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Build Premium Articles")

	// Will get the Premium articles from ForbesAPI After we map it to our Articles struct
	result, err := services.BuildCommunityPageRecommendedArticles(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Store Premium articles into Rowy Table
	store.SaveRecommendedPremiumArticles(ctx, result)

	log.EndTimeL(labels, "Build Premium Articles", startTime, nil)
	span.SetStatus(codes.Ok, "Build Premium Articles")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func BuildCategoriesHistoricalData(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 100)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "BuildCategoriesHistoricalData")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildCategoriesHistoricalData"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "BuildCategoriesHistoricalData")

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "BuildCategoriesHistoricalData: Error connecting BigQuery %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	g, ctx := errgroup.WithContext(r.Context())
	var categoriesList []store.Categories
	g.Go(func() error {
		c, err := store.GetCategories(ctx)
		if err != nil {
			log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Getting Categories Data from PG %s", err)
			span.SetStatus(codes.Error, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
		categoriesList = c
		return nil
	})
	var assetsMetaData map[string]store.AssetMetaData
	g.Go(func() error {
		a, err := store.GetCoinGeckoMetaData(ctx)
		if err != nil {
			log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Getting Assets Metadata from PG %s", err)
			span.SetStatus(codes.Error, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
		assetsMetaData = a
		return nil
	})
	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "BuildCategoriesHistoricalData: in go routines  %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}
	var (
		wg           sync.WaitGroup
		throttleChan = make(chan bool, 20)
		lock         sync.Mutex
		qErr         error
	)
	for index, category := range categoriesList {
		throttleChan <- true
		wg.Add(1)
		go func(category store.Categories, index int) {
			log.Debug("BuildCategoriesHistoricalData: start build historical data for %d ->>>:  %s", index, category.ID)
			categoriesPrices, err := bqs.BuildCategoriesHistoricalData(context.Background(), category, assetsMetaData)
			if err != nil {
				log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Building Categories historical Data from BQ %s", err)
				span.SetStatus(codes.Error, err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			lock.Lock()
			if categoriesPrices != nil {
				log.Debug("BuildCategoriesHistoricalData: start inserting historical data for %d ->>>:  %s", index, category.ID)
				bqs.InsertCategoryFundamentalsBQ(context.Background(), labels["UUID"], &categoriesPrices)
			}
			lock.Unlock()
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()

		}(category, index)

	}

	wg.Wait()
	if qErr != nil {
		log.Error("%s", qErr)
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTimeL(labels, "BuildCategoriesHistoricalData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
