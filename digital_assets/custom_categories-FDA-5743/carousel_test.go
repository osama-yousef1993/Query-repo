package app_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/app"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/services"
	"github.com/stretchr/testify/assert"
)

var (
	microservices *app.Microservices
	err           error
)

func TestCarousel_Get(t *testing.T) {

	t.Helper()
	if os.Getenv("RUN_E2E_TESTS") == "false" || os.Getenv("RUN_E2E_TESTS") == "" {
		t.Skip()
	}
	var carouselData *datastruct.TradedAssetsResp

	if microservices == nil {
		microservices, err = generateMicroService()
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest("", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create a mock response recorder
	rr := httptest.NewRecorder()
	assert := assert.New(t)

	microservices.GetCarouselData(rr, req)

	var validStatus = http.StatusOK
	assert.Equal(validStatus, rr.Code)

	responseBody, err := ioutil.ReadAll(rr.Body)
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(responseBody, &carouselData)
	assert.Equal(10, len(carouselData.Assets))
	assert.Less(0, carouselData.Total)

	for _, asset := range carouselData.Assets {

		assert.IsType("", asset.Symbol)
		assert.IsType("", asset.Name)
		assert.IsType("", asset.Logo)
		assert.IsType("", asset.DisplaySymbol)
		assert.IsType(float64(0), *asset.Price)
		assert.IsType(float64(0), *asset.MarketCap)
		assert.IsType(float64(0), *asset.Percentage)
		assert.IsType(float64(0), *asset.Percentage1H)
		assert.IsType(float64(0), *asset.Percentage7D)
		assert.IsType(float64(0), *asset.ChangeValue)
	}

}

func generateMicroService() (*app.Microservices, error) {

	db := repository.NewDao()
	expirationHeaders := common.NewConcurrentMap[app.EndpointPath, time.Time]()
	watchlistService := services.NewWatchlistService(db)
	communityPageService := services.NewCommunityPageService(db)
	portfolioService := services.NewPortfolioService(db)
	landingPageService := services.NewLandingPageService(db)
	cryptoPriceService := services.NewCryptoPriceService(db)
	exchangeService := services.NewExchangeService(db)
	educationService := services.NewEducationService(db)
	videoService := services.NewVideoService(db)
	searchService := services.NewSearchService(db)
	nftService := services.NewNftService(db)
	chartService := services.NewChartServices(db)
	newsFeedService := services.NewNewsFeedService(db)
	editorsPickService := services.NewEditorsPickService(db)
	topicsService := services.NewTopicsService(db)
	carouselService := services.NewCarouselService(db)
	dynamicDescriptionService := services.NewDynamicDescriptionService(db)
	profilesService := services.NewProfilesService(db)
	eventsService := services.NewEventsService(db)
	researchService := services.NewResearchService(db)
	customCategoryService := services.NewCustomCategoryService(db)


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
		return nil, err
	}
	return microservices, nil
}
