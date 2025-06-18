//The App Package contains all of the controller / Delivery Logic for the application.
// Each function will utilize functions from various services.

package app

import (
	"github.com/Forbes-Media/forbes-digital-assets/refactored/services"
)

/*
Microservices is responsible for an entry point to all available services
*/
type Microservices struct {
	watchListService     services.WatchListService     // Provides all services that power a watchlist
	communityPageService services.CommunityPageService // Provides all services that power the Community Page
	portfolioService     services.PortfolioService     // Provides all functionality to power the portfolio page
	landingPageService   services.LandingPageService   // Provides all functionality to power the Landing page
	cryptoPriceService   services.CryptoPriceService   // Provides all functionality to power the Crypto Price page
	educationService     services.EducationService     // Provides all functionality to power the Education page
	videoService         services.VideoService         // Provides all functionality to power the Video Block on DA Dashboard
	searchService        services.SearchService        // Provides all functionality for FDA Search
	chartService         services.ChartService         // Provides all functionality for FDA Chart
	topicsService        services.TopicsService        // Provides all functionality for FDA Chart
}

// Instantiates a new microservice objet, which currently only takes one microservice
// takes a watchlistService and returns a new microservice object.
// Add more services here
func NewMicroservices(
	watchListService services.WatchListService,
	communityPageService services.CommunityPageService,
	portfolioService services.PortfolioService,
	landingPageService services.LandingPageService,
	cryptoPriceService services.CryptoPriceService,
	educationService services.EducationService,
	videoService services.VideoService,
	searchService services.SearchService,
	chartService services.ChartService,
	topicsService services.TopicsService,
) (*Microservices, error) {

	ms := Microservices{
		watchListService:     watchListService,
		communityPageService: communityPageService,
		portfolioService:     portfolioService,
		landingPageService:   landingPageService,
		cryptoPriceService:   cryptoPriceService,
		educationService:     educationService,
		videoService:         videoService,
		searchService:        searchService,
		chartService:         chartService,
		topicsService:        topicsService,
	}

	return &ms, nil

}
