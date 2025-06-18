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
	watchListService          services.WatchListService          // Provides all services that power a watchlist
	communityPageService      services.CommunityPageService      // Provides all services that power the Community Page
	portfolioService          services.PortfolioService          // Provides all functionality to power the portfolio page
	landingPageService        services.LandingPageService        // Provides all functionality to power the Landing page
	cryptoPriceService        services.CryptoPriceService        // Provides all functionality to power the Crypto Price page
	educationService          services.EducationService          // Provides all functionality to power the Education page
	videoService              services.VideoService              // Provides all functionality to power the Video Block on DA Dashboard
	searchService             services.SearchService             // Provides all functionality for FDA Search
	nftService                services.NftService                // Provides all functionality for NFT functionalities
	chartService              services.ChartService              // Provides all functionality for FDA Chart
	newsFeedService           services.NewsFeedService           // Provides all functionality for FDA NewsFeed
	editorsPickService        services.EditorsPickService        // Provides all functionality for FDA EditorsPick
	topicsService             services.TopicsService             // Provides all functionality for FDA Topics
	carouselService           services.CarouselService           // Provides all functionality for FDA Carousel
	dynamicDescriptionService services.DynamicDescriptionService // Provides all functionality for FDA Dynamic Description
	researchService           services.ResearchService           // Provides all functionality for FDA Research
	eventsService             services.EventsService             // Provides all functionality for FDA Events
	profilesService           services.ProfilesService           // Provides all functionality for FDA All Profiles
	twitterService            services.TwitterServices
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
	nftService services.NftService,
	chartService services.ChartService,
	newsFeedService services.NewsFeedService,
	editorsPickService services.EditorsPickService,
	topicsService services.TopicsService,
	carouselService services.CarouselService,
	dynamicDescriptionService services.DynamicDescriptionService,
	researchService services.ResearchService,
	eventsService services.EventsService,
	profilesService services.ProfilesService,
	twitterService services.TwitterServices,
) (*Microservices, error) {

	ms := Microservices{
		watchListService:          watchListService,
		communityPageService:      communityPageService,
		portfolioService:          portfolioService,
		landingPageService:        landingPageService,
		cryptoPriceService:        cryptoPriceService,
		educationService:          educationService,
		videoService:              videoService,
		searchService:             searchService,
		nftService:                nftService,
		chartService:              chartService,
		newsFeedService:           newsFeedService,
		editorsPickService:        editorsPickService,
		topicsService:             topicsService,
		carouselService:           carouselService,
		dynamicDescriptionService: dynamicDescriptionService,
		researchService:           researchService,
		eventsService:             eventsService,
		profilesService:           profilesService,
		twitterService:            twitterService,
	}

	return &ms, nil

}
