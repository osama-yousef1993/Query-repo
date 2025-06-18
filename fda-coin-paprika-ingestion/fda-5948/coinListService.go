package services

import (
	"context"
	"fmt"
	"slices"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common"
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/datastruct"
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/repository"
	"github.com/coinpaprika/coinpaprika-api-go-client/coinpaprika"

	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// CoinListService defines the interface for services that manage coin lists and metadata.
type CoinListService interface {
	// BuildCoinList fetches the list of coins and inserts it into the database.
	BuildCoinList(ctx context.Context) error
	// BuildCoinMetaDataList fetches coin metadata based on coin IDs and inserts it into the database.
	BuildCoinMetaDataList(ctx context.Context) error

	BuildCoinsHistoricalOHLCVData(ctx context.Context) error
}

// coinListService is the implementation of CoinListService, using a DAO for database operations.
type coinListService struct {
	dao repository.DAO // DAO provides methods for interacting with the database
}

// NewCoinListService creates and returns a new instance of coinListService.
func NewCoinListService(dao repository.DAO) CoinListService {
	return &coinListService{dao: dao}
}

// BuildCoinList retrieves a list of coins and stores it in the database.
// It performs the following steps:
//   - Retrieves the coin list from the CoinPaprika using the DAO's query manager.
//   - Inserts the coin list into the database.
//   - Handles errors at each stage and sets appropriate status codes in the trace.
//
// Parameters:
//   - ctx: The context to manage the lifecycle of the request, including cancellation and timeouts.
//
// Returns:
//   - An error, if any occurs during the coin list retrieval or insertion. Returns nil if successful.
func (c *coinListService) BuildCoinList(ctx context.Context) error {
	// Start tracing for the BuildCoinList operation
	span, labels := common.GenerateSpan("BuildCoinList", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildCoinList"))

	// Log the start time of the operation
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildCoinList"))

	// Create a new query manager for coin list operations
	queryMGR := c.dao.NewCoinListQuery()

	// Fetch the list of coins from the coinpaprika API
	coins, err := queryMGR.GetCoinList(ctx)
	if err != nil {
		// If there’s an error fetching the coin list, log the error and return
		span.SetStatus(codes.Error, "BuildCoinList: Error Getting Coins")
		log.EndTimeL(labels, "Error Getting Coins", startTime, nil)
		return err
	}

	// Insert the fetched coin list into the database
	err = queryMGR.UpsertCoinList(ctx, coins)
	if err != nil {
		// If there’s an error inserting the coins into the database, log the error and return
		span.SetStatus(codes.Error, "BuildCoinList: Error Inserted Coins to PG")
		log.EndTimeL(labels, "BuildCoinList: Error Inserted Coins to PG", startTime, nil)
		return err
	}

	// Log success and end the trace with an OK status
	span.SetStatus(codes.Ok, "BuildCoinList: Build Coins List to PG finished")
	log.EndTimeL(labels, "BuildCoinList: Build Coins List to PG finished", startTime, nil)
	return nil
}

// BuildCoinMetaDataList retrieves coin metadata for a list of coin IDs and stores it in the database.
// It performs the following steps:
//   - Retrieves the coin IDs from the PG database.
//   - Retrieves metadata for each coin using the fetched coin IDs from CoinPaprika.
//   - Inserts the fetched metadata into the database.
//
// Parameters:
//   - ctx: The context to manage the lifecycle of the request, including cancellation and timeouts.
//
// Returns:
//   - An error, if any occurs during the coin metadata retrieval or insertion. Returns nil if successful.
func (c *coinListService) BuildCoinMetaDataList(ctx context.Context) error {
	// Start tracing for the BuildCoinMetaDataList operation
	span, labels := common.GenerateSpan("BuildCoinMetaDataList", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildCoinMetaDataList"))

	// Log the start time of the operation
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildCoinMetaDataList"))

	// Create a new query manager for coin list operations
	queryMGR := c.dao.NewCoinListQuery()

	// Fetch the list of coin IDs from the database
	coins, err := queryMGR.GetCoinsIDFromPG(ctx)
	if err != nil {
		// If there’s an error fetching the coin IDs, log the error and return
		span.SetStatus(codes.Error, "BuildCoinMetaDataList: Error Getting Coins")
		log.EndTimeL(labels, "BuildCoinMetaDataList: Error Getting Coins", startTime, err)
		return err
	}

	// Fetch the metadata for each coin using the list of coin IDs
	coinsMetaData, coinMap, err := queryMGR.GetCoinDataByID(ctx, coins)
	if err != nil {
		// If there’s an error fetching the metadata, log the error and return
		span.SetStatus(codes.Error, "BuildCoinMetaDataList: Error Getting Coins MetaData to PG")
		log.EndTimeL(labels, "BuildCoinMetaDataList: Error Getting Coins MetaData to PG", startTime, err)
		return err
	}
	// Map the data retrieved from CoinPaprika to the internal `datastruct.Coin` format.
	coinsData := MapCoinPaprikaCoins(coinsMetaData)

	// Insert the fetched coin metadata into the database
	err = queryMGR.UpsertCoinsMetaData(ctx, coinsData, coinMap)
	if err != nil {
		// If there’s an error inserting the metadata into the database, log the error and return
		span.SetStatus(codes.Error, "BuildCoinMetaDataList: Error Inserted Coins MetaData to PG")
		log.EndTimeL(labels, "BuildCoinMetaDataList: Error Inserted Coins MetaData to PG", startTime, err)
		return err
	}

	// Log success and end the trace with an OK status
	span.SetStatus(codes.Ok, "BuildCoinMetaDataList: Build Coins List to PG finished")
	log.EndTimeL(labels, "BuildCoinMetaDataList: Build Coins List to PG finished", startTime, nil)
	return nil
}

// MapCoinPaprikaCoins maps a list of CoinPaprika coin objects to the internal `datastruct.Coin` format.
// Parameters:
//   - paprikaCoins: A slice of pointers to `coinpaprika.Coin` objects.
//
// Returns:
//   - A slice of `datastruct.Coin` objects.
func MapCoinPaprikaCoins(paprikaCoins []*coinpaprika.Coin) []datastruct.Coin {
	var coins []datastruct.Coin

	for _, paprikaCoin := range paprikaCoins {
		// Initialize an empty Coin struct for mapping.
		var coin datastruct.Coin

		// Map simple fields.
		coin.ID = paprikaCoin.ID
		coin.Name = paprikaCoin.Name
		coin.Symbol = paprikaCoin.Symbol
		coin.Rank = paprikaCoin.Rank
		coin.IsNew = paprikaCoin.IsNew
		coin.IsActive = paprikaCoin.IsActive
		coin.Description = paprikaCoin.Description
		coin.Message = paprikaCoin.Message
		coin.OpenSource = paprikaCoin.OpenSource
		coin.HardwareWallet = paprikaCoin.HardwareWallet
		coin.DevelopmentStatus = paprikaCoin.DevelopmentStatus
		coin.ProofType = paprikaCoin.ProofType
		coin.OrgStructure = paprikaCoin.OrgStructure
		coin.StartedAt = paprikaCoin.StartedAt
		coin.HashAlgorithm = paprikaCoin.HashAlgorithm

		// Map whitepaper details if available.
		if paprikaCoin.Whitepaper != nil {
			if paprikaCoin.Whitepaper.Link != nil {
				coin.Whitepaper.Link = *paprikaCoin.Whitepaper.Link
			}
			if paprikaCoin.Whitepaper.Thumbnail != nil {
				coin.Whitepaper.Thumbnail = *paprikaCoin.Whitepaper.Thumbnail
			}
		}

		// Map parent details if available.
		if paprikaCoin.Parent != nil {
			if paprikaCoin.Parent.ID != nil {
				coin.Parent.ID = *paprikaCoin.Parent.ID
			}
			if paprikaCoin.Parent.Name != nil {
				coin.Parent.Name = *paprikaCoin.Parent.Name
			}
			if paprikaCoin.Parent.Symbol != nil {
				coin.Parent.Symbol = *paprikaCoin.Parent.Symbol
			}
		}

		// Map links and tags using helper functions.
		coin.Links = MapCoinsLinksExtended(paprikaCoin.LinksExtended, MapCoinsLinks(paprikaCoin.Links))
		coin.Tags = MapCoinTags(paprikaCoin.Tags)
		coin.Team = MapCoinTeam(paprikaCoin.Team)

		// Append the mapped coin to the result slice.
		coins = append(coins, coin)
	}

	return coins
}

// MapCoinsLinks maps simple links from a map of strings to the `datastruct.Links` struct.
// Parameters:
//   - links: A map with keys representing link types and values being slices of strings (URLs).
//
// Returns:
//   - A `datastruct.Links` object populated with the corresponding links.
func MapCoinsLinks(links map[string][]string) datastruct.Links {
	var coinLinks datastruct.Links

	// Iterate through the map and assign values to the appropriate fields.
	for key, link := range links {
		if len(link) == 0 {
			continue // Skip empty slices.
		}
		switch key {
		case "explorer":
			coinLinks.Explorer = link
		case "facebook":
			coinLinks.Facebook = link[0]
		case "reddit":
			coinLinks.Reddit = link[0]
		case "source_code":
			coinLinks.SourceCode = link[0]
		case "website":
			coinLinks.Website = link[0]
		case "youtube":
			coinLinks.Youtube = link[0]
		case "medium":
			coinLinks.Medium = link[0]
		}
	}

	return coinLinks
}

// MapCoinsLinksExtended maps extended links from CoinPaprika into the `datastruct.Links` struct.
// Parameters:
//   - linksExtended: A slice of `coinpaprika.CoinLink` objects representing extended link types and URLs.
//   - links: An existing `datastruct.Links` object to append or update additional links.
//
// Returns:
//   - The updated `datastruct.Links` object with extended link information.
func MapCoinsLinksExtended(linksExtended []coinpaprika.CoinLink, links datastruct.Links) datastruct.Links {
	// Iterate through the extended links and map them to the appropriate fields.
	for _, linkExtended := range linksExtended {
		if linkExtended.Type == nil || linkExtended.URL == nil {
			continue // Skip if the type or URL is nil.
		}

		switch *linkExtended.Type {
		case "announcement":
			links.Announcement = *linkExtended.URL
		case "telegram":
			links.Telegram = *linkExtended.URL
		case "twitter":
			links.Twitter = *linkExtended.URL
		case "message_board":
			links.MessageBoard = *linkExtended.URL
		case "wallet":
			links.Wallet = *linkExtended.URL
		case "blog":
			links.Blog = *linkExtended.URL
		case "chat":
			links.Chat = *linkExtended.URL
		case "slack":
			links.Slack = *linkExtended.URL
		case "discord":
			links.Discord = *linkExtended.URL
		case "explorer":
			// Avoid duplicates in the Explorer list.
			if !slices.Contains(links.Explorer, *linkExtended.URL) {
				links.Explorer = append(links.Explorer, *linkExtended.URL)
			}
		case "facebook":
			// Only assign if the Facebook field is not already populated.
			if links.Facebook == "" {
				links.Facebook = *linkExtended.URL
			}
		case "reddit":
			if links.Reddit == "" {
				links.Reddit = *linkExtended.URL
			}
		case "source_code":
			if links.SourceCode == "" {
				links.SourceCode = *linkExtended.URL
			}
		case "website":
			if links.Website == "" {
				links.Website = *linkExtended.URL
			}
		case "youtube":
			if links.Youtube == "" {
				links.Youtube = *linkExtended.URL
			}
		case "medium":
			if links.Medium == "" {
				links.Medium = *linkExtended.URL
			}
		}
	}

	return links
}

// MapCoinTags maps CoinPaprika tags into the `datastruct.Tag` structure.
// Parameters:
//   - paprikaTags: A slice of `coinpaprika.Tag` objects to be converted.
//
// Returns:
//   - A slice of `datastruct.Tag` objects.
func MapCoinTags(paprikaTags []coinpaprika.Tag) []datastruct.Tag {
	var tags []datastruct.Tag

	// Iterate through the CoinPaprika tags and map their fields.
	for _, paprikaTag := range paprikaTags {
		// Ensure no nil pointers are dereferenced.
		if paprikaTag.ID == nil || paprikaTag.Name == nil || paprikaTag.CoinCounter == nil || paprikaTag.ICOCounter == nil {
			continue // Skip invalid entries.
		}

		tags = append(tags, datastruct.Tag{
			ID:          paprikaTag.ID,
			Name:        paprikaTag.Name,
			CoinCounter: paprikaTag.CoinCounter,
			ICOCounter:  paprikaTag.ICOCounter,
		})
	}

	return tags
}

// MapCoinTeam maps CoinPaprika team members into the `datastruct.Person` structure.
// Parameters:
//   - paprikaTeam: A slice of `coinpaprika.Person` objects to be converted.
//
// Returns:
//   - A slice of `datastruct.Person` objects.
func MapCoinTeam(paprikaTeam []coinpaprika.Person) []datastruct.Person {
	var team []datastruct.Person

	// Iterate through the CoinPaprika team members and map their fields.
	for _, paprikaPerson := range paprikaTeam {
		// Ensure no nil pointers are dereferenced.
		if paprikaPerson.ID == nil || paprikaPerson.Name == nil || paprikaPerson.Position == nil {
			continue // Skip invalid entries.
		}

		team = append(team, datastruct.Person{
			ID:       *paprikaPerson.ID,
			Name:     *paprikaPerson.Name,
			Position: *paprikaPerson.Position,
		})
	}

	return team
}

func (c *coinListService) BuildCoinsHistoricalOHLCVData(ctx context.Context) error {
	span, labels := common.GenerateSpan("BuildCoinsHistoricalOHLCVData", ctx)
	defer span.End()

	span.AddEvent("Starting BuildCoinsHistoricalOHLCVData")

	startTime := log.StartTimeL(labels, "Starting BuildCoinsHistoricalOHLCVData")
	queryMGR := c.dao.NewCoinListQuery()

	coinsID, err := queryMGR.GetCoinsIDFromPG(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "BuildCoinsHistoricalOHLCVData: Error Getting CoinsID")
		log.EndTimeL(labels, "BuildCoinsHistoricalOHLCVData: Error Getting CoinsID", startTime, err)
		return err
	}

	ohlcvData, err := queryMGR.GetHistoricalOHLCVData(ctx, coinsID)
	if err != nil {
		span.SetStatus(codes.Error, "BuildCoinsHistoricalOHLCVData: Error Getting ohlcv Data")
		log.EndTimeL(labels, "BuildCoinsHistoricalOHLCVData: Error Getting ohlcv Data", startTime, err)
		return err
	}

	coinsOHLCVData := MapHistoricalOHLCVData(ohlcvData)

	err = queryMGR.UpsertHistoricalOHLCVData(ctx, coinsOHLCVData)

	if err != nil {
		span.SetStatus(codes.Error, "BuildCoinsHistoricalOHLCVData: Error Getting ohlcv Data")
		log.EndTimeL(labels, "BuildCoinsHistoricalOHLCVData: Error Getting ohlcv Data", startTime, err)
		return err
	}

	log.EndTimeL(labels, "", startTime, nil)
	span.SetStatus(codes.Ok, "")
	return nil
}

func MapHistoricalOHLCVData(ohlcvData map[string][]*coinpaprika.OHLCVEntry) []datastruct.CoinOHLCV {
	var coins []datastruct.CoinOHLCV

	for id, ohlcv := range ohlcvData {
		// Initialize an empty Coin struct for mapping.
		for _, data := range ohlcv {
			var coin datastruct.CoinOHLCV
			coin.ID = id
			coin.TimeOpen = *data.TimeOpen
			coin.TimeClose = *data.TimeClose
			coin.Open = *data.Open
			coin.High = *data.High
			coin.Low = *data.Low
			coin.Close = *data.Close
			coin.Volume = *data.Volume
			coin.MarketCap = *data.MarketCap
			coins = append(coins, coin)
		}
	}
	return coins
}
