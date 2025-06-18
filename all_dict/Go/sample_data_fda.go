v2.HandleFunc("/sample-data", GetSampleData).Methods(http.MethodGet, http.MethodOptions)
v2.HandleFunc("/nft-sample-data", GetNFTSampleData).Methods(http.MethodGet, http.MethodOptions)


func GetSampleData(w http.ResponseWriter, r *http.Request) {

	setResponseHeaders(w, 300)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetSampleData")

	defer span.End()

	// will determine the data type for which page Assets and NFTs
	// it will take two values FT and NFT

	labels["function"] = "GetSampleData"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Dynamic Description Data")

	// will return global dynamic description for NFTs or Assets page
	result, err := store.GetSampleDataPG(ctx)

	if result == nil && err == nil {
		span.SetStatus(codes.Error, "No Data Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "Dynamic Description Data", startTime, nil)
	span.SetStatus(codes.Ok, "Dynamic Description Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)

}
func GetNFTSampleData(w http.ResponseWriter, r *http.Request) {

	setResponseHeaders(w, 300)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetSampleData")

	defer span.End()

	// will determine the data type for which page Assets and NFTs
	// it will take two values FT and NFT

	labels["function"] = "GetSampleData"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Dynamic Description Data")

	// will return global dynamic description for NFTs or Assets page
	result, err := store.GetNFTSampleDataPG(ctx)

	if result == nil && err == nil {
		span.SetStatus(codes.Error, "No Data Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "Dynamic Description Data", startTime, nil)
	span.SetStatus(codes.Ok, "Dynamic Description Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)

}

///////////////
// postgresql
// NFTPrices Struct Will use to map the data that will retrieve from postgresql Table
type NFTPrices struct {
	ID                                         string  `json:"id" postgres:"id"`                                                                                             // It presents NFT Unique ID
	ContractAddress                            string  `json:"contract_address" postgres:"contract_address"`                                                                 // It presents NFT Contract Address
	AssetPlatformId                            string  `json:"asset_platform_id" postgres:"asset_platform_id"`                                                               // It presents the Chain ID that NFT is related to.
	Name                                       string  `json:"name" postgres:"name"`                                                                                         // It presents the NFT Name
	Symbol                                     string  `json:"symbol" postgres:"symbol"`                                                                                     // It presents the NFT Symbol
	DisplaySymbol                              string  `json:"displaySymbol" postgres:"display_symbol"`                                                                      // It presents the NFT Symbol
	Image                                      string  `json:"logo" postgres:"image"`                                                                                        // It presents the NFT Image
	Description                                string  `json:"description" postgres:"description"`                                                                           // It presents the NFT Description
	NativeCurrency                             string  `json:"native_currency" postgres:"native_currency"`                                                                   // It presents the NFT currency that NFT use to specify the currency like ethereum.
	FloorPriceUsd                              float64 `json:"floor_price_usd" postgres:"floor_price_usd"`                                                                   // It presents min price for the NFT in USD.
	MarketCapUsd                               float64 `json:"market_cap_usd" postgres:"market_cap_usd"`                                                                     // It presents the market cap for NFT in USD.
	Volume24hUsd                               float64 `json:"volume_24h_usd" postgres:"volume_24h_usd"`                                                                     // It presents volume for NFT in USD.
	FloorPriceNative                           float64 `json:"floor_price_native" postgres:"floor_price_native"`                                                             // It presents min price for NFT in native currency
	MarketCapNative                            float64 `json:"market_cap_native" postgres:"market_cap_native"`                                                               // It presents market cap for NFT in native currency
	Volume24hNative                            float64 `json:"volume_24h_native" postgres:"volume_24h_native"`                                                               // It presents volume for NFT in native currency
	FloorPriceInUsd24hPercentageChange         float64 `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`                 // It presents the percentage change in floor price for 24 hours for NFT
	Volume24hPercentageChangeUsd               float64 `json:"volume_24h_percentage_change_usd" postgres:"volume_24h_percentage_change_usd"`                                 // It presents the percentage change in floor price for 24 hours for NFT
	NumberOfUniqueAddresses                    int     `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`                                             // It presents the number of owners for the NFT
	NumberOfUniqueAddresses24hPercentageChange float64 `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"` // It presents the percentage change in the number of owners for 24 hours for NFTs.
	Slug                                       string  `json:"slug" postgres:"slug"`                                                                                         // It presents the slug for NFT
	TotalSupply                                float64 `json:"total_supply" postgres:"total_supply"`                                                                         // It presents total supply the NFT provide in there collection
	LastUpdated                                string  `json:"last_updated" postgres:"last_updated"`                                                                         // It presents last time NFT Data updated.
	FullCount                                  *int    `postgres:"full_count"`                                                                                               // It presents the number of NFTs that we have in Postgres.
	UUID                                       string  `json:"uuid"`                                                                                                         // It presents the number of NFTs that we have in Postgres.
	WebsiteUrl                                 string  `json:"website_url"`                                                                                                  // It presents the number of NFTs that we have in Postgres.
	TwitterUrl                                 string  `json:"twitter_url"`                                                                                                  // It presents the number of NFTs that we have in Postgres.
	DiscordUrl                                 string  `json:"discord_url"`                                                                                                  // It presents the number of NFTs that we have in Postgres.
	NativeCurrencySymbol                       string  `json:"native_currency_symbol"`                                                                                       // It presents the number of NFTs that we have in Postgres.
	MarketCap24HPercentageChangeUsd            float64 `json:"market_cap_24h_percentage_change_usd"`                                                                         // It presents the number of NFTs that we have in Postgres.
	MarketCap24HPercentageChangeNative         float64 `json:"market_cap_24h_percentage_change_native"`                                                                      // It presents the number of NFTs that we have in Postgres.
	Volume24hPercentageChangeNative            float64 `json:"volume_24h_percentage_change_native" postgres:"volume_24h_percentage_change_native"`                           // It presents the number of NFTs that we have in Postgres.
}

// It will retrieve all NFTs data from postgres
func PGGetNFTPricesTest(ctx0 context.Context, id string) (*NFTPrices, error) {

	ctx, span := tracer.Start(ctx0, "PGGetNFTPrices")
	defer span.End()

	startTime := log.StartTime("NFT Prices Query")
	pg := PGConnect()
	query := `SELECT ID,
			CONTRACT_ADDRESS,
			ASSET_PLATFORM_ID,
			NAME,
			SYMBOL,
			SYMBOL as display_symbol,
			IMAGE,
			DESCRIPTION,
			NATIVE_CURRENCY,
			FLOOR_PRICE_USD,
			MARKET_CAP_USD,
			VOLUME_24H_USD,
			FLOOR_PRICE_NATIVE,
			MARKET_CAP_NATIVE,
			VOLUME_24H_NATIVE,
			FLOOR_PRICE_IN_USD_24H_PERCENTAGE_CHANGE,
			VOLUME_24H_PERCENTAGE_CHANGE_USD,
			NUMBER_OF_UNIQUE_ADDRESSES,
			NUMBER_OF_UNIQUE_ADDRESSES_24H_PERCENTAGE_CHANGE,
			SLUG,
			TOTAL_SUPPLY,
			LAST_UPDATED,
			WEBSITE_URL,
			TWITTER_URL,
			DISCORD_URL,
			NATIVE_CURRENCY_SYMBOL,
			MARKET_CAP_24H_PERCENTAGE_CHANGE_USD,
			MARKET_CAP_24H_PERCENTAGE_CHANGE_NATIVE,
			VOLUME_24H_PERCENTAGE_CHANGE_NATIVE
		FROM PUBLIC.NFTDATALATEST
		WHERE ID = '` + id + `';`
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTime("NFT Prices Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var nft NFTPrices
	for queryResult.Next() {
		err := queryResult.Scan(&nft.ID, &nft.ContractAddress, &nft.AssetPlatformId, &nft.Name, &nft.Symbol, &nft.DisplaySymbol, &nft.Image, &nft.Description, &nft.NativeCurrency, &nft.FloorPriceUsd, &nft.MarketCapUsd, &nft.Volume24hUsd, &nft.FloorPriceNative, &nft.MarketCapNative, &nft.Volume24hNative, &nft.FloorPriceInUsd24hPercentageChange, &nft.Volume24hPercentageChangeUsd, &nft.NumberOfUniqueAddresses, &nft.NumberOfUniqueAddresses24hPercentageChange, &nft.Slug, &nft.TotalSupply, &nft.LastUpdated, &nft.WebsiteUrl, &nft.TwitterUrl, &nft.DiscordUrl, &nft.NativeCurrencySymbol, &nft.MarketCap24HPercentageChangeUsd, &nft.MarketCap24HPercentageChangeNative, &nft.Volume24hPercentageChangeNative)
		if err != nil {
			log.EndTime("NFT Prices Query", startTime, err)
			return nil, err
		}
	}
	return &nft, nil
}


func GetSampleDataPG(ctxO context.Context) ([]byte, error) {
	// gets fundamentals data from firestore

	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "GetFundamentalsFS")
	defer span.End()
	var fund []FundamentalsData
	for _, sym := range []string{"internet-computer", "pi-network-iou", "bitcoin"} {

		result, err := GetFundamentalsPG(ctx, sym)
		if err != nil {
			return nil, err
		}
		SortExchangePG(result.Exchanges)
		fund = append(fund, *result)
	}
	jsonData, err := json.Marshal(fund)
	if err != nil {
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "success")

	return jsonData, nil

}

func GetNFTSampleDataPG(ctxO context.Context) ([]byte, error) {
	// gets fundamentals data from firestore

	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "GetFundamentalsFS")
	defer span.End()
	var fund []NFTPrices
	for _, sym := range []string{"cryptopunks", "bored-ape-yacht-club", "doodles-official"} {

		result, err := PGGetNFTPricesTest(ctx, sym)
		if err != nil {
			return nil, err
		}
		fund = append(fund, *result)
	}
	jsonData, err := json.Marshal(fund)
	if err != nil {
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "success")

	return jsonData, nil

}
