DATA_NAMESPACE=_dev
ROWY_PREFIX=dev_
DB_PORT=5432
DB_HOST="forbesdevhpc-dbxtn.forbes.tessell.com"
DB_USER="master"
DB_PASSWORD="wkhzEYwlvpQTGTdR"
DB_NAME="forbes"
DB_SSLMODE=disable
PATCH_SIZE=1000
MON_LIMIT=2000000
CG_RATE_LIMIT=300
COINGECKO_URL="https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY=CG-V88xeVE4mSPsP71kS7LVWsDk


r.Handle("/get-nft", http.HandlerFunc(internal.GetNFTData)).Methods(http.MethodGet)
r.Handle("/get-assets", http.HandlerFunc(internal.GetSampleData)).Methods(http.MethodGet)

func GetNFTData(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "BuildNFTDynamicDescription")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "BuildNFTDynamicDescription")
	var nftsResult []models.NFTResult
	for _, nft := range []string{"cryptopunks", "bored-ape-yacht-club", "doodles-official"} {

		data, _, err := c.GetNFTMarket(ctx, nft)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
		}
		nftResult := MapNftData(*data)
		nftsResult = append(nftsResult, nftResult)
	}

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTsList", startTime, nil)
	span.SetStatus(codes.Ok, "OK")

	res, err := json.Marshal(nftsResult)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.WriteHeader(200)
	w.Write(res)

}

func MapNftData(data coingecko.NFTMarket) models.NFTResult {
	var nft models.NFTResult

	nft.ID = data.ID
	nft.ContractAddress = data.ContractAddress
	nft.AssetPlatformId = data.AssetPlatformID
	nft.Name = data.Name
	nft.Symbol = data.Symbol
	nft.Image = data.Image.Small
	nft.Description = data.Description
	nft.NativeCurrency = data.NativeCurrency
	nft.NativeCurrencySymbol = data.NativeCurrencySymbol
	nft.FloorPriceNative = data.FloorPrice.NativeCurrency
	nft.FloorPriceUsd = data.FloorPrice.Usd
	nft.MarketCapNative = data.MarketCap.NativeCurrency
	nft.MarketCapUsd = data.MarketCap.Usd
	nft.Volume24HNative = data.Volume24H.NativeCurrency
	nft.Volume24HUsd = data.Volume24H.Usd
	nft.FloorPriceInUsd24HPercentageChange = data.FloorPriceInUsd24HPercentageChange
	nft.FloorPrice24hPercentageChangeNative = data.FloorPrice24hPercentageChange.NativeCurrency
	nft.FloorPrice24hPercentageChangeUsd = data.FloorPrice24hPercentageChange.Usd
	nft.MarketCap24HPercentageChangeNative = data.MarketCap24HPercentageChange.NativeCurrency
	nft.MarketCap24HPercentageChangeUsd = data.MarketCap24HPercentageChange.Usd
	nft.Volume24HPercentageChangeNative = data.Volume24HPercentageChange.NativeCurrency
	nft.Volume24HPercentageChangeUsd = data.Volume24HPercentageChange.Usd
	nft.NumberOfUniqueAddresses = data.NumberOfUniqueAddresses
	nft.NumberOfUniqueAddresses24HPercentageChange = data.NumberOfUniqueAddresses24HPercentageChange
	nft.TotalSupply = data.TotalSupply
	nft.OneDaySales = ConvertToFloat(data.OneDaySales)
	nft.OneDaySales24HPercentageChange = ConvertToFloat(data.OneDaySales24HPercentageChange)
	nft.OneDayAverageSalePrice = ConvertToFloat(data.OneDayAverageSalePrice)
	nft.OneDayAverageSalePrice24HPercentageChange = ConvertToFloat(data.OneDayAverageSalePrice24HPercentageChange)
	nft.Homepage = data.Links.Homepage
	nft.Twitter = data.Links.Twitter
	nft.Discord = data.Links.Discord
	nft.FloorPrice7DPercentageChangeNative = data.FloorPrice7DPercentageChange.NativeCurrency
	nft.FloorPrice7DPercentageChangeUsd = data.FloorPrice7DPercentageChange.Usd
	nft.FloorPrice30DPercentageChangeNative = data.FloorPrice30DPercentageChange.NativeCurrency
	nft.FloorPrice30DPercentageChangeUsd = data.FloorPrice30DPercentageChange.Usd
	nft.FloorPrice14DPercentageChangeNative = data.FloorPrice14DPercentageChange.NativeCurrency
	nft.FloorPrice14DPercentageChangeUsd = data.FloorPrice14DPercentageChange.Usd
	nft.FloorPrice60DPercentageChangeNative = data.FloorPrice60DPercentageChange.NativeCurrency
	nft.FloorPrice60DPercentageChangeUsd = data.FloorPrice60DPercentageChange.Usd
	nft.FloorPrice1YPercentageChangeNative = data.FloorPrice1YPercentageChange.NativeCurrency
	nft.FloorPrice1YPercentageChangeUsd = data.FloorPrice1YPercentageChange.Usd

	return nft
}

func ConvertToFloat(strValue string) *float64 {
	var float *float64

	if strValue == "" {
		float = nil
	} else {
		val, _ := strconv.ParseFloat(strValue, 64)
		float = &val
	}

	return float
}

func GetSampleData(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "BuildNFTDynamicDescription")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "BuildNFTDynamicDescription")
	var nftsResult []coingecko.CoinsCurrentData
	currentCoinOptions := coingecko.CoinsCurrentDataOptions{
		Tickers:        false,
		Market_Data:    false,
		Community_Data: true,
		Developer_Data: true,
		Sparkline:      false,
	}
	for _, asset := range []string{"internet-computer", "pi-network-iou", "bitcoin"} {

		coinData, getCoinErr := c.GetCurrentCoinData(ctx, asset, &currentCoinOptions)
		if getCoinErr != nil {
			span.SetStatus(codes.Error, getCoinErr.Error())
			w.WriteHeader(http.StatusInternalServerError)
		}
		nftsResult = append(nftsResult, *coinData)
	}

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTsList", startTime, nil)
	span.SetStatus(codes.Ok, "OK")

	res, err := json.Marshal(nftsResult)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.WriteHeader(200)
	w.Write(res)
}


package models

type NFTResult struct {
	ID                                         string   `json:"id"`
	ContractAddress                            string   `json:"contract_address"`
	AssetPlatformId                            string   `json:"asset_platform_id"`
	Name                                       string   `json:"name"`
	Symbol                                     string   `json:"symbol"`
	Image                                      string   `json:"image"`
	Description                                string   `json:"description"`
	NativeCurrency                             string   `json:"native_currency"`
	NativeCurrencySymbol                       string   `json:"native_currency_symbol"`
	FloorPriceNative                           float64  `json:"floor_price_native"`
	FloorPriceUsd                              float64  `json:"floor_price_usd"`
	MarketCapNative                            float64  `json:"market_cap_native"`
	MarketCapUsd                               float64  `json:"market_cap_usd"`
	Volume24HNative                            float64  `json:"volume_24h_native"`
	Volume24HUsd                               float64  `json:"volume_24h_usd"`
	FloorPriceInUsd24HPercentageChange         float64  `json:"floor_price_in_usd_24h_percentage_change"`
	FloorPrice24hPercentageChangeNative        float64  `json:"floor_price_24h_percentage_change_native"`
	FloorPrice24hPercentageChangeUsd           float64  `json:"floor_price_24h_percentage_change_usd"`
	MarketCap24HPercentageChangeNative         float64  `json:"market_cap_24h_percentage_change_native"`
	MarketCap24HPercentageChangeUsd            float64  `json:"market_cap_24h_percentage_change_usd"`
	Volume24HPercentageChangeNative            float64  `json:"volume_24h_percentage_change_native"`
	Volume24HPercentageChangeUsd               float64  `json:"volume_24h_percentage_change_usd"`
	NumberOfUniqueAddresses                    float64  `json:"number_of_unique_addresses"`
	NumberOfUniqueAddresses24HPercentageChange float64  `json:"number_of_unique_addresses_24h_percentage_change"`
	TotalSupply                                float64  `json:"total_supply"`
	OneDaySales                                *float64 `json:"one_day_sales"`
	OneDaySales24HPercentageChange             *float64 `json:"one_day_sales_24h_percentage_change"`
	OneDayAverageSalePrice                     *float64 `json:"one_day_average_sale_price"`
	OneDayAverageSalePrice24HPercentageChange  *float64 `json:"one_day_average_sale_price_24h_percentage_change"`
	Homepage                                   string   `json:"homepage"`
	Twitter                                    string   `json:"twitter"`
	Discord                                    string   `json:"discord"`
	FloorPrice7DPercentageChangeNative         float64  `json:"floor_price_7d_percentage_change_native"`
	FloorPrice7DPercentageChangeUsd            float64  `json:"floor_price_7d_percentage_change_usd"`
	FloorPrice14DPercentageChangeNative        float64  `json:"floor_price_14d_percentage_change_native"`
	FloorPrice14DPercentageChangeUsd           float64  `json:"floor_price_14d_percentage_change_usd"`
	FloorPrice30DPercentageChangeNative        float64  `json:"floor_price_30d_percentage_change_native"`
	FloorPrice30DPercentageChangeUsd           float64  `json:"floor_price_30d_percentage_change_usd"`
	FloorPrice60DPercentageChangeNative        float64  `json:"floor_price_60d_percentage_change_native"`
	FloorPrice60DPercentageChangeUsd           float64  `json:"floor_price_60d_percentage_change_usd"`
	FloorPrice1YPercentageChangeNative         float64  `json:"floor_price_1y_percentage_change_native"`
	FloorPrice1YPercentageChangeUsd            float64  `json:"floor_price_1y_percentage_change_usd"`
}
