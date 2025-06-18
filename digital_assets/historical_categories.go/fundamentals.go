
type CategoryFundamental struct {
	ID                          string               `json:"id" bigquery:"id" postgres:"id"`                                                                                                  // it present the id of category
	Name                        string               `json:"name" bigquery:"name" postgres:"name"`                                                                                            // it present the name of category
	TotalTokens                 bigquery.NullInt64   `json:"total_tokens" bigquery:"total_tokens" postgres:"total_tokens"`                                                                    // it present the total number of tokens that exist in each category
	IndexPercentage24H          bigquery.NullFloat64 `json:"index_percentage_24h" bigquery:"index_percentage_24h" postgres:"index_percentage_24h"`                                            // it present the percentage change for market cap in 24h
	Volume24H                   bigquery.NullFloat64 `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`                                                                          // it present the total volume for all assets in a category
	Price24H                    bigquery.NullFloat64 `json:"price_24h" bigquery:"price_24h" postgres:"price_24h"`                                                                             // it present the total price for all assets in a category
	MarketCap                   bigquery.NullFloat64 `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"`                                                                          // Market cap of the category
	WeightIndexPrice            bigquery.NullFloat64 `bigquery:"index_price_24h" json:"price_weight_index" postgres:"price_weight_index"`                                                     // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	WeightIndexMarketCap        bigquery.NullFloat64 `bigquery:"market_cap_weight_index" json:"market_cap_weight_index" postgres:"market_cap_weight_index"`                                   // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	MarketCapIndexValue24H      bigquery.NullFloat64 `bigquery:"market_cap_24h_index" json:"market_cap_index_value_24h" postgres:"market_cap_index_value_24h"`                                // it present the index market cap value for a category and it is the change value in market cap
	MarketCapIndexPercentage24H bigquery.NullFloat64 `bigquery:"market_cap_index_percentage_24h" json:"market_cap_index_percentage_24h,omitempty" postgres:"market_cap_index_percentage_24h"` // it present the percentage change for market cap index value in 24h
	Divisor                     bigquery.NullFloat64 `bigquery:"divisor" json:"divisor" postgres:"divisor"`                                                                                   // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	TopGainers                  []CategoryTopGainer  `json:"top_gainers,omitempty" bigquery:"top_gainers" postgres:"top_gainers"`                                                             // it present the top gainers in a category depends on market cap
	CreatedAt                   time.Time            `json:"created_at" bigquery:"created_at" postgres:"created_at"`                                                                          // it present the last time this record created
	LastUpdated                 time.Time            `json:"last_updated" bigquery:"-" postgres:"last_updated"`                                                                               // it present the last time this record created
}

type CategoryTopGainer struct {
	Slug            string               `json:"slug" bigquery:"slug" postgres:"slug"`
	Logo            string               `json:"logo" bigquery:"logo" postgres:"logo"`
	Symbol          string               `json:"symbol" bigquery:"symbol" postgres:"symbol"`
	MarketCap       bigquery.NullFloat64 `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"` // Market cap of the category
}


// Maps most recent candle data to fundamental
func MapCategoryFundamental(ctxO context.Context, category CategoriesData, assets *[]Fundamentals, categoryHistoricalData HistoricalCategoriesBQ) CategoryFundamental {
	ctx, span := tracer.Start(ctxO, "mapCategoryFundamental")
	defer span.End()

	span.AddEvent("Start Map Category Data To Category Fundamentals")
	// var categoryFundamental CategoryFundamental
	totalTokens := 0
	var totalPercentageChange float64 = 0.0
	var totalPrice float64 = 0.0
	var marketCap float64 = 0.0
	var volume24h float64 = 0.0
	var topGainers []CategoryTopGainer
	for _, market := range category.Markets {
		for _, asset := range *assets {
			if asset.Symbol != "" && asset.Status == "active" && asset.Symbol == market.ID {
				totalTokens++
				if asset.MarketCap != nil {
					marketCap += *asset.MarketCap
				}
				if asset.Volume != nil {
					volume24h += *asset.Volume
				}
				if asset.Percentage24h != nil {
					totalPercentageChange += *asset.Percentage24h
				}
				if asset.Price24h != nil {
					totalPrice += *asset.Price24h
				}

				if asset.MarketCap != nil && *asset.MarketCap > 0 {
					topGainers = append(topGainers, CategoryTopGainer{
						Slug:      asset.Slug,
						Logo:      asset.Logo,
						Symbol:    asset.Symbol,
						MarketCap: bigquery.NullFloat64{Float64: *asset.MarketCap, Valid: true},
					})
				}
				break
			}
		}
	}

	// Calculate the Index Values using the Categories Historical data and assets.
	categoryFundamental := CalculateCategoriesFundamentalsIndexPrice(ctx, marketCap, totalTokens, category, assets, categoryHistoricalData)
	categoryFundamental.ID = category.ID
	categoryFundamental.Name = category.Name
	categoryFundamental.Price24H = bigquery.NullFloat64{Float64: totalPrice, Valid: true}
	categoryFundamental.TotalTokens = bigquery.NullInt64{Int64: int64(totalTokens), Valid: true}
	categoryFundamental.MarketCap = bigquery.NullFloat64{Float64: marketCap, Valid: true}
	categoryFundamental.Volume24H = bigquery.NullFloat64{Float64: volume24h, Valid: true}

	// Sort top gainers by market cap and then select only the top 3 assets.
	sort.Slice(topGainers, func(i, j int) bool {
		return topGainers[i].MarketCap.Float64 > topGainers[j].MarketCap.Float64
	})
	topGainersLen := len(topGainers)
	if topGainersLen > 3 {
		topGainersLen = 3
	}
	categoryFundamental.TopGainers = topGainers[0:topGainersLen]
	categoryFundamental.CreatedAt = time.Now()
	categoryFundamental.LastUpdated = time.Now()

	span.SetStatus(otelCodes.Ok, "Success")
	return categoryFundamental
}

// CalculateCategoriesFundamentalsIndexPrice takes all data necessary to ca
func CalculateCategoriesFundamentalsIndexPrice(ctxO context.Context, totalMarketCap float64, totalTokens int, category CategoriesData, assets *[]Fundamentals, categoryHistoricalData HistoricalCategoriesBQ) CategoryFundamental {
	var totalPriceWeightIndex float64 = 0.0
	var categoryFundamental CategoryFundamental
	// we need to calculate MarketCapWeight
	for _, market := range category.Markets {
		for _, asset := range *assets {
			if asset.Symbol != "" && asset.Status == "active" && asset.Symbol == market.ID {
				if totalMarketCap != 0 {
					MarketCapWeight := *asset.MarketCap / totalMarketCap
					totalPriceWeightIndex += MarketCapWeight * *asset.Price24h
				}
			}
		}
	}
	// we need to calculate totalMarketCapWeightIndex
	totalMarketCapWeightIndex := totalMarketCap * totalPriceWeightIndex
	//we need to calculate totalMarketCap
	var marketCapIndexValue24H float64 = 0.0
	if totalMarketCap == 0 {
		marketCapIndexValue24H = 0
	} else {
		marketCapIndexValue24H = totalMarketCap / categoryHistoricalData.Divisor.Float64
	}
	categoryFundamental.WeightIndexMarketCap = bigquery.NullFloat64{Float64: totalMarketCapWeightIndex, Valid: true}
	categoryFundamental.MarketCapIndexValue24H = bigquery.NullFloat64{Float64: marketCapIndexValue24H, Valid: true}
	categoryFundamental.WeightIndexPrice = bigquery.NullFloat64{Float64: totalPriceWeightIndex, Valid: true}
	categoryFundamental.Divisor = bigquery.NullFloat64{Float64: categoryHistoricalData.Divisor.Float64, Valid: true}
	var marketCapIndexPercentage24H float64 = 0.0
	if marketCapIndexValue24H == 0 || categoryHistoricalData.MarketCapIndexValue24H.Float64 == 0 {
		marketCapIndexPercentage24H = 0
	} else {
		marketCapIndexPercentage24H = ((marketCapIndexValue24H - categoryHistoricalData.MarketCapIndexValue24H.Float64) / categoryHistoricalData.MarketCapIndexValue24H.Float64) * 100
	}
	categoryFundamental.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: marketCapIndexPercentage24H, Valid: true}

	var indexPercentageChange float64 = 0
	if totalTokens > 0 {
		if totalPriceWeightIndex == 0 || categoryHistoricalData.WeightIndexPrice.Float64 == 0 {
			indexPercentageChange = 0
		} else {
			indexPercentageChange = ((totalPriceWeightIndex - categoryHistoricalData.WeightIndexPrice.Float64) / categoryHistoricalData.WeightIndexPrice.Float64) * 100
		}
	}
	categoryFundamental.IndexPercentage24H = bigquery.NullFloat64{Float64: indexPercentageChange, Valid: true}
	return categoryFundamental
}
