
type HistoricalCategories struct {
	Date                        time.Time                   `bigquery:"day_start" json:"day_start"`                                                       // it present the date for a category
	TotalPrice24H               float64                     `bigquery:"total_price" json:"total_price_24h"`                                               // it present the total price for all assets in a category
	TotalVolume24H              float64                     `bigquery:"total_volume" json:"total_volume_24h"`                                             // it present the total volume for all assets in a category
	TotalMarketCap24H           float64                     `bigquery:"total_market_cap" json:"total_market_cap_24h"`                                     // it present the total market cap for all assets in a category
	TotalPriceWeightIndex       float64                     `bigquery:"price_weight_index" json:"price_weight_index"`                                     // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	TotalMarketCapWeightIndex   float64                     `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`                           // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	Divisor                     float64                     `bigquery:"divisor" json:"divisor"`                                                           // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	MarketCapIndexValue24H      float64                     `bigquery:"market_cap_index_value_24h" json:"market_cap_index_value_24h"`                     // it present the index market cap value for a category and it is the change value in market cap
	MarketCapPercentage24H      float64                     `bigquery:"market_cap_percentage_24h" json:"market_cap_percentage_24h,omitempty"`             // it present the percentage change for market cap in 24h
	MarketCapIndexPercentage24H float64                     `bigquery:"market_cap_index_percentage_24h" json:"market_cap_index_percentage_24h,omitempty"` // it present the percentage change for market cap index value in 24h
	LastUpdated                 time.Time                   `bigquery:"last_updated" json:"last_updated"`                                                 // it present the last time this record updated
	Prices                      []HistoricalCategoriesSlice `bigquery:"beprices" json:"beprices,omitempty"`                                               // it present an array of object that contains price, symbol, volume and market cap for assets
	TotalTokens                 int                         `bigquery:"total_tokens" json:"total_tokens"`                                                 // it present the total number of tokens that exist in each category
	Name                        string                      `bigquery:"name" json:"name"`                                                                 // it present the name of category
	ID                          string                      `bigquery:"id" json:"id"`                                                                     // it present the id of category
	TopGainers                  []CategoryTopGainer         `bigquery:"top_gainers" json:"top_gainers"`                                                   // it present the top gainers in a category depends on market cap
}

type HistoricalCategoriesBQ struct {
	Date                        bigquery.NullTimestamp `bigquery:"Date" json:"day_start"`                                                            // it present the date for a category
	Price24H                    bigquery.NullFloat64   `bigquery:"price_24h" json:"total_price_24h"`                                                 // it present the total price for all assets in a category
	Volume24H                   bigquery.NullFloat64   `bigquery:"volume_24h" json:"total_volume_24h"`                                               // it present the total volume for all assets in a category
	MarketCap24H                bigquery.NullFloat64   `bigquery:"market_cap_24h" json:"total_market_cap_24h"`                                       // it present the total market cap for all assets in a category
	WeightIndexPrice            bigquery.NullFloat64   `bigquery:"index_price_24h" json:"price_weight_index"`                                        // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	WeightIndexMarketCap        bigquery.NullFloat64   `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`                           // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	Divisor                     bigquery.NullFloat64   `bigquery:"divisor" json:"divisor"`                                                           // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	MarketCapIndexValue24H      bigquery.NullFloat64   `bigquery:"market_cap_24h_index" json:"market_cap_index_value_24h"`                           // it present the index market cap value for a category and it is the change value in market cap
	MarketCapPercentage24H      bigquery.NullFloat64   `bigquery:"market_cap_percentage_24h" json:"market_cap_percentage_24h,omitempty"`             // it present the percentage change for market cap in 24h
	MarketCapIndexPercentage24H bigquery.NullFloat64   `bigquery:"market_cap_index_percentage_24h" json:"market_cap_index_percentage_24h,omitempty"` // it present the percentage change for market cap index value in 24h
	LastUpdated                 bigquery.NullTimestamp `bigquery:"row_last_updated" json:"last_updated"`                                             // it present the last time this record updated
	CreatedAt                   bigquery.NullTimestamp `bigquery:"created_at" json:"created_at"`                                                     // it present the last time this record created
	TotalTokens                 int                    `bigquery:"total_tokens" json:"total_tokens"`                                                 // it present the total number of tokens that exist in each category
	Name                        string                 `bigquery:"name" json:"name"`                                                                 // it present the name of category
	ID                          string                 `bigquery:"id" json:"id"`                                                                     // it present the id of category
	TopGainers                  []CategoryTopGainer    `bigquery:"top_gainers" json:"top_gainers"`                                                   // it present the top gainers in a category depends on market cap
}
type HistoricalCategoriesSlice struct {
	Symbol          string  `bigquery:"symbol" json:"symbol"`
	Price           float64 `bigquery:"price" json:"price"`
	MarketCap       float64 `bigquery:"market_cap" json:"market_cap"`
}

func (bq *BQStore) BuildCategoriesHistoricalData(ctx0 context.Context, categoryPG Categories, assetsMetaData map[string]AssetMetaData) ([]HistoricalCategoriesBQ, error) {
	ctx, span := tracer.Start(ctx0, "BuildCategoriesHistoricalData")

	defer span.End()

	log.Debug("BuildCategoriesHistoricalData")

	query := bq.Query(`
	SELECT
		day_start,
		SUM(price) AS total_price,
		SUM(market_cap) AS total_market_cap,
		SUM(volume) AS total_volume,
		ARRAY_AGG(STRUCT('symbol',
			symbol,
			'price',
			price,
			'marketCap',
			market_cap
			)) AS beprices
	FROM (
		SELECT
			symbol,
			TIMESTAMP_TRUNC(time, DAY) AS day_start,
			Max(price) AS price, -- Using MAX() to get one value per 24-hour interval
			MAX(MarketCap) AS market_cap,
			MAX(Volume) AS volume
		FROM (
			SELECT
				ID AS symbol,
				Occurance_Time AS time,
				CAST(AVG(Price) AS FLOAT64) AS price,
				CAST(AVG(MarketCap) AS FLOAT64) AS MarketCap,
				CAST(AVG(Volume) AS FLOAT64) AS Volume,
				ROW_NUMBER() OVER (PARTITION BY ID, TIMESTAMP_TRUNC(Occurance_Time, DAY)
				ORDER BY
					Occurance_Time) AS row_num
			FROM
				api-project-901373404215.digital_assets.Digital_Asset_MarketData c
			WHERE
				Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 366 day)
				AND ID in UNNEST(@coins)
			GROUP BY 
				symbol,
				time
		) AS foo
	WHERE
		row_num = 1 -- Only select the first row within each 24-hour interval
	GROUP BY
		day_start,
		symbol
	ORDER BY
		day_start DESC ) AS fo
	GROUP BY
		day_start
	ORDER BY 
		day_start asc
	`)

	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "coins",
			Value: categoryPG.Coins,
		},
	}

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("BuildCategoriesHistoricalData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}
	var categories []HistoricalCategories
	for {
		var category HistoricalCategories

		err := it.Next(&category)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		categories = append(categories, category)
	}
	var allCategories []HistoricalCategoriesBQ
	var categoriesResult []HistoricalCategoriesBQ
	var divisor float64 = 0.0
	for _, category := range categories {
		var topGainers []CategoryTopGainer
		var categoryData HistoricalCategoriesBQ
		var totalPriceWeightIndex float64 = 0.0
		var totalMarketCapWeightIndex float64 = 0.0
		for _, total := range category.Prices {
			var asset HistoricalCategoriesSlice
			var topGainer CategoryTopGainer

			assetMeta := assetsMetaData[total.Symbol]
			topGainer.MarketCap = bigquery.NullFloat64{Float64: total.MarketCap, Valid: true}
			topGainer.Logo = assetMeta.LogoURL
			topGainer.Symbol = assetMeta.ID
			topGainer.Slug = strings.ToLower(fmt.Sprintf("%s-%s", strings.ReplaceAll(assetMeta.Name, " ", "-"), assetMeta.ID))
			topGainers = append(topGainers, topGainer)

			asset.Symbol = total.Symbol
			asset.MarketCap = CheckAndConvertFloat(total.MarketCap)
			asset.Price = CheckAndConvertFloat(total.Price)
			// PriceWeight is a value we need can calculate by divided price for an asset by total price for all assets in a category
			// PriceWeight := CheckAndConvertFloat((total.Price / category.TotalPrice24H))
			// MarketCapWeight is a value we need can calculate by divided MarketCap for an asset by total marketcap for all assets in a category
			MarketCapWeight := CheckAndConvertFloat((total.MarketCap / category.TotalMarketCap24H))
			// totalPriceWeightIndex is Summation for MarketCapWeight multiple by Price for asset
			// totalPriceWeightIndex we need it to calculate totalMarketCapWeightIndex
			totalPriceWeightIndex += MarketCapWeight * asset.Price
			// categoryData.Prices = append(categoryData.Prices, allPrices)
		}
		// totalMarketCapWeightIndex is a value we need to calculate the Divisor.
		// We need to multiple TotalMarketCap24H for category by totalPriceWeightIndex
		totalMarketCapWeightIndex = category.TotalMarketCap24H * totalPriceWeightIndex
		// Divisor is a value we need to calculate so we can calculate the Index Value from it.
		// To calculate Divisor we need the TotalMarketCap24H for category that will divided by Base Value.
		// The Base Value can be 100 or 1000 to calculate.
		// For our calculation we will use 1000 as base value.
		// Divisor will calculated from the oldest value.
		if divisor == 0 {
			divisor = CheckAndConvertFloat(category.TotalMarketCap24H / 1000)
		}
		// indexValue it will present the index value change for market cap in 24 hour
		// So we can use it to measure the changes in MarketCap value.
		var indexValue float64 = 0.0
		if category.TotalMarketCap24H == 0 {
			indexValue = 0
		} else {
			indexValue = CheckAndConvertFloat(category.TotalMarketCap24H / divisor) // this will present the index value change for market cap // MarketCapIndexValue
		}

		categoryData.WeightIndexMarketCap = bigquery.NullFloat64{Float64: CheckAndConvertFloat(totalMarketCapWeightIndex), Valid: true}
		categoryData.WeightIndexPrice = bigquery.NullFloat64{Float64: CheckAndConvertFloat(totalPriceWeightIndex), Valid: true}
		categoryData.Date = bigquery.NullTimestamp{Timestamp: category.Date, Valid: true}
		categoryData.MarketCap24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalMarketCap24H), Valid: true}
		categoryData.Price24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalPrice24H), Valid: true}
		categoryData.Volume24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalVolume24H), Valid: true}
		categoryData.Divisor = bigquery.NullFloat64{Float64: divisor, Valid: true}
		categoryData.MarketCapIndexValue24H = bigquery.NullFloat64{Float64: indexValue, Valid: true}
		categoryData.TotalTokens = len(category.Prices)
		categoryData.ID = categoryPG.ID
		categoryData.Name = categoryPG.Name
		categoryData.LastUpdated = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
		categoryData.CreatedAt = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}

		sort.Slice(topGainers, func(i, j int) bool {
			return topGainers[i].MarketCap.Float64 > topGainers[j].MarketCap.Float64
		})
		topGainersLen := len(topGainers)
		if topGainersLen > 3 {
			topGainersLen = 3
		}
		categoryData.TopGainers = topGainers[0:topGainersLen]
		allCategories = append(allCategories, categoryData)
	}
	SortHistoricalCategories(allCategories)
	for i := 0; i < len(allCategories); i++ {
		y := allCategories[i]
		if i == len(allCategories)-1 {
			y.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
			y.MarketCapPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
		} else {
			y.MarketCapPercentage24H = bigquery.NullFloat64{Float64: ((ConvertBQFloatToNormalFloat(y.MarketCap24H) - ConvertBQFloatToNormalFloat(allCategories[i+1].MarketCap24H)) / ConvertBQFloatToNormalFloat(allCategories[i+1].MarketCap24H)) * 100, Valid: true}
			y.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: ((ConvertBQFloatToNormalFloat(y.MarketCapIndexValue24H) - ConvertBQFloatToNormalFloat(allCategories[i+1].MarketCapIndexValue24H)) / ConvertBQFloatToNormalFloat(allCategories[i+1].MarketCapIndexValue24H)) * 100, Valid: true}
		}

		categoriesResult = append(categoriesResult, y)
	}
	// cal market cap percentage change 24h - total
	// only for 24 h change market cap percentage.
	log.Info("BuildCategoriesHistoricalData")
	return categoriesResult, nil
}

func CheckAndConvertFloat(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	convValue, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", v), 64)
	return convValue
}

func SortHistoricalCategories(categories []HistoricalCategoriesBQ) {
	sort.Slice(categories, func(i, j int) bool {
		return categories[i].Date.Timestamp.After(categories[j].Date.Timestamp)
	})
}

func (bq *BQStore) InsertCategoriesHistoricalDataBQ(ctx0 context.Context, uuid string, allCategories []HistoricalCategoriesBQ) error {
	ctx, span := tracer.Start(ctx0, "InsertCategoriesHistoricalDataBQ")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "InsertCategoriesHistoricalDataBQ"

	startTime := log.StartTimeL(labels, "InsertCategoriesHistoricalDataBQ")

	categoryHistoricalTable := "Categories_historical_data_test"

	bqInserter := bq.Dataset("digital_assets").Table(categoryHistoricalTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, allCategories)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up Categories Historical and retrying insert")
			l := len(allCategories)
			var ticks []HistoricalCategoriesBQ
			ticks = append(ticks, allCategories...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertCategoriesHistoricalDataBQ(ctx, uuid, a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			return retryError
		}
		//if not a 413 error return the error
		return inserterErr

	}

	span.SetStatus(codes.Ok, "InsertCategoryFundamentalsBQ Completed")
	log.EndTimeL(labels, "InsertCategoryFundamentalsBQ", startTime, nil)
	return nil
}




// BQInsertCommunityMembersInfo insert all members info that fetched from PG to BQ
// Takes (ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo)
// Returns (error)
//
// Returns  error if the insert process to BQ failed and no error if successful
// func (bq *BQStore) InsertCategoriesHistoricalDataBQ(ctx0 context.Context, uuid string, allCategories []HistoricalCategoriesBQ) error {
// 	ctx, span := tracer.Start(ctx0, "InsertCategoriesHistoricalDataBQ")
// 	defer span.End()

// 	labels := make(map[string]string)
// 	labels["uuid"] = uuid
// 	span.SetAttributes(attribute.String("uuid", uuid))
// 	labels["spanID"] = span.SpanContext().SpanID().String()
// 	labels["traceID"] = span.SpanContext().TraceID().String()
// 	labels["bigquery"] = "true"
// 	span.SetAttributes(attribute.Bool("bigquery", true))
// 	labels["subFunction"] = "InsertCategoriesHistoricalDataBQ"
// 	span.AddEvent(fmt.Sprintf("Starting %s", "InsertCategoriesHistoricalDataBQ"))
// 	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "InsertCategoriesHistoricalDataBQ"))
// 	categoryHistoricalTable := "Categories_historical_data_test"

// 	var initialRecord []string
// 	// var subsequentRecords string
// 	for index, category := range allCategories {

// 		// start build the Select statement for all rows that will be inserted or updated
// 		if index == 0 {
// 			initialRecord = append(initialRecord, BuildSelectStatementForCommunityMembersInfo(category))
// 		} else {
// 			initialRecord = append(initialRecord, fmt.Sprintf(" UNION ALL %s", BuildSelectStatementForCommunityMembersInfo(category)))
// 		}
// 		if len(initialRecord) >= 9999 || index == len(allCategories)-1 {
// 			if strings.Contains(initialRecord[0], " UNION ALL ") {
// 				strings.Replace(initialRecord[0], " UNION ALL ", "SELECT ", 0)
// 			}
// 			queryString := `MERGE INTO api-project-901373404215.digital_assets.` + categoryHistoricalTable + ` T
// 			USING (
// 			  ` + strings.Join(initialRecord, "") + `
// 			) AS S
// 			ON T.id = S.id and T.Date = S.Date
// 			WHEN MATCHED THEN
// 			  UPDATE SET
// 			  id = S.id,
// 			  name = S.name,
// 			  total_tokens = S.total_tokens,
// 			  market_cap_24h = S.market_cap_24h,
// 			  market_cap_24h_index = S.market_cap_24h_index
// 			  market_cap_percentage_24h = S.market_cap_percentage_24h
// 			  market_cap_index_percentage_24h = S.market_cap_index_percentage_24h
// 			  volume_24h = S.volume_24h
// 			  price_24h = S.price_24h
// 			  index_price_24h = S.index_price_24h
// 			  divisor = S.divisor
// 			  Date = S.Date
// 			  row_last_updated = S.row_last_updated
// 			  created_at = S.created_at
// 			WHEN NOT MATCHED THEN
// 			  INSERT (id, name, total_tokens, market_cap_24h, market_cap_24h_index, market_cap_percentage_24h, volume_24h, price_24h, index_price_24h, divisor, Date, row_last_updated, created_at)
// 			  VALUES (
// 				S.id,
// 				S.name,
// 				S.total_tokens,
// 				S.market_cap_24h,
// 				S.market_cap_24h_index,
// 				S.market_cap_percentage_24h,
// 				S.market_cap_index_percentage_24h,
// 				S.volume_24h,
// 				S.price_24h,
// 				S.index_price_24h,
// 				S.divisor,
// 				S.Date,
// 				S.row_last_updated,
// 				S.created_at
// 			  );`
// 			query := bq.Query(queryString)

// 			job, err := query.Run(ctx)
// 			if err != nil {
// 				log.EndTimeL(labels, "InsertCategoriesHistoricalDataBQ Error Upserting Member Info ", startTime, err)
// 				return err
// 			}
// 			log.Info("InsertCategoriesHistoricalDataBQ BigQuery Job ID : %s", job.ID())

// 			_, err = job.Wait(ctx)
// 			if err != nil {
// 				log.EndTimeL(labels, "InsertCategoriesHistoricalDataBQ Error Upserting Member Info ", startTime, err)
// 				return err
// 			}
// 		}
// 	}

// 	log.EndTimeL(labels, "InsertCategoriesHistoricalDataBQ Finished Successfully ", startTime, nil)
// 	span.SetStatus(codes.Ok, "InsertCategoriesHistoricalDataBQ Finished Successfully ")

// 	return nil
// }

// // BuildSelectStatementForCommunityMembersInfo build member select Query
// // Takes (memberInfo datastruct.BQCommunityMemberInfo)
// // Returns (string)
// //
// // Returns query string that we need to use in merge statement
// func BuildSelectStatementForCommunityMembersInfo(category HistoricalCategoriesBQ) string {
// 	rowDate := category.Date.Timestamp
// 	date := rowDate.Format("2006-01-02 15:04:05")
// 	rowLastUpdatedDate := category.LastUpdated.Timestamp
// 	rowLastUpdated := rowLastUpdatedDate.Format("2006-01-02 15:04:05")
// 	rowCreatedAt := category.CreatedAt.Timestamp
// 	createdAt := rowCreatedAt.Format("2006-01-02 15:04:05")
// 	record := `
// 	SELECT
// 	` + category.ID + ` AS id,
// 	` + category.Name + ` AS name,
// 	` + fmt.Sprintf("%d", category.TotalTokens) + ` AS total_tokens,
// 	` + fmt.Sprintf("%v", category.TotalMarketCap24H) + ` AS market_cap_24h,
// 	` + fmt.Sprintf("%v", category.MarketCapIndexValue24H) + ` AS market_cap_24h_index,
// 	` + fmt.Sprintf("%v", category.MarketCapPercentage24H) + ` AS market_cap_percentage_24h,
// 	` + fmt.Sprintf("%v", category.MarketCapIndexPercentage24H) + ` AS market_cap_index_percentage_24h,
// 	` + fmt.Sprintf("%v", category.TotalVolume24H) + ` AS volume_24h,
// 	` + fmt.Sprintf("%v", category.TotalPrice24H) + ` AS price_24h,
// 	` + fmt.Sprintf("%v", category.TotalPriceWeightIndex) + ` AS index_price_24h,
// 	` + fmt.Sprintf("%v", category.Divisor) + ` AS divisor,
// 	TIMESTAMP("` + string(date) + `") AS Date,
// 	TIMESTAMP("` + string(rowLastUpdated) + `") AS row_last_updated,
// 	TIMESTAMP("` + string(createdAt) + `") AS created_at
// 	`

// 	return record
// }

ID                        string               `json:"id" bigquery:"id" postgres:"id"`
Name                      string               `json:"name" bigquery:"name" postgres:"name"`
TotalTokens               bigquery.NullInt64   `json:"total_tokens" bigquery:"total_tokens" postgres:"total_tokens"`
Percentage24H             bigquery.NullFloat64 `json:"percentage_24h" bigquery:"percentage_24h" postgres:"percentage_24h"`
Volume24H                 bigquery.NullFloat64 `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`
AveragePrice              bigquery.NullFloat64 `json:"index_price_24h" bigquery:"average_price" postgres:"average_price"`
TotalPriceWeightIndex     bigquery.NullFloat64 `bigquery:"index_price_24h" json:"price_weight_index"`
Divisor                   bigquery.NullFloat64 `bigquery:"divisor" json:"divisor"`
TotalMarketCapWeightIndex bigquery.NullFloat64 `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`
MarketCapIndexValue24H    bigquery.NullFloat64 `bigquery:"market_cap_24h_index" json:"market_cap_index_value_24h"`
MarketCap                 bigquery.NullFloat64 `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"` // Market cap of the category
Price24H                  bigquery.NullFloat64 `json:"price_24h" bigquery:"price_24h" postgres:"price_24h"`    // Market cap of the category
TopGainers                []CategoryTopGainer  `json:"top_gainers,omitempty" bigquery:"top_gainers" postgres:"top_gainers"`
CreatedAt                 time.Time            `json:"created_at" bigquery:"created_at" postgres:"created_at"`
LastUpdated               time.Time            `json:"last_updated" bigquery:"-" postgres:"last_updated"`

	// we need to calculate TotalPriceWeightIndex
	var totalPriceWeightIndex float64 = 0.0
	var divisor float64 = 0.0
	// we need to calculate MarketCapWeight
	for _, market := range category.Markets {
		for _, asset := range *assets {
			if asset.Symbol != "" && asset.Status == "active" && asset.Symbol == market.ID {
				MarketCapWeight := *asset.MarketCap / marketCap
				totalPriceWeightIndex += MarketCapWeight * *asset.Price24h
			}
		}
	}
	// we need to calculate totalMarketCapWeightIndex
	totalMarketCapWeightIndex := marketCap * totalPriceWeightIndex
	// we need to calculate Divisor
	if divisor == 0 {
		divisor = marketCap / 1000
	}
	//we need to calculate MarketCapIndexValue24H
	var marketCapIndexValue24H float64 = 0.0
	if marketCap == 0 {
		marketCapIndexValue24H = 0
	} else {
		marketCapIndexValue24H = marketCap / divisor
	}
	categoryFundamental.TotalMarketCapWeightIndex = bigquery.NullFloat64{Float64: totalMarketCapWeightIndex, Valid: true}
	categoryFundamental.MarketCapIndexValue24H = bigquery.NullFloat64{Float64: marketCapIndexValue24H, Valid: true}
	categoryFundamental.TotalPriceWeightIndex = bigquery.NullFloat64{Float64: totalPriceWeightIndex, Valid: true}
	categoryFundamental.Divisor = bigquery.NullFloat64{Float64: divisor, Valid: true}
	categoryFundamental.Price24H = bigquery.NullFloat64{Float64: totalPrice, Valid: true}





// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================
// ========================================================================================================================

type HistoricalCategories struct {
	Date                        time.Time                   `bigquery:"day_start" json:"day_start"`                                                       // it present the date for a category
	TotalPrice24H               float64                     `bigquery:"total_price" json:"total_price_24h"`                                               // it present the total price for all assets in a category
	TotalVolume24H              float64                     `bigquery:"total_volume" json:"total_volume_24h"`                                             // it present the total volume for all assets in a category
	TotalMarketCap24H           float64                     `bigquery:"total_market_cap" json:"total_market_cap_24h"`                                     // it present the total market cap for all assets in a category
	TotalPriceWeightIndex       float64                     `bigquery:"price_weight_index" json:"price_weight_index"`                                     // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	TotalMarketCapWeightIndex   float64                     `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`                           // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	Divisor                     float64                     `bigquery:"divisor" json:"divisor"`                                                           // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	MarketCapIndexValue24H      float64                     `bigquery:"market_cap_index_value_24h" json:"market_cap_index_value_24h"`                     // it present the index market cap value for a category and it is the change value in market cap
	MarketCapPercentage24H      float64                     `bigquery:"market_cap_percentage_24h" json:"market_cap_percentage_24h,omitempty"`             // it present the percentage change for market cap in 24h
	MarketCapIndexPercentage24H float64                     `bigquery:"market_cap_index_percentage_24h" json:"market_cap_index_percentage_24h,omitempty"` // it present the percentage change for market cap index value in 24h
	LastUpdated                 time.Time                   `bigquery:"last_updated" json:"last_updated"`                                                 // it present the last time this record updated
	Prices                      []HistoricalCategoriesSlice `bigquery:"beprices" json:"beprices,omitempty"`                                               // it present an array of object that contains price, symbol, volume and market cap for assets
	TotalTokens                 int                         `bigquery:"total_tokens" json:"total_tokens"`                                                 // it present the total number of tokens that exist in each category
	Name                        string                      `bigquery:"name" json:"name"`                                                                 // it present the name of category
	ID                          string                      `bigquery:"id" json:"id"`                                                                     // it present the id of category
	TopGainers                  []CategoryTopGainer         `bigquery:"top_gainers" json:"top_gainers"`                                                   // it present the top gainers in a category depends on market cap
}


type HistoricalCategoriesBQ struct {
	ID                          string                 `bigquery:"id" json:"id"`                         // it present the id of category
	Name                        string                 `bigquery:"name" json:"name"`                     // it present the name of category
	TotalTokens                 bigquery.NullInt64     `bigquery:"total_tokens" json:"total_tokens"`     // it present the total number of tokens that exist in each category
	Percentage24H               bigquery.NullFloat64   `bigquery:"percentage_24h" json:"percentage_24h"` // it present the total price for all assets in a category
	Volume24H                   bigquery.NullFloat64   `bigquery:"volume_24h" json:"total_volume_24h"`   // it present the total volume for all assets in a category
	Price24H                    bigquery.NullFloat64   `bigquery:"price_24h" json:"total_price_24h"`     // it present the total price for all assets in a category
	AveragePrice                bigquery.NullFloat64   `bigquery:"average_price" json:"average_price"`
	MarketCap24H                bigquery.NullFloat64   `bigquery:"market_cap_24h" json:"total_market_cap_24h"`                                       // it present the total market cap for all assets in a category
	MarketCapPercentage24H      bigquery.NullFloat64   `bigquery:"market_cap_percentage_change" json:"market_cap_percentage_change,omitempty"`       // it present the percentage change for market cap in 24h
	WeightIndexPrice            bigquery.NullFloat64   `bigquery:"price_weight_index" json:"price_weight_index"`                                     // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	WeightIndexMarketCap        bigquery.NullFloat64   `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`                           // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	MarketCapIndexValue24H      bigquery.NullFloat64   `bigquery:"index_market_cap_24h" json:"index_market_cap_24h"`                                 // it present the index market cap value for a category and it is the change value in market cap
	MarketCapIndexPercentage24H bigquery.NullFloat64   `bigquery:"index_market_cap_percentage_24h" json:"market_cap_index_percentage_24h,omitempty"` // it present the percentage change for market cap index value in 24h
	Divisor                     bigquery.NullFloat64   `bigquery:"divisor" json:"divisor"`                                                           // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	TopGainers                  []CategoryTopGainer    `bigquery:"top_gainers" json:"top_gainers"`                                                   // it present the top gainers in a category depends on market cap
	Date                        bigquery.NullTimestamp `bigquery:"Date" json:"day_start"`                                                            // it present the date for a category
	LastUpdated                 bigquery.NullTimestamp `bigquery:"row_last_updated" json:"last_updated"`                                             // it present the last time this record updated
}

type HistoricalCategoriesSlice struct {
	Symbol    string  `bigquery:"symbol" json:"symbol"`
	Price     float64 `bigquery:"price" json:"price"`
	MarketCap float64 `bigquery:"market_cap" json:"market_cap"`
}

func (bq *BQStore) BuildCategoriesHistoricalData(ctx0 context.Context, categoryPG Categories, assetsMetaData map[string]AssetMetaData) ([]HistoricalCategoriesBQ, error) {
	ctx, span := tracer.Start(ctx0, "BuildCategoriesHistoricalData")

	defer span.End()

	log.Debug("BuildCategoriesHistoricalData")

	query := bq.Query(`
	SELECT
		day_start,
		SUM(price) AS total_price,
		SUM(market_cap) AS total_market_cap,
		SUM(volume) AS total_volume,
		ARRAY_AGG(STRUCT('symbol',
			symbol,
			'price',
			price,
			'marketCap',
			market_cap
			)) AS beprices
	FROM (
		SELECT
			symbol,
			TIMESTAMP_TRUNC(time, DAY) AS day_start,
			Max(price) AS price, -- Using MAX() to get one value per 24-hour interval
			MAX(MarketCap) AS market_cap,
			MAX(Volume) AS volume
		FROM (
			SELECT
				ID AS symbol,
				Occurance_Time AS time,
				CAST(AVG(Price) AS FLOAT64) AS price,
				CAST(AVG(MarketCap) AS FLOAT64) AS MarketCap,
				CAST(AVG(Volume) AS FLOAT64) AS Volume,
				ROW_NUMBER() OVER (PARTITION BY ID, TIMESTAMP_TRUNC(Occurance_Time, DAY)
				ORDER BY
					Occurance_Time) AS row_num
			FROM
				api-project-901373404215.digital_assets.Digital_Asset_MarketData c
			WHERE
				Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 366 day)
				AND ID in UNNEST(@coins)
				And MarketCap != 0
				AND Volume != 0
			GROUP BY 
				symbol,
				time
		) AS foo
	WHERE
		row_num = 1 -- Only select the first row within each 24-hour interval
	GROUP BY
		day_start,
		symbol
	ORDER BY
		day_start DESC ) AS fo
	GROUP BY
		day_start
	ORDER BY 
		day_start desc
	`)

	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "coins",
			Value: categoryPG.Coins,
		},
	}

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("BuildCategoriesHistoricalData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}
	var categories []HistoricalCategories
	for {
		var category HistoricalCategories

		err := it.Next(&category)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		categories = append(categories, category)
	}
	var allCategories []HistoricalCategoriesBQ
	var categoriesResult []HistoricalCategoriesBQ
	var divisor float64 = 0.0
	for _, category := range categories {
		var topGainers []CategoryTopGainer
		var categoryData HistoricalCategoriesBQ
		var totalPriceWeightIndex float64 = 0.0
		var totalMarketCapWeightIndex float64 = 0.0
		for _, priceObject := range category.Prices {
			var assetCat HistoricalCategoriesSlice
			var topGainer CategoryTopGainer

			assetMeta := assetsMetaData[priceObject.Symbol]
			topGainer.MarketCap = bigquery.NullFloat64{Float64: priceObject.MarketCap, Valid: true}
			topGainer.Logo = assetMeta.LogoURL
			topGainer.Symbol = assetMeta.ID
			topGainer.Slug = strings.ToLower(fmt.Sprintf("%s-%s", strings.ReplaceAll(assetMeta.Name, " ", "-"), assetMeta.ID))
			topGainers = append(topGainers, topGainer)

			assetCat.Symbol = priceObject.Symbol
			assetCat.MarketCap = priceObject.MarketCap
			assetCat.Price = priceObject.Price

			// PriceWeight is a value we need can calculate by divided price for an asset by total price for all assets in a category
			// PriceWeight := CheckAndConvertFloat((priceObject.Price / category.TotalPrice24H))
			// MarketCapWeight is a value we need can calculate by divided MarketCap for an asset by total marketcap for all assets in a category
			MarketCapWeight := (priceObject.MarketCap / category.TotalMarketCap24H)
			// totalPriceWeightIndex is Summation for MarketCapWeight multiple by Price for asset
			// totalPriceWeightIndex we need it to calculate totalMarketCapWeightIndex
			totalPriceWeightIndex += MarketCapWeight * assetCat.Price

		}

		// totalMarketCapWeightIndex is a value we need to calculate the Divisor.
		// We need to multiple TotalMarketCap24H for category by totalPriceWeightIndex
		totalMarketCapWeightIndex = (category.TotalMarketCap24H * totalPriceWeightIndex) / category.TotalMarketCap24H
		// Divisor is a value we need to calculate so we can calculate the Index Value from it.
		// To calculate Divisor we need the TotalMarketCap24H for category that will divided by Base Value.
		// The Base Value can be 100 or 1000 to calculate.
		// For our calculation we will use 1000 as base value.
		// Divisor will calculated from the oldest value.
		if divisor == 0 {
			divisor = CheckAndConvertFloat(category.TotalMarketCap24H / 1000)
		}
		// indexValue it will present the index value change for market cap in 24 hour
		// So we can use it to measure the changes in MarketCap value.
		var indexValue float64 = 0.0
		if category.TotalMarketCap24H == 0 {
			indexValue = 0
		} else {
			indexValue = category.TotalMarketCap24H / divisor // this will present the index value change for market cap // MarketCapIndexValue
		}

		categoryData.WeightIndexMarketCap = bigquery.NullFloat64{Float64: CheckAndConvertFloat(totalMarketCapWeightIndex), Valid: true}
		categoryData.WeightIndexPrice = bigquery.NullFloat64{Float64: CheckAndConvertFloat(totalPriceWeightIndex), Valid: true}
		categoryData.Date = bigquery.NullTimestamp{Timestamp: category.Date, Valid: true}
		categoryData.MarketCap24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalMarketCap24H), Valid: true}
		categoryData.Price24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalPrice24H), Valid: true}
		categoryData.Volume24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalVolume24H), Valid: true}
		categoryData.Divisor = bigquery.NullFloat64{Float64: divisor, Valid: true}
		categoryData.MarketCapIndexValue24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(indexValue), Valid: true}
		categoryData.TotalTokens = bigquery.NullInt64{Int64: int64(len(category.Prices)), Valid: true}
		categoryData.ID = categoryPG.ID
		categoryData.Name = categoryPG.Name
		categoryData.LastUpdated = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
		categoryData.CreatedAt = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}

		sort.Slice(topGainers, func(i, j int) bool {
			return topGainers[i].MarketCap.Float64 > topGainers[j].MarketCap.Float64
		})
		topGainersLen := len(topGainers)
		if topGainersLen > 3 {
			topGainersLen = 3
		}
		categoryData.TopGainers = topGainers[0:topGainersLen]
		allCategories = append(allCategories, categoryData)
	}
	SortHistoricalCategories(allCategories)
	for i := 0; i < len(allCategories); i++ {
		y := allCategories[i]
		if i == len(allCategories)-1 {
			y.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
			y.MarketCapPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
			y.IndexPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
		} else {
			y.MarketCapPercentage24H = bigquery.NullFloat64{Float64: CheckValueValidate(ConvertBQFloatToNormalFloat(y.MarketCap24H), ConvertBQFloatToNormalFloat(allCategories[i+1].MarketCap24H)), Valid: true}
			y.IndexPercentage24H = bigquery.NullFloat64{Float64: CheckValueValidate(ConvertBQFloatToNormalFloat(y.WeightIndexPrice), ConvertBQFloatToNormalFloat(allCategories[i+1].WeightIndexPrice)), Valid: true}
			y.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: CheckValueValidate(ConvertBQFloatToNormalFloat(y.MarketCapIndexValue24H), ConvertBQFloatToNormalFloat(allCategories[i+1].MarketCapIndexValue24H)), Valid: true}
		}

		categoriesResult = append(categoriesResult, y)
	}
	log.Info("BuildCategoriesHistoricalData")
	return categoriesResult, nil
}

func CheckAndConvertFloat(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	convValue, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", v), 64)
	return convValue
}

func CheckValueValidate(newValue float64, oldValue float64) float64 {
	indexCalculation := ((newValue - oldValue) / oldValue) * 100
	result := CheckAndConvertFloat(indexCalculation)
	return result
}

func SortHistoricalCategories(categories []HistoricalCategoriesBQ) {
	sort.Slice(categories, func(i, j int) bool {
		return categories[i].Date.Timestamp.After(categories[j].Date.Timestamp)
	})
}

func (bq *BQStore) InsertCategoriesHistoricalDataBQ(ctx0 context.Context, uuid string, allCategories []HistoricalCategoriesBQ) error {
	ctx, span := tracer.Start(ctx0, "InsertCategoriesHistoricalDataBQ")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "InsertCategoriesHistoricalDataBQ"

	startTime := log.StartTimeL(labels, "InsertCategoriesHistoricalDataBQ")

	categoryHistoricalTable := "Categories_historical_data_test"

	bqInserter := bq.Dataset("digital_assets").Table(categoryHistoricalTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, allCategories)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up Categories Historical and retrying insert")
			l := len(allCategories)
			var ticks []HistoricalCategoriesBQ
			ticks = append(ticks, allCategories...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertCategoriesHistoricalDataBQ(ctx, uuid, a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			return retryError
		}
		//if not a 413 error return the error
		return inserterErr

	}

	span.SetStatus(codes.Ok, "InsertCategoryFundamentalsBQ Completed")
	log.EndTimeL(labels, "InsertCategoryFundamentalsBQ", startTime, nil)
	return nil
}

func (bq *BQStore) GetCategoriesHistoricalData(ctx0 context.Context) (map[string]HistoricalCategoriesBQ, error) {
	ctx, span := tracer.Start(ctx0, "GetCategoriesHistoricalData")
	defer span.End()

	log.Debug("GetCategoriesHistoricalData")

	categoriesTableName := "Categories_historical_data_test"

	query := bq.Query(`
	SELECT
		id,
		name,
		total_tokens,
		volume_24h,
		price_24h,
		index_percentage_24h,
		market_cap_24h,
		market_cap_24h_index,
		market_cap_index_percentage_24h,
		market_cap_percentage_24h,
		market_cap_weight_index,
		index_price_24h,
		divisor,
		row_last_updated
	FROM (
		SELECT
		id,
		name,
		total_tokens,
		volume_24h,
		price_24h,
		index_percentage_24h,
		market_cap_24h,
		market_cap_24h_index,
		market_cap_index_percentage_24h,
		market_cap_percentage_24h,
		market_cap_weight_index,
		index_price_24h,
		divisor,
		row_last_updated,
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY row_last_updated DESC) AS row_num
		FROM
		api-project-901373404215.digital_assets.` + categoriesTableName + ` )
	WHERE
		row_num = 1
	ORDER BY
		id ASC
	`)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetAuthorData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	categoriesHistorical := make(map[string]HistoricalCategoriesBQ)

	for {
		var category HistoricalCategoriesBQ
		err := it.Next(&category)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		categoriesHistorical[category.ID] = category

	}

	return categoriesHistorical, nil

}
