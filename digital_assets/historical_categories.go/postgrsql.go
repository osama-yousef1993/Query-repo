
// takes an array of category fundamentals data and upserts it to the Category fundamentals table in Postgres.
func UpsertCategoryFundamentalsPG(ctx0 context.Context, allFundamentals *[]CategoryFundamental, labels map[string]string) error {

	ctx, span := tracer.Start(ctx0, "Upsert Category Fundamentals")
	defer span.End()

	startTime := log.StartTime("Upsert Category Fundamentals")

	pg := PGConnect()
	fundamentals := *allFundamentals

	valueString := make([]string, 0, len(fundamentals))
	valueArgs := make([]interface{}, 0, len(fundamentals)*14)
	var i = 0

	tableName := "categories_fundamentals_test"

	for y := 0; y < len(fundamentals); y++ {
		var f = fundamentals[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*14+1, i*14+2, i*14+3, i*14+4, i*14+5, i*14+6, i*14+7, i*14+8, i*14+9, i*14+10, i*14+11, i*14+12, i*14+13, i*14+14)
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, f.ID)
		valueArgs = append(valueArgs, f.Name)
		valueArgs = append(valueArgs, f.TotalTokens.Int64)
		valueArgs = append(valueArgs, f.IndexPercentage24H.Float64)
		valueArgs = append(valueArgs, f.Volume24H.Float64)
		valueArgs = append(valueArgs, f.Price24H.Float64)
		valueArgs = append(valueArgs, f.MarketCap.Float64)
		valueArgs = append(valueArgs, f.WeightIndexPrice.Float64)
		valueArgs = append(valueArgs, f.WeightIndexMarketCap.Float64)
		valueArgs = append(valueArgs, f.MarketCapIndexValue24H.Float64)
		valueArgs = append(valueArgs, f.MarketCapIndexPercentage24H.Float64)
		valueArgs = append(valueArgs, f.Divisor.Float64)
		topGainers, _ := json.Marshal(f.TopGainers)
		valueArgs = append(valueArgs, topGainers)
		valueArgs = append(valueArgs, f.LastUpdated)

		i++

		if len(valueArgs) >= 65000 || y == len(fundamentals)-1 {
			upsertStatement := " ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, total_tokens = EXCLUDED.total_tokens, index_percentage_24h = EXCLUDED.index_percentage_24h, volume_24h = EXCLUDED.volume_24h, price_24h = EXCLUDED.price_24h, market_cap = EXCLUDED.market_cap, price_weight_index = EXCLUDED.price_weight_index, market_cap_weight_index = EXCLUDED.market_cap_weight_index, market_cap_index_value_24h = EXCLUDED.market_cap_index_value_24h, market_cap_index_percentage_24h = EXCLUDED.market_cap_index_percentage_24h, divisor = EXCLUDED.divisor, top_gainers = EXCLUDED.top_gainers, last_updated= EXCLUDED.last_updated;"
			insertStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), upsertStatement)
			latencyTimeStart := time.Now()
			_, inserterError := pg.ExecContext(ctx, insertStatement, valueArgs...)
			latency := time.Since(latencyTimeStart)

			log.InfoL(labels, fmt.Sprintf("Upsert Category Fundamentals : time to insert %dms", latency.Milliseconds()))

			if inserterError != nil {
				log.ErrorL(labels, fmt.Sprintf("UpsertCategoryFundamentals TimeElapsed: %fs", latency.Seconds()), inserterError)
				log.EndTime("Upsert Category Fundamentals", startTime, inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(fundamentals))
			valueArgs = make([]interface{}, 0, len(fundamentals)*9)
			i = 0
		}
	}
	log.EndTime("Upsert Category Fundamentals", startTime, nil)

	return nil
}



type Categories struct {
	ID          string   `json:"id" postgres:"id"`                     // ID of the category
	Name        string   `json:"name" postgres:"name"`                 // ID of the category
	TotalTokens int      `json:"total_tokens" postgres:"total_tokens"` // ID of the category
	Coins       []string `json:"coins" postgres:"coins"`               // Top 3 coins in the category                           // List of all the assets in the category
}

func GetCategories(ctx0 context.Context) ([]Categories, error) {
	pg := PGConnect()

	ctx, span := tracer.Start(ctx0, "GetCategories")

	defer span.End()
	startTime := log.StartTime("Get Categories Query")

	var categories []Categories
	span.AddEvent("Start Getting Categories")
	queryResult, err := pg.QueryContext(ctx, `
	select 
		id, 
		name, 
		count(markets ->> 'id') as total_tokens, 
		array_agg(markets ->> 'id') as coins
	from (
			select json_array_elements(markets) as markets, id, name
			from coingecko_categories
		) as foo

	where id = 'layer-1' 
	group by id, name
	`)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetCategories")
		log.EndTime("Get Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var category Categories
		err := queryResult.Scan(&category.ID, &category.Name, &category.TotalTokens, pq.Array(&category.Coins))
		if err != nil {
			span.SetStatus(codes.Error, "PGGetCategories scan error")
			log.EndTime("Get Categories Query", startTime, err)
			return nil, err
		}
		categories = append(categories, category)
	}
	log.EndTime("Get Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return categories, nil
}




type TopAsset struct {
	Symbol    string  `json:"symbol" postgres:"symbol"`
	MarketCap float64 `json:"market_cap" postgres:"market_cap"`
}

func GetTopAssets(ctx0 context.Context) (map[string]TopAsset, error) {
	pg := PGConnect()

	ctx, span := tracer.Start(ctx0, "GetCategories")

	defer span.End()
	startTime := log.StartTime("Get Categories Query")

	topAssets := make(map[string]TopAsset)
	span.AddEvent("Start Getting Categories")
	queryResult, err := pg.QueryContext(ctx, `
	select 
		symbol, 
		market_cap 
	from 
		fundamentalslatest
	where market_cap is not null
	order by market_cap desc
	limit 100
	`)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetCategories")
		log.EndTime("Get Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var topAsset TopAsset
		err := queryResult.Scan(&topAsset.Symbol, &topAsset.MarketCap)
		if err != nil {
			span.SetStatus(codes.Error, "PGGetCategories scan error")
			log.EndTime("Get Categories Query", startTime, err)
			return nil, err
		}
		topAssets[topAsset.Symbol] = topAsset
	}
	log.EndTime("Get Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return topAssets, nil
}

//" ON CONFLICT (id) DO UPDATE SET 
// 	id = EXCLUDED.id,
//  name = EXCLUDED.name,
//  total_tokens = EXCLUDED.total_tokens,
//  percentage_24h = EXCLUDED.percentage_24h,
//  volume_24h = EXCLUDED.volume_24h,
//  price_24h = EXCLUDED.price_24h,
//  average_price = EXCLUDED.average_price,
//  market_cap = EXCLUDED.market_cap,
//  market_cap_percentage_24h = EXCLUDED.market_cap_percentage_24h,
//  index_price_24h = EXCLUDED.index_price_24h,
//  market_cap_weight_index = EXCLUDED.market_cap_weight_index,
//  index_market_cap_24h = EXCLUDED.index_market_cap_24h,
//  index_market_cap_percentage_24h = EXCLUDED.index_market_cap_percentage_24h,
//  divisor = EXCLUDED.divisor,
//  top_gainers = EXCLUDED.top_gainers,
//  last_updated= EXCLUDED.last_updated;
SELECT 
	id,
	name,
	total_tokens,
	percentage_24h,
	volume_24h,
	price_24h,
	average_price,
	market_cap,
	market_cap_percentage_24h,
	price_weight_index,
	market_cap_weight_index,
	index_market_cap_24h,
	index_market_cap_percentage_24h,
	divisor,
	top_gainers,
	last_updated
FROM public.categories_fundamentals_test;