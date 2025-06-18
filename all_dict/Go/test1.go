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
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
r.Handle("/consume-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeCategories))).Methods(http.MethodPost)



func ConsumeCategories(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeCategories")
	defer span.End()

	var (
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 20)
		mu           = &sync.Mutex{}
		categories   []coingecko.CategoriesData
		assetsMap    = make(map[string][]string)
	)

	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeCategories")
	var maxRetries = 3
RETRY:
	cgRateLimiter.Wait(limiterContext)
	categoriesData, err := c.GetCategoriesData(ctx)
	if err != nil {
		log.EndTimeL(labels, "Error getting Categories Data: %s", startTime, err)
		if maxRetries > 0 {
			maxRetries--
			goto RETRY
		}
		w.WriteHeader(http.StatusInternalServerError)
	}
	// get all market for each category
	for i := 0; i < len(categoriesData); i++ {
		// var marketData []coingecko.CoinsMarketData
		category := categoriesData[i]
		throttleChan <- true
		wg.Add(1)
		go func(categoryId string) {
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			data, err := c.GetCoinsMarketData(ctx, &coingecko.CoinsMarketOptions{VSCurrency: "usd", Category: categoryId, Page: 1, Per_Page: 250})
			addToTotalCalls(ctx)
			if err != nil {
				log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
				if maxRetries > 0 {
					maxRetries--
					goto RETRY
				}
				log.DebugL(labels, "Retrying call for assets with category ID %s . Attempt #%v ", categoryId, maxRetries)
				<-throttleChan
				wg.Done()
				return
			}
			mu.Lock()
			// marketData = append(marketData, *data...)
			category.Markets = *data
			assetsMap, err = BuildCategoryAssets(ctx, assetsMap, category.Markets, categoryId)
			if err != nil {
				log.ErrorL(labels, "Exchange Metadata weren't stored in postgres at time %s, error %v", startTime, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Unlock()
			categories = append(categories, category)
			<-throttleChan
			wg.Done()

		}(category.ID)
	}

	file, _ := json.MarshalIndent(assetsMap, " ", "")
	_ = ioutil.WriteFile("map.json", file, 0644)

	// Upsert Categories with all it's data
	store.UpsertCoinGeckoCategoriesData(ctx, categories)
	// Update the tags in  coingecko_asset_metadata
	store.UpdateAssetsMetaData(ctx, assetsMap)

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeCategories", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func BuildCategoryAssets(ctx0 context.Context, assetsMap map[string][]string, markets []coingecko.CoinsMarketData, categoryId string) (map[string][]string, error) {
	_, span :=  tracer.Start(ctx0, "BuildCategoryAssets")
	defer span.End()
	for _, asset := range markets {
		assetsMap[asset.ID] = append(assetsMap[asset.ID], categoryId)
	}
	return assetsMap, nil
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

CREATE  TYPE coingecko_categories as (
    id TEXT,
	name TEXT,
	market_cap FLOAT,
	market_cap_change_24h FLOAT,
	content TEXT,
	top_3_coins VARCHAR(100)[],
	volume_24h FLOAT,
	markets JSON,
	last_updated TIMESTAMPTZ
);

CREATE OR REPLACE PROCEDURE UpsertCoinGeckoCategoriesData(IN categories_data coingecko_categories[])
AS 
$BODY$
DECLARE
    category_data coingecko_categories;
BEGIN
    FOREACH category_data in ARRAY categories_data LOOP 
        INSERT INTO coingecko_categories(id, name, market_cap, market_cap_change_24h, content, top_3_coins, volume_24h, markets, last_updated)
        VALUES (category_data.ID, category_data.Name, category_data.MarketCap, category_data.MarketCapChange24H, category_data.Content, category_data.Top3Coins, category_data.Volume24H, category_data.Markets, category_data.UpdatedAt)
        on conflict (id) DO UPDATE SET id=EXCLUDED.id, name=EXCLUDED.name, market_cap=EXCLUDED.market_cap, market_cap_change_24h=EXCLUDED.market_cap_change_24h, content=EXCLUDED.content, 
		top_3_coins=EXCLUDED.top_3_coins, volume_24h=EXCLUDED.volume_24h, markets=EXCLUDED.markets, last_updated = EXCLUDED.last_updated;
    END LOOP;
END;
$BODY$ LANGUAGE plpgsql;

CREATE TABLE coingecko_categories (
	"id" TEXT,
	"name" TEXT,
	"market_cap" FLOAT,
	"market_cap_change_24h" FLOAT,
	"content" TEXT,
	"top_3_coins" VARCHAR(500)[],
	"volume_24h" FLOAT,
	"markets" JSON,
	"last_updated" TIMESTAMPTZ DEFAULT (Now()),
	primary key ("id")
);


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// if we don't need to add CGExchangesTickers to PG this function will be removed
func UpsertCoinGeckoCategoriesData(ctx0 context.Context, categoriesData []coingecko.CategoriesData) error {
	ctx, span := tracer.Start(ctx0, "UpsertCoinGeckoCategoriesData")
	defer span.End()

	pg := PGConnect()
	categoriesDataTMP := categoriesData
	valueString := make([]string, 0, len(categoriesData))
	valueArgs := make([]interface{}, 0, len(categoriesData)*9)
	tableName := "coingecko_categories"
	var i = 0
	span.AddEvent("Starting Inserting Data to Categories Data to PG")
	for y := 0; y < len(categoriesDataTMP); y++ {
		var category = categoriesDataTMP[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9)
		valueString = append(valueString, valString)

		valueArgs = append(valueArgs, category.ID)
		valueArgs = append(valueArgs, category.Name)
		valueArgs = append(valueArgs, category.MarketCap)
		valueArgs = append(valueArgs, category.MarketCapChange24H)
		valueArgs = append(valueArgs, category.Content)
		valueArgs = append(valueArgs, pq.Array(category.Top3Coins))
		valueArgs = append(valueArgs, category.Volume24H)
		markets, _ := json.Marshal(category.Markets)
		valueArgs = append(valueArgs, markets)
		valueArgs = append(valueArgs, category.UpdatedAt)
		i++
		
		if len(valueArgs) >= 65000 || y == len(categoriesDataTMP)-1 {
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "on conflict (id) DO UPDATE SET id=EXCLUDED.id, name=EXCLUDED.name, market_cap=EXCLUDED.market_cap, market_cap_change_24h=EXCLUDED.market_cap_change_24h, content=EXCLUDED.content, top_3_coins=EXCLUDED.top_3_coins, volume_24h=EXCLUDED.volume_24h, markets=EXCLUDED.markets, last_updated = EXCLUDED.last_updated"
			query := insertStatementCoins + updateStatement

			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("Insertion error %v", inserterError)
				span.SetStatus(codes.Error, "Error Inserting Categories")
			}

			valueString = make([]string, 0, len(categoriesDataTMP))
			valueArgs = make([]interface{}, 0, len(categoriesDataTMP)*9)

			i = 0
		}
	}
	return nil
}

func UpdateAssetsMetaData(ctx0 context.Context, assetsData map[string][]string) error {
	ctx, span := tracer.Start(ctx0, "UpsertCoinGeckoCategoriesData")
	defer span.End()

	pg := PGConnect()
	tableName := "coingecko_asset_metadata"
	span.AddEvent("Starting Updating Tags for Assets Metadata   PG")
	for key, value := range assetsData {
		updateStatement := fmt.Sprintf("UPDATE %s SET tags = $1 WHERE id = $2",tableName)
		_, inserterError := pg.ExecContext(ctx, updateStatement, pq.Array(value), key)
		if inserterError != nil {
			log.Error("Insertion error %v", inserterError)
			span.SetStatus(codes.Error, "Error Updating Assets Metadata")
		}
	}
	return nil
}




// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
dynamicDominanceData, err := store.GetDynamicDescriptionDominanceData(ctx, labels)
if err != nil {
	log.DebugL(labels, "BuildDynamicDescription Data returns Empty from PG, error %v", err)
	span.SetStatus(codes.Error, err.Error())
	return nil, err
}
dynamicDescription.Dominance.BTC.MarketCapPercentage = data.Data.MarketCapPercentage["btc"]
dynamicDescription.Dominance.BTC.Name = dynamicDominanceData["btc"].Name
dynamicDescription.Dominance.BTC.Slug = dynamicDominanceData["btc"].Slug
dynamicDescription.Dominance.ETH.MarketCapPercentage = data.Data.MarketCapPercentage["eth"]
dynamicDescription.Dominance.ETH.Name = dynamicDominanceData["eth"].Name
dynamicDescription.Dominance.ETH.Slug = dynamicDominanceData["eth"].Slug



type Dominance struct {
	BTC DominanceAssetsData `json:"btc" postgres:"btc_dominance"`
	ETH DominanceAssetsData `json:"eth" postgres:"eth_dominance"`
}

type DominanceAssetsData struct {
	MarketCapPercentage float64 `json:"market_cap_percentage" postgres:"market_cap_percentage"`
	Name                string  `json:"name" postgres:"name"`
	Slug                string  `json:"slug" postgres:"slug"`
	DisplaySymbol       string  `json:"display_symbol" postgres:"display_symbol"`
}


// Get Dynamic Description for Traded Assets Page
func GetDynamicDescriptionDominanceData(ctx0 context.Context, labels map[string]string) (map[string]models.DominanceAssetsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "DynamicDescription")
	defer span.End()
	startTime := log.StartTime("Dynamic Description Data")

	pg := PGConnect()

	// Will MAp the Global Data from Coingecko
	// var assetsData []models.DominanceAssetsData
	assetsData := make(map[string]models.DominanceAssetsData)

	queryResult, err := pg.QueryContext(ctx, `
		select 
			name, 
			slug,
			display_symbol
		from 
			fundamentalslatest
		where 
			display_symbol in ('btc', 'eth')
		order by 
			name asc 
		limit 2
		`)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTimeL(labels, "Dynamic Description Data Query", startTime, err)
		span.SetStatus(codes.Error, "Dynamic Description Data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var assetData models.DominanceAssetsData
		err := queryResult.Scan(&assetData.Name, &assetData.Slug, &assetData.DisplaySymbol)

		if err != nil {
			log.EndTimeL(labels, "Dynamic Description Data Query", startTime, err)
			span.SetStatus(codes.Error, "Dynamic Description Data Scan error")
			return nil, err
		}
		assetsData[assetData.DisplaySymbol] =  assetData

	}
	return assetsData, nil
}





type Dominance struct {
	BTC DominanceAssetsData `json:"btc" postgres:"btc_dominance"`
	ETH DominanceAssetsData `json:"eth" postgres:"eth_dominance"`
}

type DominanceAssetsData struct {
	MarketCapPercentage float64 `json:"market_cap_percentage" postgres:"market_cap_percentage"`
	Name                string  `json:"name" postgres:"name"`
	Slug                string  `json:"slug" postgres:"slug"`
}