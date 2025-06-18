// education.go
// this function work correctly to get only 12 articles only from all sections 
func GetTop12ArticlesFromLearnSection(sections []Section) []EducationArticle {
	var latestArticles []EducationArticle
	var length int
	sectionsLen := len(sections)

	if sectionsLen > 1 {
		minLength := len(sections[0].Articles)
		maxLength := len(sections[0].Articles)
		maxLengthIndex := 0
		for index, section := range sections[1:sectionsLen] {
			SortArticles(sections[index].Articles, false)
			artLength := len(section.Articles)
			if artLength < minLength {
				minLength = artLength
			} else if artLength > maxLength {
				maxLengthIndex = index
				maxLength = artLength
			}
		}
		for i := 0; i < minLength; i++ {
			for j := 0; j < sectionsLen; j++ {
				latestArticles = append(latestArticles, sections[j].Articles[i])
				if len(latestArticles) >= 12 {
					goto END
				}
			}
		}
		for i := minLength; i < maxLength; i++ {
			latestArticles = append(latestArticles, sections[maxLengthIndex].Articles[i])
			if len(latestArticles) >= 12 {
				goto END
			}
		}

	} else {
		SortArticles(sections[0].Articles, false)
		articlesLength := len(sections[0].Articles)
		if articlesLength > 12 {
			length = 12
		} else {
			length = articlesLength
		}
		latestArticles = append(latestArticles, sections[0].Articles[0:length]...)
	}
END:
	return latestArticles

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

//nft changes
v1.HandleFunc("/nft-chains", GetNFTChains).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/tradedNFT", GetTradedNFT).Methods(http.MethodGet, http.MethodOptions)

func GetNFTChains(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetNFTChains")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildVideos"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Assets Calculator Data")

	data, err := store.GetNFTChains(ctx)
	if data == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)

	}
	span.SetStatus(codes.Ok, "GetNFTChains")
	log.EndTimeL(labels, "BuildVideos ", startTime, nil)
	w.WriteHeader(200)
	w.Write(data)

}

func GetTradedNFT(w http.ResponseWriter, r *http.Request) {
	startTime := store.StartTime("Get Traded NFT Data")
	// updated each 5 minute
	setResponseHeaders(w, 300)
	limit := html.EscapeString(r.URL.Query().Get("limit"))
	pageNum := html.EscapeString(r.URL.Query().Get("pageNum"))
	sortBy := html.EscapeString(r.URL.Query().Get("sortBy"))
	direction := html.EscapeString(r.URL.Query().Get("direction"))
	chain_id := html.EscapeString(r.URL.Query().Get("chain_id"))

	lim, err := strconv.Atoi(limit)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	pg, err := strconv.Atoi(pageNum)

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	result, err := store.PGGetTradedNFT(r.Context(), lim, pg, sortBy, direction, chain_id)

	if result == nil && err == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	reader := bytes.NewReader(result)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	store.ConsumeTime("Get Traded NFT Data", startTime, nil)
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

// firestore
type NFTChain struct {
	ID   string `json:"id" firestore:"id"`
	Name string `json:"name" firestore:"name"`
}

func GetNFTChains(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNFTChains")
	defer span.End()

	var nftChains []NFTChain

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "nft_chains")
	// Get the Global Description and the Lists Section from firestore
	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting NFT Chains Data from FS")

	for {
		var nftChain NFTChain
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&nftChain)
		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		nftChains = append(nftChains, nftChain)
	}

	jsonData, err := BuildJsonResponse(ctx, nftChains, "NFT Chains Data")

	if err != nil {
		log.Error("Error Converting NFT Chains to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

// postgres

type TradedNFT struct {
	ID                                         string  `json:"id" postgres:"id"`
	ContractAddress                            string  `json:"contract_address" postgres:"contract_address"`
	AssetPlatformId                            string  `json:"asset_platform_id" postgres:"asset_platform_id"`
	Name                                       string  `json:"name" postgres:"name"`
	Symbol                                     string  `json:"symbol" postgres:"symbol"`
	Image                                      string  `json:"image" postgres:"image"`
	Description                                string  `json:"description" postgres:"description"`
	NativeCurrency                             string  `json:"native_currency" postgres:"native_currency"`
	FloorPriceUsd                              float64 `json:"floor_price_usd" postgres:"floor_price_usd"`
	MarketCapUsd                               float64 `json:"market_cap_usd" postgres:"market_cap_usd"`
	Volume24hUsd                               float64 `json:"volume_24h_usd" postgres:"volume_24h_usd"`
	FloorPriceNative                           float64 `json:"floor_price_native" postgres:"floor_price_native"`
	MarketCapNative                            float64 `json:"market_cap_native" postgres:"market_cap_native"`
	Volume24hNative                            float64 `json:"volume_24h_native" postgres:"volume_24h_native"`
	FloorPriceInUsd24hPercentageChange         float64 `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`
	NumberOfUniqueAddresses                    int     `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`
	NumberOfUniqueAddresses24hPercentageChange float64 `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"`
	TotalSupply                                float64 `json:"total_supply" postgres:"total_supply"`
	Slug                                       string  `json:"slug" postgres:"slug"`
	WebsiteUrl                                 string  `json:"website_url" postgres:"website_url"`
	TwitterUrl                                 string  `json:"twitter_url" postgres:"twitter_url"`
	DiscordUrl                                 string  `json:"discord_url" postgres:"discord_url"`
	LastUpdated                                string  `json:"last_updated" postgres:"last_updated"`
	FullCount                                  *int    `postgres:"full_count"`
}

type TradedNFTResp struct {
	NFT                   []TradedNFT `json:"nft"`
	Total                 int         `json:"total"`
	HasTemporaryDataDelay bool        `json:"hasTemporaryDataDelay"`
	Source                string      `json:"source"`
}


func PGGetTradedNFT(ctx0 context.Context, lim int, pageNum int, sortBy string, direction string, chain_id string) ([]byte, error) {

	ctx, span := tracer.Start(ctx0, "PGGetTradedAssets")
	defer span.End()

	switch sortBy {
	case "price":
		sortBy = "floor_price_usd"
	case "market":
		sortBy = "market_cap_usd"
	case "percentage":
		sortBy = "floor_price_in_usd_24h_percentage_change"
	case "name":
		sortBy = "name"
	default:
		sortBy = "volume_24h_usd"
	}

	startTime := log.StartTime("Pagination Query")
	var nfts []TradedNFT

	pg := PGConnect()
	query := fmt.Sprintf(`SELECT 
							id,
							contract_address,
							asset_platform_id,
							name,
							symbol,
							image,
							description,
							native_currency,
							floor_price_usd,
							market_cap_usd,
							volume_24h_usd,
							floor_price_native,
							market_cap_native,
							volume_24h_native,
							floor_price_in_usd_24h_percentage_change,
							number_of_unique_addresses,
							number_of_unique_addresses_24h_percentage_change,
							total_supply,
							slug,
							website_url,
							twitter_url,
							discord_url,
							last_updated,
							full_count
						FROM 
							PUBLIC.nft_chains_filter(%d,%d,'%s','%s','%s')`, lim, pageNum-1, sortBy, direction, chain_id) //The frontend starts at 1, while the query will consider the pagenum always has to subtract 1
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTime("Pagination Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft TradedNFT
		err := queryResult.Scan(&nft.ID, &nft.ContractAddress, &nft.AssetPlatformId, &nft.Name, &nft.Symbol, &nft.Image, &nft.Description, &nft.NativeCurrency, &nft.FloorPriceUsd, &nft.MarketCapUsd, &nft.Volume24hUsd, &nft.FloorPriceNative, &nft.MarketCapNative, &nft.Volume24hNative, &nft.FloorPriceInUsd24hPercentageChange,&nft.NumberOfUniqueAddresses, &nft.NumberOfUniqueAddresses24hPercentageChange, &nft.TotalSupply, &nft.Slug, &nft.WebsiteUrl, &nft.TwitterUrl, &nft.DiscordUrl, &nft.LastUpdated, &nft.FullCount)
		if err != nil {
			log.EndTime("Pagination Query", startTime, err)
			return nil, err
		}
		nfts = append(nfts, nft)
	}

	var resp = TradedNFTResp{Source: data_source, Total: *nfts[0].FullCount, NFT: nfts}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	log.EndTime("Pagination Query", startTime, nil)
	return jsonData, nil
}

// schema
CREATE OR REPLACE FUNCTION nft_chains_filter(lim integer, pageNum int, sortBy text, direction text, chain_id text)
returns Table(
	id TEXT,
	contract_address TEXT,
	asset_platform_id TEXT,
	name TEXT,
	symbol TEXT,
	image TEXT,
	description TEXT,
	native_currency TEXT ,
	floor_price_usd FLOAT,
	market_cap_usd FLOAT,
	volume_24h_usd FLOAT,
	floor_price_native FLOAT,
	market_cap_native FLOAT,
	volume_24h_native FLOAT,
	floor_price_in_usd_24h_percentage_change FLOAT,
	number_of_unique_addresses INTEGER,
	number_of_unique_addresses_24h_percentage_change FLOAT,
	total_supply FLOAT,
	slug TEXT,
	website_url TEXT,
	twitter_url TEXT,
	discord_url TEXT,
	last_updated TIMESTAMPTZ,
	full_count bigint
)
AS $$
DECLARE where_stat Text := '';

BEGIN
	If chain_id = '' Then
		where_stat = FORMAT('asset_platform_id != ''%s''', chain_id);
	END IF;
	If chain_id != '' Then
		where_stat = FORMAT('asset_platform_id = ''%s''', chain_id);
	END IF;
  RETURN QUERY EXECUTE FORMAT(
      'SELECT 
            id,
            contract_address,
            asset_platform_id,
            name,
            symbol,
            image,
            description,
            native_currency,
            floor_price_usd,
            market_cap_usd,
            volume_24h_usd,
            floor_price_native,
            market_cap_native,
            volume_24h_native,
            floor_price_in_usd_24h_percentage_change,
            number_of_unique_addresses,
            number_of_unique_addresses_24h_percentage_change,
            total_supply,
            slug,
            website_url,
            twitter_url,
            discord_url,
            last_updated,
      count(id) OVER() AS full_count
      FROM 
          public.nftdatalatest
      where 
        %s
      order by %s %s NULLS LAST 
      limit %s offset %s',
                          where_stat,
                          sortBy,
                          direction,
                          lim,
                          lim*pageNum
                          ) USING sortBy,direction,lim,pageNum;
END
$$ LANGUAGE plpgsql;