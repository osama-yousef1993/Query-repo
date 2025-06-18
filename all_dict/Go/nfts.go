package coingecko

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/codes"
)

type NFTCollectionListOptions struct {
	Order           string `url:"order,omitempty"`             //valid values: h24_volume_native_asc, h24_volume_native_desc, floor_price_native_asc, floor_price_native_desc, market_cap_native_asc, market_cap_native_desc, market_cap_usd_asc, market_cap_usd_desc
	AssetPlatformId string `url:"asset_platform_id,omitempty"` //The ids of the coin, comma separated crytocurrency symbols (base). refers to /coins/list
	Per_Page        int    `url:"per_page,omitempty"`          // valid values: 1..250. Total results per page. Default value : 100
	Page            int    `url:"page,omitempty"`              //Page through results. Default value : 1
	ApiKey          string `url:"x_cg_pro_api_key,omitempty"`  //coingecko api key

}

type NFTList []struct {
	ID              string `json:"id"`
	ContractAddress string `json:"contract_address"`
	Name            string `json:"name"`
	AssetPlatformID string `json:"asset_platform_id"`
	Symbol          string `json:"symbol"`
}

type NFTMarketsList []struct {
	ID                                         string       `json:"id"`
	ContractAddress                            string       `json:"contract_address"`
	AssetPlatformID                            string       `json:"asset_platform_id"`
	Name                                       string       `json:"name"`
	Symbol                                     string       `json:"symbol"`
	Image                                      Image        `json:"image"`
	Description                                string       `json:"description"`
	NativeCurrency                             string       `json:"native_currency"`
	NativeCurrencySymbol                       string       `json:"native_currency_symbol"`
	FloorPrice                                 CurrencyInfo `json:"floor_price"`
	MarketCap                                  CurrencyInfo `json:"market_cap"`
	Volume24H                                  CurrencyInfo `json:"volume_24h"`
	FloorPriceInUsd24HPercentageChange         float64      `json:"floor_price_in_usd_24h_percentage_change"`
	MarketCap24HPercentageChange               CurrencyInfo `json:"market_cap_24h_percentage_change"`
	Volume24HPercentageChange                  CurrencyInfo `json:"volume_24h_percentage_change"`
	NumberOfUniqueAddresses                    float64      `json:"number_of_unique_addresses"`
	NumberOfUniqueAddresses24HPercentageChange float64      `json:"number_of_unique_addresses_24h_percentage_change"`
	TotalSupply                                float64      `json:"total_supply"`
}

// Used in the GetNFTMarket Function.
type NFTMarket struct {
	ID                                         string       `json:"id"`
	ContractAddress                            string       `json:"contract_address"`
	AssetPlatformID                            string       `json:"asset_platform_id"`
	Name                                       string       `json:"name"`
	Symbol                                     string       `json:"symbol"`
	Image                                      Image        `json:"image"`
	Description                                string       `json:"description"`
	NativeCurrency                             string       `json:"native_currency"`
	NativeCurrencySymbol                       string       `json:"native_currency_symbol"`
	FloorPrice                                 CurrencyInfo `json:"floor_price"`
	MarketCap                                  CurrencyInfo `json:"market_cap"`
	Volume24H                                  CurrencyInfo `json:"volume_24h"`
	FloorPriceInUsd24HPercentageChange         float64      `json:"floor_price_in_usd_24h_percentage_change"`
	MarketCap24HPercentageChange               CurrencyInfo `json:"market_cap_24h_percentage_change"`
	Volume24HPercentageChange                  CurrencyInfo `json:"volume_24h_percentage_change"`
	NumberOfUniqueAddresses                    float64      `json:"number_of_unique_addresses"`
	NumberOfUniqueAddresses24HPercentageChange float64      `json:"number_of_unique_addresses_24h_percentage_change"`
	TotalSupply                                float64      `json:"total_supply"`
	Links                                      NFTLinks     `json:"links"`
}
type Image struct {
	Small string `json:"small"`
}
type CurrencyInfo struct {
	NativeCurrency float64 `json:"native_currency"`
	Usd            float64 `json:"usd"`
}

type NFTMarketChartOptions struct {
	Days   string `url:"days,omitempty"`             //Valid values: any integer, e.g. 1, 14, 30 , 90 , … or max
	ApiKey string `url:"x_cg_pro_api_key,omitempty"` //coingecko api key
}

type NFTLinks struct {
	Homepage string `json:"homepage"`
	Twitter  string `json:"twitter"`
	Discord  string `json:"discord"`
}

type NFTMarketChart struct {
	FloorPriceUsd    [][]interface{} `json:"floor_price_usd"`    //Time(int),Float64 or string
	FloorPriceNative [][]interface{} `json:"floor_price_native"` //Time(int),Float64 or string
	H24VolumeUsd     [][]interface{} `json:"h24_volume_usd"`     //Time(int),Float64 or string
	H24VolumeNative  [][]interface{} `json:"h24_volume_native"`  //Time(int),Float64 or string
	MarketCapUsd     [][]interface{} `json:"market_cap_usd"`     //Time(int),Float64 or string
	MarketCapNative  [][]interface{} `json:"market_cap_native"`  //Time(int),Float64 or string
}

/*
	 	Use this to obtain all the NFT ids in order to make API calls, paginated to 100 items
		https://www.coingecko.com/en/api/documentation
*/
func (c *client) GetNFTCollectionList(ctx0 context.Context, opts *NFTCollectionListOptions) (*NFTList, map[string][]string, error) {

	if ctx0 == nil {
		ctx0 = context.Background()
	}

	ctx, span := tr.Start(ctx0, "coingecko.client.GetNFTCollectionList")
	defer span.End()

	var nftList NFTList
	data, resHeaders, err := c.getWithHeaders(ctx, "/nfts/list", opts)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}
	err = c.unmarshal(ctx, data, &nftList)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}

	span.SetStatus(codes.Ok, "success")

	return &nftList, resHeaders, nil
}

/*
Default value: 100 Max value is 250 You can only get up to 250 results per page.
https://apiguide.coingecko.com/exclusive-endpoints/for-paid-plan-subscribers
*/
func (c *client) GetNFTMarketsList(ctx0 context.Context, opts *NFTCollectionListOptions) (*NFTMarketsList, map[string][]string, error) {

	if ctx0 == nil {
		ctx0 = context.Background()
	}

	ctx, span := tr.Start(ctx0, "coingecko.client.GetNFTMarketsList")
	defer span.End()

	var nftList NFTMarketsList
	data, resHeaders, err := c.getWithHeaders(ctx, "/nfts/markets", opts)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}
	err = c.unmarshal(ctx, data, &nftList)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}

	span.SetStatus(codes.Ok, "success")

	return &nftList, resHeaders, nil
}

/*
Get historical market data of a NFT collection, including floor price, market cap, and 24h volume, by number of days away from now.
https://apiguide.coingecko.com/exclusive-endpoints/for-paid-plan-subscribers
*/
func (c *client) GetNFTMarketChart(ctx0 context.Context, opts *NFTMarketChartOptions, nftID string) (*NFTMarketChart, map[string][]string, error) {

	if ctx0 == nil {
		ctx0 = context.Background()
	}

	ctx, span := tr.Start(ctx0, "coingecko.client.GetNFTMarketChart")
	defer span.End()

	var nftList NFTMarketChart
	data, resHeaders, err := c.getWithHeaders(ctx, fmt.Sprintf("/nfts/%s/market_chart", nftID), opts)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}
	err = c.unmarshal(ctx, data, &nftList)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}

	span.SetStatus(codes.Ok, "success")

	return &nftList, resHeaders, nil
}

/*
		Gets NFT market data for a single collection
	 	https://www.coingecko.com/en/api/documentation
*/
func (c *client) GetNFTMarket(ctx0 context.Context, nftID string) (*NFTMarket, map[string][]string, error) {

	if ctx0 == nil {
		ctx0 = context.Background()
	}

	ctx, span := tr.Start(ctx0, "coingecko.client.GetNFTMarketChart")
	defer span.End()

	var nftList NFTMarket
	data, resHeaders, err := c.getWithHeaders(ctx, fmt.Sprintf("/nfts/%s", nftID), nil)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}
	err = c.unmarshal(ctx, data, &nftList)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, err
	}

	span.SetStatus(codes.Ok, "success")

	return &nftList, resHeaders, nil
}





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

go get -u github.com/Forbes-Media/coingecko-client
go get -u github.com/lestrrat-go/jwx