package datastruct

import (
	"fmt"
	"os"
)

var TrendingCryptoCollectionName = fmt.Sprintf("%strending_crypto", os.Getenv("ROWY_PREFIX"))
var TrendingNFTCollectionName = fmt.Sprintf("%strending_nfts", os.Getenv("ROWY_PREFIX"))

// We will use it with FT and NFT tending data that will be mapping from BQ
type TrendingAssets struct {
	Slug  string `json:"slug" bigquery:"slug" firestore:"slug"`         // It provided Slug value for FT or NFT
	Order int    `json:"order" bigquery:"order_slug" firestore:"order"` // It provided the order for slug by page views
}

// It will be using to mapping data from PG for NFT data
type NFT struct {
	Id   string `postgresql:"id"`   // It provided the id for nft
	Slug string `postgresql:"slug"` // It provided Slug value for NFT
}

// This query will returns the Pag views for Assets
var TrendingCryptoQuery = `
SELECT
  REGEXP_EXTRACT(GA_pagePath, r'/digital-assets/assets/([^/]+)') AS slug,
  ROW_NUMBER() OVER(ORDER BY SUM(GA_pageViews) desc) as order_slug
FROM
  DataMart.v_DataMart
WHERE
  GA_pagePath LIKE '%/digital-assets/assets/%'
GROUP BY
  1
ORDER BY
  2 asc
limit 15
`

// This query will returns the Pag views for NFTs
var TrendingNFTQuery = `
SELECT
    REPLACE(REGEXP_EXTRACT(events_data_mart.GA_eventLabel, r'nftprices{([^/]+)}'), ' ', '-') AS slug,
    ROW_NUMBER() OVER(ORDER BY COALESCE(SUM(CASE WHEN  events_data_mart.GA_eventAction   = "subscribesuccess"  AND 'yes' = 'yes' THEN events_data_mart.hits
              WHEN  events_data_mart.GA_eventAction   != "subscribesuccess"  THEN events_data_mart.hits
              ELSE 0
              END  ), 0) desc) as order_slug
FROM api-project-901373404215.DataMart.v_events_datamart
     AS events_data_mart
WHERE ((UPPER(( events_data_mart.GA_eventAction  )) = UPPER('click'))) AND ((UPPER(( events_data_mart.GA_eventCategory  )) = UPPER('forbes digital assets'))) AND ((UPPER(( events_data_mart.GA_eventLabel  )) LIKE UPPER('%nftprices{%}')))
GROUP BY
    1
ORDER BY
    2 asc
limit 15
`
