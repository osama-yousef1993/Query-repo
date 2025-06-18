package datastruct

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type TimeSeriesResultPG struct {
	Symbol                  string      `json:"symbol" firestore:"symbol" postgres:"symbol"  bigquery:"symbol"`                                    // Symbol of chart
	TargetResolutionSeconds int         `json:"targetResolutionSeconds" postgres:"target_resolution_seconds" bigquery:"target_resolution_seconds"` // resolution seconds of chart interval
	Slice                   []SlicePG   `firestore:"be-prices" postgres:"be-prices" bigquery:"beprices"`                                           // Array of data that contains the price and time data for specific interval
	FESlice                 []FESlicePG `json:"prices" firestore:"prices" postgres:"prices"`                                                       // Array of data that contains the price and time data for specific interval
	IsIndex                 bool        `json:"isIndex" postgres:"is_index" bigquery:"is_index"`                                                   // it's bool value to set the row is index
	Source                  string      `json:"source" postgres:"source" bigquery:"source"`                                                        // it determine the source of the data
	Interval                string      `json:"interval" postgres:"tm_interval" bigquery:"interval"`                                               // it determine the interval for the data
	Status                  string      `json:"status" postgres:"status"`                                                                          // Status of the asset EX: active/inactive
	Notice                  string      `json:"notice"`                                                                                            // Used to To Notify FE with unexpected chart changes. EX there 24hr chart displaying 2 days worth of trade data
	Period                  string      `json:"period"`                                                                                            // The Period to build the data for ex: 24h, 7d ..
	AssetType               string      `json:"assetType"`                                                                                         // The type this chart data belong to ex: FT, NFT or CATEGORY
}
type SlicePG struct {
	Time             time.Time `json:"Time" firestore:"x" postgres:"Time" bigquery:"Time"`
	AvgClose         float64   `json:"Price" firestore:"y" postgres:"Price" bigquery:"Price"`
	FloorPriceNative float64   `json:"floorprice_usd" firestore:"floorprice_usd" postgres:"floorprice_native" bigquery:"floorpricenative"`    //for NFT Table
	MarketCapNative  float64   `json:"marketCap_native" firestore:"marketCap_native" postgres:"marketCap_native" bigquery:"marketCap_native"` //for NFT Table
	MarketCapUSD     float64   `json:"marketCap_usd" firestore:"marketCap_usd" postgres:"marketCap_usd" bigquery:"marketCap_usd"`             //for NFT Table and Category Table
	VolumeNative     float64   `json:"volume_native" firestore:"volume_native" postgres:"volume_native" bigquery:"volume_native"`             //for NFT Table
	VolumeUSD        float64   `json:"volume_usd" firestore:"volume_usd" postgres:"volume_usd" bigquery:"volume_usd"`                         //for NFT Table
}

type FESlicePG struct {
	Time      time.Time `json:"x" firestore:"x" postgres:"x"`
	AvgClose  float64   `json:"y" firestore:"y" postgres:"y"`
}

type SlicePGResult []SlicePG

func (c SlicePGResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *SlicePGResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}
