package datastruct

type ChangeLog struct {
	CurrencyID string `json:"currency_id" postgresql:"currency_id"` // Coin ID from coin Paprika
	OldID      string `json:"old_id" postgresql:"old_id"`           // Old Coin Id from CoinPaprika
	NewID      string `json:"new_id" postgresql:"new_id"`           // New Coin Id from CoinPaprika
	ChangedAt  string `json:"changed_at" postgresql:"changed_at"`   // When this Coin ID was changed
}
