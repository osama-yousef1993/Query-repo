type BearerToken struct {
	ISS    string    `json:"iss"`    // the entity that issued the toke
	SUB    string    `json:"sub"`    // the entity that the token is issued to
	AUD    string    `json:"aud"`    // the entity that the token is issued to
	SIG    string    `json:"sig"`    // the signature
	ADDR   string    `json:"addr"`   // the eth address
	Wallet string    `json:"wallet"` // the entity that the token is issued to
	Exp    time.Time `json:"exp"` // the entity that the token is issued to
}

type Signature struct {
	Signature string `json:"signature"`
}
