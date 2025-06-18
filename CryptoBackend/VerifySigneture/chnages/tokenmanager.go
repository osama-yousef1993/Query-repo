package dto

type JWT struct {
	IAT     int64   `json:"iat,omitempty"`     //unix time
	ISS     string  `json:"iss,omitempty"`     // the entity that issued the toke
	SUB     string  `json:"sub,omitempty"`     // the entity that the token is issued to
	Grants  []Grant `json:"grants,omitempty"`  // users granted services
	AUD     string  `json:"aud,omitempty"`     // the audiance that token related to
	SIG     string  `json:"sig,omitempty"`     // the signature for the wallet
	ADDR    string  `json:"addr,omitempty"`    // the Wallet address generate from public key
	Exp     int     `json:"exp,omitempty"`     // the expiration time that token will destroy after it
	Message string  `json:"message,omitempty"` // the message that hold the time for the token
}

type Grant struct {
	GrantId    string `json:"grant_id"`        //id of the promotion ex:fin_cry
	Expiration int64  `json:"expiration_date"` //expiration date of the grant in unix time
}

// https://magic.link/docs/api/server-side-sdks/go#getissuer
// Results provied from the magic sdk token functions
type MagicDIDValidationResults struct {
	IsDIDValid    bool   `json:"isDIDValid"`    // a boolean set to true or false based on results from magic sdk
	IsAuthorized  bool   `json:"isAuthorized"`  // a boolean set to true or false based on results from magic sdk
	WalletAddress string `json:"walletAddress"` // a wallet address which is extracted fromthe did token
	Issuer        string `json:"issuer"`        // an issuer id this is the users decentralized id
}
