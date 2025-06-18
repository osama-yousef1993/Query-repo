package dto

type GetEntityInfoDTO struct {
	// Entity natural ID
	NaturalID string `json:"naturalId" form:"naturalId"`
	// Entity type
	Type string `json:"type" form:"type"`
	// The number of results to limit to
	Limit int `json:"limit" form:"limit"`
	// list url
	ListUri string `json:"listUri" form:"listUri"`
}
