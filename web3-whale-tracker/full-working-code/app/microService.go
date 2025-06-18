package app

import "github.com/Forbes-Media/web3-whale-tracker/services"

type Microservices struct {
	transactions services.TransactionsService
}

// Instantiates a new microservice objet, which currently only takes one microServices
// takes a watchlistService and returns a new microservice object.
// Add more services here
func NewMicroservices(
	transactions services.TransactionsService,

) (*Microservices, error) {

	ms := Microservices{
		transactions: transactions,
	}

	return &ms, nil

}
