package app

import (
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/services"
)

// Microservices acts as a container for all service dependencies within the application.
// It provides a centralized way to access various services.
type Microservices struct {
	coinListService      services.CoinListService      // Service responsible for coin-related operations.
	exchangesListService services.ExchangesListService // Service responsible for exchanges-related operations.
}

// NewMicroservices initializes the Microservices container with the given dependencies.
// Parameters:
//   - coinListService: The CoinListService implementation to be used.
//
// Returns:
//   - A pointer to the initialized Microservices struct.
//   - An error if any required dependency is invalid or fails to initialize.
func NewMicroservices(
	coinListService services.CoinListService,
	exchangesListService services.ExchangesListService,
) (*Microservices, error) {
	// Initialize the Microservices container.
	ms := &Microservices{
		coinListService:      coinListService,
		exchangesListService: exchangesListService,
	}

	return ms, nil
}
