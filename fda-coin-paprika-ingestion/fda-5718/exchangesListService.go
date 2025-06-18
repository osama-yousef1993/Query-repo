package services

import "github.com/Forbes-Media/fda-coin-paprika-ingestion/repository"

// ExchangesListService is a service that handles the exchanges list build process.
type ExchangesListService interface {
	BuildExchangesList() error
}
type exchangesListService struct {
	dao repository.DAO
}

func NewExchangesListService(dao repository.DAO) ExchangesListService {
	return &exchangesListService{dao: dao}
}

func (e *exchangesListService) BuildExchangesList() error {

	return nil
}
