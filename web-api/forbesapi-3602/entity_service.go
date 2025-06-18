package service

import (
	"github.com/Forbes-Media/Systems-web-api/dao"
	"github.com/Forbes-Media/Systems-web-api/model/dto"
	"github.com/Forbes-Media/fum"
)

type EntityService interface {
	GetEntityInfo(params dto.GetEntityInfoDTO) (*fum.EntityInbox, error)
}

type entityServiceImpl struct {
	entityDAO dao.EntityDAO
}

func NewEntityService(entityDAO dao.EntityDAO) EntityService {
	return &entityServiceImpl{entityDAO: entityDAO}
}

func (e *entityServiceImpl) GetEntityInfo(params dto.GetEntityInfoDTO) (*fum.EntityInbox, error) {
	var (
		infoCard *fum.EntityInbox
		entity   *fum.Entity
		image    string
	)
	
}
