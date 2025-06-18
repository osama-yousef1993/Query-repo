package controller

import (
	"github.com/Forbes-Media/Systems-golang-common-libraries/fginutils"
	"github.com/Forbes-Media/Systems-web-api/model/dto"
	"github.com/Forbes-Media/Systems-web-api/service"
	"github.com/gin-gonic/gin"
)

type EntityController struct {
	entityService service.EntityService
}

var _ fginutils.Controller = &EntityController{}

func NewEntityController(entityService service.EntityService) *EntityController {
	return &EntityController{entityService}
}

func (ec *EntityController) RegisterRoutes(r *gin.RouterGroup) {
	entity := r.Group("/entities")
	entity.GET("/related-info", ec.getEntityRelatedInfoHandler)
}

func (ec *EntityController) getEntityRelatedInfoHandler(c *gin.Context) {
	var reqDto dto.GetEntityInfoDTO
	if err := c.ShouldBindQuery(&reqDto); err != nil {
		fginutils.Abort(c, err)
		return
	}

	infoCard, err := ec.entityService.GetEntityInfo(reqDto)
	if err != nil {
		fginutils.AbortBadRequest(c, err)
		return
	}

	c.Render(200, fginutils.WithJSON2(gin.H{"infoCard": infoCard}))
}
