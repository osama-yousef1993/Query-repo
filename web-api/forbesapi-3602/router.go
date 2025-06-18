package router

import (
	"github.com/Forbes-Media/Systems-web-api/di"
	"github.com/gin-gonic/gin"
	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"
)

func SetupRouter(app *di.App) *gin.Engine {
	if app.Config.Env == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())

	webApiRoutes := r.Group("/webapi")
	webApiRoutes.GET("/swagger/*any", ginswagger.WrapHandler(swaggerfiles.Handler))

	app.ContentController.RegisterRoutes(webApiRoutes.Group(""))
	app.HealthCheckController.RegisterRoutes(webApiRoutes.Group(""))
	app.RecommendController.RegisterRoutes(webApiRoutes.Group(""))
	app.QotdController.RegisterRoutes(webApiRoutes.Group(""))
	app.EntityController.RegisterRoutes(webApiRoutes.Group(""))

	return r
}
