//go:build wireinject
// +build wireinject

// Handles the dependency injection setup for ContentApi using the google/wire framework.
// This file will be ignored by the build system - wire_gen.go will be used instead for final compilation.
//
// ALL DI EDITS SHOULD BE MADE IN THIS FILE ONLY! DO NOT MANUALLY MODIFY wire_gen.go!!!
package di

import (
	"reflect"
	"runtime"

	"github.com/Forbes-Media/Systems-golang-common-libraries/fclient"
	"github.com/Forbes-Media/Systems-golang-common-libraries/fdao"
	"github.com/Forbes-Media/Systems-golang-common-libraries/futils"
	"github.com/Forbes-Media/Systems-web-api/controller"
	"github.com/Forbes-Media/Systems-web-api/dao"
	"github.com/Forbes-Media/Systems-web-api/model"
	"github.com/Forbes-Media/Systems-web-api/service"
	"github.com/Forbes-Media/fum"
	"github.com/google/wire"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// App contains all the top-level dependencies required by the application to run
type App struct {
	model.Config
	*controller.ContentController
	*controller.RecommendController
	*controller.HealthCheckController
	*controller.QotdController
	*controller.EntityController
}

var daoSet = wire.NewSet(
	NewRefHydratorFromDAOs,
	dao.NewAuthorDAO,
	dao.NewBadgeDAO,
	dao.NewContentDAO,
	dao.NewChannelSectionMappingDAO,
	dao.NewMnetCategoriesDAO,
	dao.NewPublicationDAO,
	dao.NewQotdDAO,
	dao.NewEntityDAO,
)

var svcSet = wire.NewSet(
	service.NewRecommendService,
	service.NewContentService,
	service.NewQotdService,
	service.NewEntityService,
)

var controllerSet = wire.NewSet(
	controller.NewHealthCheckController,
	controller.NewRecommendController,
	controller.NewContentController,
	controller.NewQotdController,
	controller.NewEntityController,
)

var apiClientSet = wire.NewSet(
	NewForbesApiClientFromConfig,
)

func InitializeApp() *App {
	wire.Build(
		NewConfFromEnv,
		NewContentsMongoClient,
		daoSet,
		svcSet,
		controllerSet,
		apiClientSet,
		wire.Struct(new(App), "*"),
	)
	return nil
}

// =============================================================================
// BELOW ARE UTILITIES FOR CONFIGURING DEPENDENCY INJECTION
// =============================================================================

func NewConfFromEnv() model.Config {
	env := futils.PopulateFromEnvFiles[model.Config]("../vault/secrets/app", "vault/secrets/app", "/vault/secrets/app", ".env", "../.env")
	env.UserAgent = "WebApi/" + runtime.Version()
	return env
}

func NewContentsMongoClient(conf model.Config) *mongo.Client {
	host := "mongodb+srv://" + conf.DbContentsHost + "/?ssl=true"
	return fdao.NewMongoClient(conf.DbContentsUser, conf.DbContentsPass, host, readpref.Primary(), false)
}

func NewRefHydratorFromDAOs(
	authorDAO dao.AuthorDAO,
	badgeDAO dao.BadgeDAO,
	pubDAO dao.PublicationDAO,
) fdao.RefHydrator {
	return fdao.NewRefHydrator(map[reflect.Type]any{
		reflect.TypeOf(&fum.Author{}):      authorDAO,
		reflect.TypeOf(&fum.Badge{}):       badgeDAO,
		reflect.TypeOf(&fum.Publication{}): pubDAO,
	})
}

func NewForbesApiClientFromConfig(conf model.Config) fclient.ForbesApiClient {
	return fclient.NewForbesApiClient(conf.UserAgent, conf.ForbesApiHost)
}
