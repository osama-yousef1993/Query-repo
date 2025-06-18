// This package is responsible for exposing access to stored daata
//
// Repository is responsible for eposing functions that interact with the following
//   - Databases
//   - Third Party APIS
package repository

/*
	DAO - DATA ACCESS OBJECT
	Responsible for Providing access to Database Queries

*/
import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/repository/common/cloudUtils"
	"github.com/Forbes-Media/crypto-backend-api/repository/common/databaseUtils"

	"github.com/Forbes-Media/go-tools/log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DAO interface {
	// All Functions should end with Query
	NewMemberInfoQuery() MemberInfoQuery
	NewConfigurationQuery() ConfigurationQuery
	UpdateConfigurations(context.Context) error
	GetCachedGrants() *[]datastruct.ConfiguredGrant
	NewMemberReportQuery() MemberReportQuery
}

var (
	BQProjectID = "api-project-901373404215"
	bqUtils     = cloudUtils.NewBigqueryUtils("api-project-901373404215")
	fsUtils     = cloudUtils.NewFirestoreUtils()
	pgUtils     = databaseUtils.NewPostgresqlUtils(pgHost, pgDBPort, pgDBUser, pgDBPassword, pgDBName, pgDBSSLMode)
	mysqlUtils  = databaseUtils.NewMySqlUtils(mysqlHost, mysqlDBPort, mysqlDBUser, mysqlDBPassword, mysqlDBName)
)

var (
	DBClientOnce sync.Once
)

var (
	pgHost       = os.Getenv("DB_HOST")
	pgDBPort     = os.Getenv("DB_PORT")
	pgDBUser     = os.Getenv("DB_USER")
	pgDBPassword = os.Getenv("DB_PASSWORD")
	pgDBName     = os.Getenv("DB_NAME")
	pgDBSSLMode  = os.Getenv("DB_SSLMODE")
)
var (
	mysqlHost       = os.Getenv("MYSQL_HOST")
	mysqlDBPort     = os.Getenv("MYSQL_PORT")
	mysqlDBUser     = os.Getenv("MYSQL_USER")
	mysqlDBPassword = os.Getenv("MYSQL_PASSWORD")
	mysqlDBName     = os.Getenv("MYSQL_DB")
)

type dao struct {
	GrantConfig []datastruct.ConfiguredGrant
}

// returns a dao interface
func NewDao() DAO {

	var dao = &dao{}
	dao.UpdateConfigurations(context.Background()) // tiries to update the configuration of the dao durin instantiation
	return dao
}

// UpdateConfigurations Is responsible for updating any caches that contain configuration information
//
// Takes a context.Context
// Runs a query to update the local cache which contains the configuration cache.
// Returns an error if somethng goes wrong.
func (dao *dao) UpdateConfigurations(ctx context.Context) error {

	//loads dependent configurations when createing a new object
	var configQuery = &configurationQuery{}
	grantConfiguration, err := configQuery.GetGrantsConfiguration(ctx)
	if err != nil {
		log.Critical("could not find promotional grants")
		return errors.New("could not find promotional grants")
	}
	dao.GrantConfig = *grantConfiguration
	return nil
}

// returns cahed grants
func (dao *dao) GetCachedGrants() *[]datastruct.ConfiguredGrant {
	return &dao.GrantConfig
}

// Returns a new memberInfoQuery object that implements the MemberInfoQuery Interface
// Contains all queries in rgarts to updating, and getting information about a member
func (dao *dao) NewMemberInfoQuery() MemberInfoQuery {
	return &memberinfoQuery{}
}

// Returns a new memberReportQuery object that implements the MemberReportQuery Interface
// Contains all queries in rgarts to getting member info and store it in BQ table.
func (dao *dao) NewMemberReportQuery() MemberReportQuery {
	return &memberReportQuery{}
}

func (dao *dao) NewConfigurationQuery() ConfigurationQuery {
	return &configurationQuery{}

}
