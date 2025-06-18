package databaseUtils

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Forbes-Media/go-tools/log"
	"go.nhat.io/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Bigquery utils contains functions that helps interact with Bigquery databases
type MySqlUtils interface {
	GetMySqlClient() *sql.DB //creates a bigquery client
	MySqlClose()
}

type mySqlUtils struct {
	mysql           *sql.DB
	DBClientOnce    sync.Once
	MySqlHost       string
	MySqlDBPort     string
	MySqlDBUser     string
	MySqlDBPassword string
	MySqlDBName     string
}

// creates a new bigquery utils object
func NewMySqlUtils(MySqlHost string, MySqlDBPort string, MySqlDBUser string, MySqlDBPassword string, MySqlDBName string) MySqlUtils {
	return &mySqlUtils{MySqlHost: MySqlHost, MySqlDBPort: MySqlDBPort, MySqlDBUser: MySqlDBUser, MySqlDBPassword: MySqlDBPassword, MySqlDBName: MySqlDBName}
}

// creates a firestore client and sync it using sync.Once instead of creating it everytime we call the function
func (m *mySqlUtils) GetMySqlClient() *sql.DB {
	if m.mysql == nil {

		m.DBClientOnce.Do(func() {

			if m.MySqlHost == "" || m.MySqlDBPort == "" || m.MySqlDBUser == "" || m.MySqlDBPassword == "" || m.MySqlDBName == "" {
				log.Critical("could not load mysql dependencies")
				return
			}
			connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", m.MySqlDBUser, m.MySqlDBPassword, m.MySqlHost, m.MySqlDBPort, m.MySqlDBName)

			driverName, err := otelsql.Register("mysql",
				otelsql.TraceAll(),
				otelsql.WithDatabaseName(os.Getenv("DB_MYSQL_NAME")),
				otelsql.WithSystem(semconv.DBSystemMySQL),
			)
			if err != nil {
				log.Error("%s", err)
				return
			}

			m.mysql, err = sql.Open(driverName, connectionString)

			if err != nil {
				log.Error("%s", err)
				return
			}

			if err := otelsql.RecordStats(m.mysql); err != nil {
				return
			}
			maxLifetime := 5 * time.Minute

			m.mysql.SetConnMaxLifetime(maxLifetime)
			//MySql.SetConnMaxIdleTime(maxLifetime)
			connectionError := m.mysql.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
				return
			}
		})
	}
	return m.mysql
}

func (m *mySqlUtils) MySqlClose() {
	if m.mysql != nil {
		m.mysql.Close()
	}
}
