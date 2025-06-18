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
type PostgresqlUtils interface {
	GetPostgresqlClient() *sql.DB //creates a bigquery client
	PGClose()
}

type postgresqlUtils struct {
	pg           *sql.DB
	DBClientOnce sync.Once
	PGHost       string
	PGDBPort     string
	PGDBUser     string
	PGDBPassword string
	PGDBName     string
	PGDBSSLMode  string
}

// creates a new bigquery utils object
func NewPostgresqlUtils(PGHost string, PGDBPort string, PGDBUser string, PGDBPassword string, PGDBName string, PGDBSSLMode string) PostgresqlUtils {
	return &postgresqlUtils{PGHost: PGHost, PGDBPort: PGDBPort, PGDBUser: PGDBUser, PGDBPassword: PGDBPassword, PGDBName: PGDBName, PGDBSSLMode: PGDBSSLMode}
}

// creates a firestore client and sync it using sync.Once instead of creating it everytime we call the function
func (p *postgresqlUtils) GetPostgresqlClient() *sql.DB {
	if p.pg == nil {

		p.DBClientOnce.Do(func() {

			if p.PGHost == "" || p.PGDBPort == "" || p.PGDBUser == "" || p.PGDBPassword == "" || p.PGDBName == "" || p.PGDBSSLMode == "" {
				log.Critical("could not load postgres dependencies")
				return
			}
			connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", p.PGHost, p.PGDBPort, p.PGDBUser, p.PGDBPassword, p.PGDBName, p.PGDBSSLMode)

			driverName, err := otelsql.Register("postgres",
				otelsql.TraceAll(),
				otelsql.WithDatabaseName(os.Getenv("DB_NAME")),
				otelsql.WithSystem(semconv.DBSystemPostgreSQL),
			)
			if err != nil {
				log.Error("%s", err)
				return
			}

			p.pg, err = sql.Open(driverName, connectionString)

			if err != nil {
				log.Error("%s", err)
				return
			}

			if err := otelsql.RecordStats(p.pg); err != nil {
				return
			}
			maxLifetime := 5 * time.Minute

			p.pg.SetConnMaxLifetime(maxLifetime)
			//pg.SetConnMaxIdleTime(maxLifetime)
			connectionError := p.pg.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
				return
			}
		})
	}
	return p.pg
}

func (p *postgresqlUtils) PGClose() {
	if p.pg != nil {
		p.pg.Close()
	}
}
