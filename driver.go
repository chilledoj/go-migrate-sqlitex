package gomigratesqlitex

import (
	"context"
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"io"
	nurl "net/url"
	"strings"
	"sync/atomic"
)

var _ database.Driver = (*SqlitexDriver)(nil)

func init() {
	database.Register("sqlitex", &SqlitexDriver{})
}

var DefaultMigrationsTable = "schema_migrations"
var (
	//ErrDatabaseDirty  = fmt.Errorf("database is dirty")
	ErrNilConfig = fmt.Errorf("no config")
	//ErrNoDatabaseName = fmt.Errorf("no database name")
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
}

func WithInstance(pool *sqlitex.Pool, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	driver := &SqlitexDriver{
		pool:   pool,
		config: config,
	}

	if err := driver.ensureVersionTable(); err != nil {
		return nil, err
	}

	return driver, nil
}

func (s *SqlitexDriver) ensureVersionTable() error {
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (version uint64, dirty bool);
CREATE UNIQUE INDEX IF NOT EXISTS version_unique ON %s (version);
`, s.config.MigrationsTable, s.config.MigrationsTable)

	conn := s.pool.Get(context.TODO())
	defer s.pool.Put(conn)

	if err := sqlitex.ExecScript(conn, query); err != nil {
		return err
	}
	return nil
}

type SqlitexDriver struct {
	pool     *sqlitex.Pool
	isLocked atomic.Bool
	config   *Config
}

func (s *SqlitexDriver) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}
	dbfile := strings.Replace(migrate.FilterCustomQuery(purl).String(), "sqlitex://", "", 1)

	pool, err := sqlitex.Open(dbfile, 0, 10)
	if err != nil {
		return nil, err
	}

	qv := purl.Query()

	migrationsTable := qv.Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	driver, err := WithInstance(pool, &Config{
		MigrationsTable: migrationsTable,
		DatabaseName:    purl.Path,
	})
	if err != nil {
		return nil, err
	}

	return driver, nil
}

func (s *SqlitexDriver) Close() error {
	return s.pool.Close()
}

func (s *SqlitexDriver) Lock() error {
	if !s.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (s *SqlitexDriver) Unlock() error {
	if !s.isLocked.CompareAndSwap(true, false) {
		return errors.New("not unlocked")
	}
	return nil
}

func (s *SqlitexDriver) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	query := string(migr[:])

	conn := s.pool.Get(nil)
	defer s.pool.Put(conn)

	return sqlitex.ExecScript(conn, query)
}

func (s *SqlitexDriver) SetVersion(version int, dirty bool) error {
	conn := s.pool.Get(nil)
	defer s.pool.Put(conn)

	qry := "DELETE FROM " + s.config.MigrationsTable

	if err := sqlitex.ExecScript(conn, qry); err != nil {
		return database.Error{
			Query:   []byte(qry),
			OrigErr: err,
		}
	}

	if version >= 0 || (version == database.NilVersion && dirty) {
		qry := fmt.Sprintf("INSERT INTO %s (version,dirty) VALUES (?,?)", s.config.MigrationsTable)

		err := sqlitex.Exec(conn, qry, nil, int64(version), dirty)
		if err != nil {
			return database.Error{
				Query:   []byte(qry),
				OrigErr: err,
			}
		}

	}

	return nil
}

func (s *SqlitexDriver) Version() (int, bool, error) {
	conn := s.pool.Get(nil)
	defer s.pool.Put(conn)
	query := "SELECT version, dirty FROM " + s.config.MigrationsTable + " LIMIT 1"

	version := -1
	dirty := false

	if err := sqlitex.Exec(conn, query, func(stmt *sqlite.Stmt) error {

		version = stmt.ColumnInt(0)
		dirtyInt := stmt.ColumnInt(1)
		if dirtyInt == 1 {
			dirty = true
		} else {
			dirty = false
		}

		return nil
	}); err != nil {

		return 0, false, database.Error{
			Line:    1,
			Query:   []byte(query),
			OrigErr: err,
		}
	}
	return version, dirty, nil
}

func (s *SqlitexDriver) Drop() error {
	query := `SELECT name FROM sqlite_master WHERE type = 'table';`
	conn := s.pool.Get(nil)
	defer s.pool.Put(conn)

	tableNames := make([]string, 0)

	err := sqlitex.Exec(conn, query, func(stmt *sqlite.Stmt) error {
		tableName := stmt.ColumnText(0)
		if len(tableName) == 0 {
			tableNames = append(tableNames, tableName)
		}
		return nil
	})

	if err != nil {
		return err
	}

	if len(tableNames) > 0 {
		for _, t := range tableNames {
			query := "DROP TABLE " + t
			err := sqlitex.ExecScript(conn, query)
			if err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
		query := "VACUUM"
		err := sqlitex.ExecScript(conn, query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}
