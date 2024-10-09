package gomigratesqlitex

import (
	"context"
	"crawshaw.io/sqlite/sqlitex"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"path/filepath"
	"testing"
)

var testDbName = "sqlitex.db"

func Test(t *testing.T) {
	dir := t.TempDir()

	t.Logf("DB path : %s\n", filepath.Join(dir, testDbName))
	p := &SqlitexDriver{}

	addr := fmt.Sprintf("sqlitex://%s", filepath.Join(dir, testDbName))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}

	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
	err = d.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, testDbName))

	db, err := sqlitex.Open(filepath.Join(dir, testDbName), 0, 10)
	if err != nil {
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			return
		}
	}()
	driver, err := WithInstance(db, &Config{})
	if err != nil {
		t.Fatal(err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"ql", driver)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)
}

func TestMigrationTable(t *testing.T) {
	dir := t.TempDir()

	t.Logf("DB path : %s\n", filepath.Join(dir, testDbName))

	db, err := sqlitex.Open(filepath.Join(dir, testDbName), 0, 10)
	if err != nil {
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			return
		}
	}()

	config := &Config{
		MigrationsTable: "my_migration_table",
	}
	driver, err := WithInstance(db, config)
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"ql", driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("UP")
	err = m.Up()
	if err != nil {
		t.Fatal(err)
	}

	conn := db.Get(context.TODO())
	defer db.Put(conn)
	stmt := conn.Prep(fmt.Sprintf("SELECT count(*) as num FROM %s", config.MigrationsTable))
	_, err = sqlitex.ResultInt(stmt)
	if err != nil {
		t.Fatal(err)
	}

}
