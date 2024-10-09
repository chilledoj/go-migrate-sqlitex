# Go Migrate SqliteX Driver

`sqlitex://path/to/database?query`

This is a SQLite driver for [Golang-Migrate](https://github.com/golang-migrate/migrate). It uses the [crawshaw/sqlite](https://github.com/crawshaw/sqlite) package (and generally the `sqlitex` sub-package commands) to connect to the DB.

This is generally an adaptation of the existing [sqlite3](https://github.com/golang-migrate/migrate/tree/master/database/sqlite3) implementation in the golang-migrate package.

It also uses the same additional query parameters, which are optional.

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table.  Defaults to `schema_migrations`. |

## Notes

Unlike the `sqlite3` implementation, the migrations are not wrapped in a transaction or savepoint.