# sqlrpc

[![Build Status](https://drone.io/github.com/anacrolix/sqlrpc/status.png)](https://drone.io/github.com/anacrolix/sqlrpc/latest)
[![GoDoc](https://godoc.org/github.com/anacrolix/sqlrpc?status.svg)](https://godoc.org/github.com/anacrolix/sqlrpc)

Package sqlrpc provides an RPC service that exposes a `database/sql.*DB`, and a SQL driver that can connect to it. Any `database/sql.*DB` can be exposed, but the primary motivation is to expose the excellent SQLite3. `cmd/sqlite3server` is provided for this purpose, and can easily be adapted to other DBs.

## cmd/sqlite3server

Serves RPC on :6033 by default (MySQL's default port 3306, in reverse). `--dsn` passes the data source name as you would to http://godoc.org/github.com/mattn/go-sqlite3#SQLiteDriver.Open

    sqlite3server --dsn=some/sqlite3.db

To connect in Go:

```go
import (
	"database/sql"
	_ "github.com/anacrolix/sqlrpc"
)

func main() {
	db, err := sql.Open("sqlrpc", "localhost:6033")
	...
```
