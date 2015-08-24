// Package sqlrpc provides an RPC interface, and corresponding client and
// server implementations for a `database/sql.*DB`, and a SQL driver that
// wraps the the RPC client. Any `database/sql.*DB` can be exposed through the
// server, but the primary motivation is to expose the excellent SQLite3.
// `cmd/sqlite3server` is provided for this purpose, and can easily be adapted
// to other DBs.
package sqlrpc

type ExecArgs struct {
	StmtRef int
	Values  []interface{}
}

type CommitArgs struct {
	TxID interface{}
}

type RowsReply struct {
	Columns []string
	RowsId  int
}

type RowsNextArgs struct {
	RowsRef   int
	NumValues int
}

type RowsNextReply struct {
	Values []interface{}
	EOF    bool
}

type ResultReply struct {
	LastInsertId    int64
	LastInsertIdErr error
	RowsAffected    int64
	RowsAffectedErr error
}

type PrepareArgs struct {
	Query string
	TxId  int
	InTx  bool
}
