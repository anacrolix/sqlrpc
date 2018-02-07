package sqlrpc

import (
	"database/sql"

	"github.com/anacrolix/sqlrpc/refs"
)

const logRefs = false

type ref struct {
	sqlObj interface{}
}

type RefId = refs.Id

type Server struct {
	DB *sql.DB

	Refs refs.Manager

	// RPC
	Service
}

func (me *Server) db() *sql.DB {
	return me.DB
}
