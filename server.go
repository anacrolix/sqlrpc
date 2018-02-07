package sqlrpc

import (
	"database/sql"
	"net/rpc"

	"github.com/anacrolix/sqlrpc/refs"
)

const logRefs = false

type ref struct {
	sqlObj interface{}
}

type RefId = refs.Id

type Server struct {
	// RPC
	Service
	RpcServer rpc.Server
}

func (me *Server) db() *sql.DB {
	return me.DB
}
