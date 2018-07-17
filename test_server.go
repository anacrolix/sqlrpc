// build test
package sqlrpc

import (
	"database/sql"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type testServer struct {
	*Server
	L net.Listener
}

func (me testServer) Close() {
	me.DB.Close()
	me.L.Close()
}

func (me testServer) NewClient(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlrpc", me.L.Addr().String())
	require.NoError(t, err)
	return db
}

func startServer(t testing.TB) testServer {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	server := &Server{}
	server.Service.DB = db
	require.NoError(t, server.RpcServer.RegisterName("SQLRPC", &server.Service))
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	go http.Serve(l, &server.RpcServer)
	return testServer{server, l}
}
