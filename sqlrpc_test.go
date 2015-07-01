package sqlrpc

import (
	"database/sql"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"testing"

	_ "github.com/anacrolix/envpprof"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var serverAddr string

func init() {
	rpc.HandleHTTP()
	backendDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	s := Server{DB: backendDB}
	err = rpc.Register(&s)
	if err != nil {
		log.Fatal(err)
	}
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatal(err)
	}
	serverAddr = l.Addr().String()
	go http.Serve(l, nil)
}

func TestPing(t *testing.T) {
	db, err := sql.Open("sqlrpc", serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSimple(t *testing.T) {
	db, err := sql.Open("sqlrpc", serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("create table test(universe)")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Exec("insert into test values(?)", 42)
	if err != nil {
		t.Fatal(err)
	}
	ra, _ := res.RowsAffected()
	assert.EqualValues(t, 1, ra)
	var answer int
	row := db.QueryRow("select * from test")
	err = row.Scan(&answer)
	if err != nil {
		t.Fatal(err)
	}
	assert.EqualValues(t, 42, answer)
	res, err = db.Exec("insert into test values(?)", 42)
	if err != nil {
		t.Fatal(err)
	}
	ra, _ = res.RowsAffected()
	assert.EqualValues(t, 1, ra)
}
