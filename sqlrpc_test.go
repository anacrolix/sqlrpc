package sqlrpc

import (
	"database/sql"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"testing"
	"time"

	_ "github.com/anacrolix/envpprof"
	"github.com/bradfitz/iter"
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

func TestDatabaseLocked(t *testing.T) {
	db, _ := sql.Open("sqlrpc", serverAddr)
	// db.SetMaxOpenConns(1)
	defer db.Close()
	db.Exec("create table a(b)")
	tx, _ := db.Begin()
	// Lock the database.
	tx.Exec("insert into a values (42)")
	// Release the database asynchronously.
	go func() {
		time.Sleep(10 * time.Millisecond)
		tx.Commit()
	}()
	started := time.Now()
	var err error
	var retries int
	for retries = range iter.N(100) {
		// This won't succeed until the other transaction releases its lock.
		_, err = db.Exec("update a set b=b+1")
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	t.Log(time.Since(started))
	t.Logf("retries: %d", retries)
	if err != nil {
		// Database lock should have been released by now.
		t.Fatalf("%#v", err)
	}
	var b int
	db.QueryRow("select b from a").Scan(&b)
	assert.EqualValues(t, 43, b)
	assert.Nil(t, db.Close())
}
