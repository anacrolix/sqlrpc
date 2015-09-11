package sqlrpc

import (
	"database/sql"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"testing"
	"time"

	_ "github.com/anacrolix/envpprof"
	"github.com/bradfitz/iter"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	serverAddr string
	service    *Service
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	rpc.HandleHTTP()
	backendDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	backendDB.SetMaxOpenConns(1)
	service = &Service{
		DB:     backendDB,
		Expiry: time.Second,
	}
	err = rpc.Register(service)
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

// Shows that transactions tie up a connection. The server.DB should have a
// connection limit of 1.
func TestConcurrentTransactionsSQLite(t *testing.T) {
	started := time.Now()
	tx1, _ := service.DB.Begin()
	t.Log(time.Since(started))
	go func() {
		time.Sleep(10 * time.Millisecond)
		tx1.Rollback()
	}()
	started = time.Now()
	tx2, _ := service.DB.Begin()
	tx2.Rollback()
	took := time.Since(started)
	t.Log(took)
	if took < 10*time.Millisecond {
		t.Fatal("transactions did not lock each other out")
	}
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
	require.NoError(t, err)
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
	assert.Equal(t, 0, len(service.refs))
}

func TestTransactionSingleConnection(t *testing.T) {
	db, _ := sql.Open("sqlrpc", serverAddr)
	defer db.Close()
	db.Exec("drop table if exists a")
	tx, _ := db.Begin()
	tx.Exec("create table a(b)")
	tx.Exec("insert into a values(?)", 1)
	t.Log(time.Now())
	row := tx.QueryRow("select * from a where b < ?", 2)
	var i int
	err := row.Scan(&i)
	t.Log(time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 1, i)
	tx.Exec("insert into a values(?)", 2)
	rows, err := tx.Query("select b from a where b > ?", 0)
	require.NoError(t, err)
	cols, _ := rows.Columns()
	require.EqualValues(t, []string{"b"}, cols)
	require.True(t, rows.Next())
	rows.Scan(&i)
	require.EqualValues(t, 1, i)
	require.True(t, rows.Next())
	rows.Scan(&i)
	require.EqualValues(t, 2, i)
	require.False(t, rows.Next())
	tx.Rollback()
}

func TestDatabaseLocked(t *testing.T) {
	db, _ := sql.Open("sqlrpc", serverAddr)
	defer db.Close()
	_, err := db.Exec("create table a(b)")
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	// Lock the database.
	tx.Exec("insert into a values (42)")
	// Release the database asynchronously.
	go func() {
		time.Sleep(10 * time.Millisecond)
		tx.Commit()
	}()
	started := time.Now()
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
	rows, err := db.Query("select b from a")
	assert.Nil(t, err)
	cols, err := rows.Columns()
	assert.Nil(t, err)
	assert.EqualValues(t, []string{"b"}, cols)
	for rows.Next() {
		assert.Nil(t, rows.Scan(&b))
	}
	assert.Nil(t, rows.Err())
	rows.Close()
	assert.EqualValues(t, 43, b)
	assert.Nil(t, db.Close())
	time.Sleep(12 * time.Millisecond)
	service.mu.Lock()
	assert.Equal(t, 0, len(service.refs))
	service.mu.Unlock()
}

func Benchmark(b *testing.B) {
	db, _ := sql.Open("sqlrpc", serverAddr)
	defer db.Close()
	db.Exec("drop table if exists a")
	db.Exec("create table a(b)")
	for range iter.N(b.N) {
		for i := range iter.N(10) {
			db.Exec("insert into a values (?)", i)
		}
		rows, _ := db.Query("select * from a where b < ?", 3)
		var count int
		for rows.Next() {
			var b int
			rows.Scan(&b)
			if b < 3 {
				count++
			}
		}
		assert.Nil(b, rows.Err())
		assert.EqualValues(b, 3, count)
		rows.Close()
		db.Exec("delete from a")
	}
	assert.Equal(b, 0, len(service.refs))
}

func TestExpires(t *testing.T) {
	db, _ := sql.Open("sqlrpc", serverAddr)
	defer db.Close()
	db.Exec("drop table if exists a")
	db.Exec("create table a(b)")
	db.Exec("insert into a default values")
	rows, _ := db.Query("select * from a where b < ?", 3)
	time.Sleep(time.Second)
	assert.False(t, rows.Next())
	// Rows handle should be expired.
	require.Error(t, rows.Err())
}

func TestMaxIntTimerDuration(t *testing.T) {
	tr := time.AfterFunc(math.MaxInt64, func() {
		t.Fatal("timer fired")
	})
	time.Sleep(time.Millisecond)
	require.True(t, tr.Stop())
}
