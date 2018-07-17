package sqlrpc

import (
	"context"
	"database/sql"
	"log"
	"math"
	"testing"
	"time"

	_ "github.com/anacrolix/envpprof"
	"github.com/bradfitz/iter"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
}

// Shows that transactions tie up a connection. The server.DB should have a
// connection limit of 1.
func TestConcurrentTransactionsSQLite(t *testing.T) {
	server := startServer(t)
	defer server.Close()
	started := time.Now()
	tx1, _ := server.DB.Begin()
	t.Log(time.Since(started))
	go func() {
		time.Sleep(10 * time.Millisecond)
		tx1.Rollback()
	}()
	started = time.Now()
	tx2, _ := server.DB.Begin()
	tx2.Rollback()
	took := time.Since(started)
	t.Log(took)
	if took < 10*time.Millisecond {
		t.Fatal("transactions did not lock each other out")
	}
}

func TestPing(t *testing.T) {
	server := startServer(t)
	defer server.Close()
	db, err := sql.Open("sqlrpc", server.L.Addr().String())
	require.NoError(t, err)
	defer db.Close()
	err = db.Ping()
	require.NoError(t, err)
}

func TestSimple(t *testing.T) {
	server := startServer(t)
	defer server.Close()
	db, err := sql.Open("sqlrpc", server.L.Addr().String())
	require.NoError(t, err)
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
	assert.Equal(t, 0, len(server.Refs.GetAll()))
}

// Try variations of {Exec,Query}{,Context} with and without NamedArgs.
func TestSimpleNamedContext(t *testing.T) {
	server := startServer(t)
	defer server.Close()
	db, err := sql.Open("sqlrpc", server.L.Addr().String())
	require.NoError(t, err)
	defer db.Close()
	db.Exec("drop table if exists test")
	_, err = db.ExecContext(context.Background(), "create table test(universe)")
	require.NoError(t, err)
	res, err := db.Exec("insert into test values(:n)", sql.Named("n", 42))
	require.NoError(t, err)
	ra, _ := res.RowsAffected()
	assert.EqualValues(t, 1, ra)
	var answer int
	row := db.QueryRowContext(context.Background(), "select * from test")
	err = row.Scan(&answer)
	require.NoError(t, err)
	assert.EqualValues(t, 42, answer)
	res, err = db.ExecContext(context.Background(), "insert into test values(?)", 42)
	require.NoError(t, err)
	ra, _ = res.RowsAffected()
	assert.EqualValues(t, 1, ra)
	assert.Equal(t, 0, server.Refs.Len())
}

func TestTransactionSingleConnection(t *testing.T) {
	server := startServer(t)
	defer server.Close()
	db, _ := sql.Open("sqlrpc", server.L.Addr().String())
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
	server := startServer(t)
	defer server.Close()
	db, _ := sql.Open("sqlrpc", server.L.Addr().String())
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
	assert.Equal(t, 0, server.Refs.Len())
}

func Benchmark(b *testing.B) {
	server := startServer(b)
	defer server.Close()
	db, _ := sql.Open("sqlrpc", server.L.Addr().String())
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
	assert.Equal(b, 0, server.Refs.Len())
}

func TestMaxIntTimerDuration(t *testing.T) {
	tr := time.AfterFunc(math.MaxInt64, func() {
		t.Fatal("timer fired")
	})
	time.Sleep(time.Millisecond)
	require.True(t, tr.Stop())
}

func TestUnregisteredValue(t *testing.T) {
	server := startServer(t)
	defer server.Close()
	db := server.NewClient(t)
	defer db.Close()
	_, err := db.Exec("create table a(b datetime)")
	require.NoError(t, err)
	now := time.Now()
	_, err = db.Exec("insert into a values(?)", now)
	require.NoError(t, err)
	var v time.Time
	err = db.QueryRow("select * from a").Scan(&v)
	require.NoError(t, err)
}
