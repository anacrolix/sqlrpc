package main

import (
	"database/sql"
	"flag"
	"log"
	"net/http"
	"net/rpc"

	_ "github.com/anacrolix/envpprof"
	_ "github.com/mattn/go-sqlite3"

	"github.com/anacrolix/sqlrpc"
)

func main() {
	dsn := flag.String("dsn", "", "sqlite3 dsn")
	flag.Parse()
	db, err := sql.Open("sqlite3", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	// db.SetMaxOpenConns(1)
	s := sqlrpc.Server{DB: db}
	rpc.Register(&s)
	rpc.HandleHTTP()
	http.ListenAndServe(":6033", nil)
}
