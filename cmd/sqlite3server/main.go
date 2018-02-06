package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"time"

	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/tagflag"
	_ "github.com/mattn/go-sqlite3"

	"github.com/anacrolix/sqlrpc"
)

func refsHandler(s *sqlrpc.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for ref, val := range s.Refs() {
			fmt.Fprintf(w, "%d: %#v\n\n", ref, val)
		}
	})
}

func main() {
	log.SetFlags(log.Flags() | log.Llongfile)
	flags := struct {
		DSN            string
		Addr           string
		DbMaxOpenConns int
	}{
		Addr:           "localhost:6033",
		DbMaxOpenConns: 1,
	}
	tagflag.Parse(&flags)
	db, err := sql.Open("sqlite3", flags.DSN)
	if err != nil {
		log.Fatalf("error opening database: %s", err)
	}
	db.SetMaxOpenConns(flags.DbMaxOpenConns)
	s := sqlrpc.Server{DB: db, Expiry: time.Minute}
	s.Service.Server = &s
	rpc.RegisterName("SQLRPC", &s.Service)
	rpc.HandleHTTP()
	http.Handle("/refs", refsHandler(&s))
	log.Print(http.ListenAndServe(flags.Addr, nil))
}
