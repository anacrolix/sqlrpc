package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"time"

	_ "github.com/anacrolix/envpprof"
	_ "github.com/mattn/go-sqlite3"

	"github.com/anacrolix/sqlrpc"
)

func refsHandler(s *sqlrpc.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for ref, val := range s.Refs() {
			fmt.Fprintf(w, "%d: %#v\n\n", ref, val)
		}
	})
}

func main() {
	log.SetFlags(log.Flags() | log.Llongfile)
	dsn := flag.String("dsn", "", "sqlite3 dsn")
	addr := flag.String("addr", ":6033", "listen")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "unexpected positional arguments\n")
		os.Exit(2)
	}
	db, err := sql.Open("sqlite3", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	s := sqlrpc.Service{DB: db, Expiry: time.Minute}
	rpc.Register(&s)
	rpc.HandleHTTP()
	http.Handle("/refs", refsHandler(&s))
	log.Print(http.ListenAndServe(*addr, nil))
}
