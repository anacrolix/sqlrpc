package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/rpc"

	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/tagflag"
	_ "github.com/mattn/go-sqlite3"

	"github.com/anacrolix/sqlrpc"
)

func refsHandler(s *sqlrpc.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refs := s.Refs()
		fmt.Fprintf(w, "# refs: %d\n", len(refs))
		for ref, val := range refs {
			fmt.Fprintf(w, "%d: %#v\n", ref, val)
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
	s := sqlrpc.Server{DB: db}
	s.Service.Server = &s
	rpc.RegisterName("SQLRPC", &s.Service)
	rpc.HandleHTTP()
	http.Handle("/refs", refsHandler(&s))
	log.Print(http.ListenAndServe(flags.Addr, nil))
}
