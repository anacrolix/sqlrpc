package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"sort"

	"github.com/anacrolix/missinggo/slices"
	"github.com/anacrolix/sqlrpc/refs"

	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/tagflag"
	_ "github.com/mattn/go-sqlite3"

	"github.com/anacrolix/sqlrpc"
)

func refsHandler(s *sqlrpc.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_refs := s.Refs.GetAll()
		fmt.Fprintf(w, "# refs: %d\n", len(_refs))
		ms := slices.FromMap(_refs)
		sort.Slice(ms, func(i, j int) bool {
			return ms[i].Key.(refs.Id) < ms[j].Key.(refs.Id)
		})
		for _, e := range ms {
			fmt.Fprintf(w, "%d: %#v\n", e.Key, e.Elem)
		}
	})
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	flags := struct {
		DSN            string
		Addr           string
		DbMaxOpenConns int
	}{
		Addr:           "localhost:6033",
		DbMaxOpenConns: 1,
	}
	tagflag.Parse(&flags)
	log.Printf("opening sqlite3 with data source name: %q", flags.DSN)
	db, err := sql.Open("sqlite3", flags.DSN)
	if err != nil {
		log.Fatalf("error opening database: %s", err)
	}
	db.SetMaxOpenConns(flags.DbMaxOpenConns)
	s := sqlrpc.Server{DB: db}
	s.Service.Refs = &s.Refs
	s.Service.DB = db
	rpc.RegisterName("SQLRPC", &s.Service)
	rpc.HandleHTTP()
	http.Handle("/refs", refsHandler(&s))
	log.Printf("listening on %q", flags.Addr)
	log.Print(http.ListenAndServe(flags.Addr, nil))
}
