// It is the intention of the command to emulate the sqlite3 command-line
// utility where reasonable.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/anacrolix/envpprof"
	"github.com/docopt/docopt-go"

	_ "github.com/anacrolix/sqlrpc"
)

// Multiline strings in Go SUCK.
const doc = "" +
	"Usage: sqlrpc-cli [--dsn=<dsn>] <query>...\n" +
	"Options:\n" +
	"  --dsn=<dsn> data source name (database connection information)  [default: localhost:6033]"

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	opts, err := docopt.Parse(doc, nil, true, "", false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing options: %s", err)
		os.Exit(2)
	}
	dsn := opts["--dsn"].(string)
	db, err := sql.Open("sqlrpc", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening database: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()
	for _, arg := range opts["<query>"].([]string) {
		rows, err := db.Query(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error executing sql: %s\n", err)
			os.Exit(1)
		}
		cols, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		dest := make([]interface{}, len(cols))
		for i := range cols {
			dest[i] = new(interface{})
		}
		for rows.Next() {
			err = rows.Scan(dest...)
			if err != nil {
				log.Fatal(err)
			}
			for i := range cols {
				if i != 0 {
					fmt.Printf("|")
				}
				v := *dest[i].(*interface{})
				if v != nil {
					fmt.Printf(func() string {
						switch v.(type) {
						case []byte:
							return "%s"
						default:
							return "%v"
						}
					}(), v)
				}
			}
			fmt.Printf("\n")
		}
	}
}
