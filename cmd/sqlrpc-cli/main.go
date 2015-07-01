// It is the intention of the command to emulate the sqlite3 command-line
// utility where reasonable.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/anacrolix/sqlrpc"
)

func main() {
	dsn := flag.String("dsn", "localhost:6033", "data source name: database connection information")
	flag.Parse()
	db, err := sql.Open("sqlrpc", *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening database: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()
	for _, arg := range flag.Args() {
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
					fmt.Printf("%s", v)
				}
			}
			fmt.Printf("\n")
		}
	}
}
