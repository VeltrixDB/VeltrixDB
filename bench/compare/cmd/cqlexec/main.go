// Command cqlexec runs CQL statements against ScyllaDB (or Cassandra) with the
// same gocql driver go-ycsb uses, so compare.sh needs neither cqlsh nor
// Docker on the client machine. Rows of a SELECT are printed as key=value.
//
//	cqlexec -hosts h1,h2,h3 -e "CREATE KEYSPACE …" -e "SELECT release_version FROM system.local"
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ";") }
func (m *multi) Set(s string) error { *m = append(*m, s); return nil }

func main() {
	hosts := flag.String("hosts", "127.0.0.1", "comma-separated contact points (host or host:port)")
	user := flag.String("user", "cassandra", "username (used only if the server asks)")
	pass := flag.String("pass", "cassandra", "password")
	wait := flag.Duration("wait", 2*time.Minute, "keep retrying the connection this long (cluster still starting)")
	var stmts multi
	flag.Var(&stmts, "e", "statement (repeatable, run in order)")
	flag.Parse()
	if len(stmts) == 0 {
		log.Fatal("cqlexec: no -e statement")
	}

	cluster := gocql.NewCluster(strings.Split(*hosts, ",")...)
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	var s *gocql.Session
	var err error
	for deadline := time.Now().Add(*wait); ; time.Sleep(3 * time.Second) {
		if s, err = cluster.CreateSession(); err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		log.Fatalf("cqlexec: connect %s: %v", *hosts, err)
	}
	defer s.Close()
	for _, st := range stmts {
		iter := s.Query(st).Iter()
		for row := map[string]interface{}{}; iter.MapScan(row); row = map[string]interface{}{} {
			keys := make([]string, 0, len(row))
			for k := range row {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, k := range keys {
				if i > 0 {
					fmt.Print(" ")
				}
				fmt.Printf("%s=%v", k, row[k])
			}
			fmt.Println()
		}
		if err := iter.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "cqlexec: %s: %v\n", st, err)
			os.Exit(1)
		}
	}
}
