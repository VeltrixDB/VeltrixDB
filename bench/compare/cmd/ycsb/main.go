// Command ycsb runs YCSB core workloads (go-ycsb) against VeltrixDB,
// Aerospike or ScyllaDB with one runner, one set of workload files and the
// same measurement code, so the numbers are comparable.
//
//	ycsb load <db> -P workloads/workloada [-p name=value ...] [-threads N]
//	ycsb run  <db> -P workloads/workloada [-p name=value ...] [-threads N]
//
// <db> is veltrixdb (ycsbdriver), aerospike or cassandra (go-ycsb's drivers;
// cassandra speaks CQL and is what ScyllaDB is benchmarked with). Driver
// properties: veltrixdb.addr; aerospike.host, aerospike.port, aerospike.ns;
// cassandra.cluster, cassandra.keyspace, cassandra.connections.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	_ "github.com/pingcap/go-ycsb/db/aerospike"
	_ "github.com/pingcap/go-ycsb/db/cassandra"
	_ "github.com/pingcap/go-ycsb/pkg/workload"

	_ "github.com/VeltrixDB/veltrixdb/bench/compare/ycsbdriver"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(s string) error { *m = append(*m, s); return nil }

func main() {
	if len(os.Args) < 3 || (os.Args[1] != "load" && os.Args[1] != "run") {
		fmt.Fprintln(os.Stderr, "usage: ycsb load|run <veltrixdb|aerospike|cassandra> -P workload [-p k=v] [-threads N] [-target ops/s]")
		os.Exit(2)
	}
	command, dbName := os.Args[1], os.Args[2]
	fs := flag.NewFlagSet("ycsb", flag.ExitOnError)
	var files, props multi
	fs.Var(&files, "P", "workload property file (repeatable)")
	fs.Var(&props, "p", "property name=value (repeatable)")
	threads := fs.Int("threads", 0, "worker threads (overrides threadcount)")
	target := fs.Int("target", 0, "target ops/s (0 = unthrottled)")
	_ = fs.Parse(os.Args[3:])

	p := properties.NewProperties()
	if len(files) > 0 {
		p = properties.MustLoadFiles(files, properties.UTF8, false)
	}
	for _, kv := range props {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("bad property %q (want name=value)", kv)
		}
		p.Set(parts[0], parts[1])
	}
	p.Set(prop.DoTransactions, strconv.FormatBool(command == "run"))
	p.Set(prop.Command, command)
	if *threads > 0 {
		p.Set(prop.ThreadCount, strconv.Itoa(*threads))
	}
	if *target > 0 {
		p.Set(prop.Target, strconv.Itoa(*target))
	}

	measurement.InitMeasure(p)
	wl, err := ycsb.GetWorkloadCreator(p.GetString(prop.Workload, "core")).Create(p)
	if err != nil {
		log.Fatalf("workload: %v", err)
	}
	creator := ycsb.GetDBCreator(dbName)
	if creator == nil {
		log.Fatalf("unknown db %q (want veltrixdb, aerospike or cassandra)", dbName)
	}
	db, err := creator.Create(p)
	if err != nil {
		log.Fatalf("db %s: %v", dbName, err)
	}

	fmt.Printf("# db=%s command=%s", dbName, command)
	for _, k := range []string{"workload", "recordcount", "operationcount", "threadcount", "fieldcount", "fieldlength",
		"readproportion", "updateproportion", "insertproportion", "readmodifywriteproportion", "scanproportion", "requestdistribution"} {
		if v, ok := p.Get(k); ok {
			fmt.Printf(" %s=%s", k, v)
		}
	}
	fmt.Println()
	start := time.Now()
	client.NewClient(p, wl, client.DbWrapper{DB: db}).Run(context.Background())
	fmt.Printf("# elapsed %s\n", time.Since(start).Round(time.Millisecond))
	measurement.Output()
	_ = db.Close()
	wl.Close()
}
