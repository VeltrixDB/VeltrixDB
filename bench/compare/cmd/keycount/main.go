// Command keycount checks that every node of a VeltrixDB cluster holds the
// same YCSB records: it pages through RANGE on each node's binary port and
// prints, per node, how many keys under -prefix it serves and how many of the
// union it is missing. With RF 3 on 3 nodes every node must hold every key; a
// YCSB run cannot tell (a read of a missing key counts as an empty success).
//
//	keycount -addrs h1:9000,h2:9000,h3:9000 [-prefix usertable:]
//
// Exit status 1 when any node misses a key. Keys written while the check
// runs show up as missing on nodes that have not applied them yet — run it
// after the workload.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
)

func main() {
	addrs := flag.String("addrs", "127.0.0.1:9000", "comma-separated binary-protocol addresses")
	prefix := flag.String("prefix", "usertable:", "key prefix (YCSB table + ':')")
	flag.Parse()
	end := (*prefix)[:len(*prefix)-1] + string((*prefix)[len(*prefix)-1]+1)

	list := strings.Split(*addrs, ",")
	sets := make([]map[string]bool, len(list))
	union := map[string]bool{}
	for i, a := range list {
		c, err := client.DialBinary(a, 5*time.Second)
		if err != nil {
			fmt.Printf("keycount %s: %v\n", a, err)
			os.Exit(1)
		}
		set := map[string]bool{}
		for start := *prefix; ; {
			kvs, err := c.RangeScan(start, end, 1000, false)
			if err != nil {
				fmt.Printf("keycount %s: %v\n", a, err)
				os.Exit(1)
			}
			for _, kv := range kvs {
				set[kv.Key] = true
				union[kv.Key] = true
			}
			if len(kvs) < 1000 {
				break
			}
			start = kvs[len(kvs)-1].Key + "\x00"
		}
		c.Close()
		sets[i] = set
	}
	bad := false
	for i, a := range list {
		miss := len(union) - len(sets[i])
		if miss > 0 {
			bad = true
		}
		fmt.Printf("keycount %s: keys=%d missing=%d (of %d across the cluster)\n", a, len(sets[i]), miss, len(union))
	}
	if bad {
		os.Exit(1)
	}
}
