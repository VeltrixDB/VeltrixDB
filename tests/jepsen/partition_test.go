package jepsen

// partition_test.go — the network-partition hook.
//
// A real partition between processes on one host needs packet filtering,
// which needs root: iptables / nftables on Linux, pf on macOS (pfctl). The
// harness therefore does not partition by itself. Set
//
//	VELTRIX_JEPSEN_PARTITION_CMD   shell command that isolates one node
//	VELTRIX_JEPSEN_HEAL_CMD        shell command that removes every rule
//
// and the nemesis adds a "partition" fault that runs them through sh -c with
// these environment variables:
//
//	NODE_ID, NODE_PORT (client), NODE_REPL_PORT (P+1), NODE_RAFT_PORT (P+2),
//	NODE_GOSSIP_PORT (P+3), OTHER_PORTS (the other nodes' client ports,
//	space separated)
//
// See README.md for a Linux iptables example and its limits (on loopback a
// port-based rule set is asymmetric; separate network namespaces or
// containers give a symmetric partition).

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Partitioner isolates one node from the others and heals.
type Partitioner interface {
	Name() string
	Isolate(n *node, others []*node) error
	Heal() error
}

type execPartitioner struct{ isolate, heal string }

// partitionerFromEnv returns the hook, or nil when it is not configured.
func partitionerFromEnv() Partitioner {
	iso, heal := os.Getenv("VELTRIX_JEPSEN_PARTITION_CMD"), os.Getenv("VELTRIX_JEPSEN_HEAL_CMD")
	if iso == "" || heal == "" {
		return nil
	}
	return &execPartitioner{isolate: iso, heal: heal}
}

func (p *execPartitioner) Name() string { return "exec-hook" }

func (p *execPartitioner) Isolate(n *node, others []*node) error {
	var ports []string
	for _, o := range others {
		ports = append(ports, strconv.Itoa(o.port))
	}
	cmd := exec.Command("sh", "-c", p.isolate)
	cmd.Env = append(os.Environ(),
		"NODE_ID="+n.id,
		"NODE_PORT="+strconv.Itoa(n.port),
		"NODE_REPL_PORT="+strconv.Itoa(n.port+1),
		"NODE_RAFT_PORT="+strconv.Itoa(n.port+2),
		"NODE_GOSSIP_PORT="+strconv.Itoa(n.port+3),
		"OTHER_PORTS="+strings.Join(ports, " "))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	return nil
}

func (p *execPartitioner) Heal() error {
	if out, err := exec.Command("sh", "-c", p.heal).CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	return nil
}
