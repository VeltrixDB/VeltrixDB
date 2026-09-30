package cluster

import "testing"

func TestRoutingKey_CoLocatesDerivedKeys(t *testing.T) {
	cases := map[string]string{
		"doc-42":                 "doc-42",
		"@vec/docs/doc-42":       "doc-42",
		"@txt/docs/doc-42":       "doc-42",
		"@idx/by_lang/en/doc-42": "doc-42",
		"@vec/docs/a/b":          "a/b", // ids may contain '/'
		"@idx/by_path/%2Fx/a/b":  "a/b",
		"@vecns/docs":            "@vecns/docs",
		"@vec/malformed":         "@vec/malformed",
		"@vec/docs/":             "@vec/docs/",
		"@other/x/y":             "@other/x/y",
	}
	for in, want := range cases {
		if got := RoutingKey(in); got != want {
			t.Errorf("RoutingKey(%q) = %q, want %q", in, got, want)
		}
	}
	if HashKey("@vec/docs/doc-42") != HashKey("doc-42") || hashKey("@txt/x/doc-42") != hashKey("doc-42") {
		t.Fatal("derived keys must hash like their record")
	}
}

func TestSearchPeers_SkipsLocalAndFailed(t *testing.T) {
	pm := NewPartitionMap(DefaultClusterConfig())
	for i, id := range []string{"n1", "n2", "n3"} {
		if err := pm.AddNode(id, "127.0.0.1", 7000+i); err != nil {
			t.Fatal(err)
		}
	}
	if err := pm.UpdateNodeState("n3", NodeStateFailed); err != nil {
		t.Fatal(err)
	}
	got := pm.SearchPeers("n1")
	if len(got) != 1 || got[0] != "n2" {
		t.Fatalf("SearchPeers = %v, want [n2]", got)
	}
}
