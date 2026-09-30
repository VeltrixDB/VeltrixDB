package ycsbdriver

import (
	"bytes"
	"testing"
)

func TestRecordEncoding(t *testing.T) {
	in := map[string][]byte{"field0": []byte("a"), "field1": bytes.Repeat([]byte("x"), 1000), "f": nil}
	all, err := decode(encode(in), nil)
	if err != nil || len(all) != 3 || !bytes.Equal(all["field1"], in["field1"]) {
		t.Fatalf("round trip: %v err=%v", all, err)
	}
	some, _ := decode(encode(in), []string{"field0"})
	if len(some) != 1 || string(some["field0"]) != "a" {
		t.Fatalf("field filter: %v", some)
	}
	if _, err := decode([]byte{5, 0, 'a'}, nil); err == nil {
		t.Fatal("truncated record must fail")
	}
}
