package annds

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestFvecsRoundTripAndGroundTruth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.fvecs")
	vecs := [][]float32{{1, 0}, {0, 1}, {0.9, 0.1}, {-1, 0}}
	f, _ := os.Create(path)
	for _, v := range vecs {
		_ = binary.Write(f, binary.LittleEndian, int32(len(v)))
		for _, x := range v {
			_ = binary.Write(f, binary.LittleEndian, math.Float32bits(x))
		}
	}
	f.Close()
	got, err := ReadFvecs(path, 0)
	if err != nil || len(got) != 4 || got[2][0] != 0.9 {
		t.Fatalf("ReadFvecs = %v err=%v", got, err)
	}
	if two, _ := ReadFvecs(path, 2); len(two) != 2 {
		t.Fatalf("max ignored: %d", len(two))
	}
	gt := GroundTruth(got, [][]float32{{1, 0}}, 2)
	if gt[0][0] != 0 || gt[0][1] != 2 {
		t.Fatalf("ground truth = %v, want [0 2]", gt)
	}
	if r := Recall([][]int{{2, 3}}, gt, 2); r != 0.5 {
		t.Fatalf("recall = %v, want 0.5", r)
	}
}
