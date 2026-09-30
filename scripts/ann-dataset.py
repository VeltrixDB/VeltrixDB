#!/usr/bin/env python3
"""ann-dataset.py — fetch an ann-benchmarks dataset and write it as fvecs.

The real-embedding recall test (storage/real_embeddings_test.go) and
cmd/vecbench read the simple fvecs format (per vector: int32 dim, then dim
float32, little-endian) so neither needs an HDF5 library. This script does
the one-time conversion.

Usage:
  scripts/ann-dataset.py glove-100-angular OUT_DIR [--train N] [--test Q]

Writes OUT_DIR/<name>.train.fvecs (first N base vectors) and
OUT_DIR/<name>.test.fvecs (first Q queries). Ground truth is NOT copied:
ann-benchmarks' neighbours are computed over the full base set, so for a
subset they are wrong; the consumers compute exact neighbours themselves.

Requires: h5py, numpy (pip install h5py numpy).
"""
import argparse
import os
import struct
import sys
import urllib.request

import h5py
import numpy as np

BASE = "http://ann-benchmarks.com/{}.hdf5"


def write_fvecs(path, arr):
    arr = np.ascontiguousarray(arr, dtype="<f4")
    n, d = arr.shape
    with open(path, "wb") as f:
        head = struct.pack("<i", d)
        for row in arr:
            f.write(head)
            f.write(row.tobytes())
    print(f"wrote {path}: {n} × {d}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name", help="ann-benchmarks dataset, e.g. glove-100-angular")
    ap.add_argument("out")
    ap.add_argument("--train", type=int, default=100000)
    ap.add_argument("--test", type=int, default=500)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    h5 = os.path.join(a.out, a.name + ".hdf5")
    if not os.path.exists(h5):
        url = BASE.format(a.name)
        print(f"downloading {url}", file=sys.stderr)
        urllib.request.urlretrieve(url, h5)
    with h5py.File(h5, "r") as f:
        train = f["train"][: a.train]
        test = f["test"][: a.test]
        metric = f.attrs.get("distance", b"")
    if isinstance(metric, bytes):
        metric = metric.decode()
    print(f"{a.name}: metric={metric} train={train.shape} test={test.shape}")
    write_fvecs(os.path.join(a.out, a.name + ".train.fvecs"), train)
    write_fvecs(os.path.join(a.out, a.name + ".test.fvecs"), test)


if __name__ == "__main__":
    main()
