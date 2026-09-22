#!/usr/bin/env python3
"""Compare two res/blocks_log.csv files produced by `node`.

Rows are: finality,block_height,block_hash,block_local_hash,block_size
`block_local_hash` is sha256 of the exact JSON written to Redis, so an equal
hash at an equal height means the two runs produced byte-identical blocks.

Usage:
  compare_blocks_log.py A.csv B.csv [--finality Final]
"""
import argparse, collections, csv, sys

def load(path, finality):
    rows = {}
    with open(path) as f:
        for r in csv.reader(f):
            if len(r) != 5:
                continue
            fin, height, bhash, lhash, size = r
            if finality and fin != finality:
                continue
            # Last write wins: a restart can re-emit a height.
            rows[int(height)] = (bhash, lhash, int(size))
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("a"); ap.add_argument("b")
    ap.add_argument("--finality", default="Final",
                    help="Final | None | DoomSlug; empty string for all")
    args = ap.parse_args()

    a, b = load(args.a, args.finality), load(args.b, args.finality)
    common = sorted(set(a) & set(b))
    if not common:
        print(f"no overlapping heights for finality={args.finality!r} "
              f"(a: {len(a)} rows, b: {len(b)} rows)")
        return 1

    mismatched = [h for h in common if a[h] != b[h]]
    print(f"finality={args.finality!r} overlap={len(common)} "
          f"heights {common[0]}..{common[-1]}")
    print(f"  a only: {len(set(a) - set(b))}   b only: {len(set(b) - set(a))}")

    gaps = [(p, q) for p, q in zip(common, common[1:]) if q != p + 1]
    if gaps:
        print(f"  non-contiguous overlap at {len(gaps)} places, e.g. {gaps[:5]}")

    if not mismatched:
        print(f"  MATCH: all {len(common)} overlapping blocks are byte-identical")
        return 0

    print(f"  MISMATCH: {len(mismatched)}/{len(common)} blocks differ")
    kinds = collections.Counter(
        "block_hash" if a[h][0] != b[h][0] else "content" for h in mismatched)
    print(f"  {dict(kinds)}")
    for h in mismatched[:10]:
        print(f"    {h}: a={a[h]}  b={b[h]}")
    return 1

if __name__ == "__main__":
    sys.exit(main())
