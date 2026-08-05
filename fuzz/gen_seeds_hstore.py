#!/usr/bin/env python3
"""Seed corpus for hstore_diff (lane p1-mb-contribc).

Arm layout (see fuzz/core/src/hstorefam_diff.rs):
  0 in/out: [0, flags, seed(8), text...]
  1 recv:   [1, wire...]
  2 ctor:   [2, flags, pairs/text...]
  3 ops:    [3, flags, seed(8), pairsA, pairsB, probe]
  4 akeys:  [4, flags, pairs, keyspec...]
Literals lifted from vendored contrib/hstore/sql/hstore.sql.
"""
import hashlib
import os
import struct
import sys

OUT = os.path.join(os.path.dirname(__file__), "corpus", "hstore_diff")
os.makedirs(OUT, exist_ok=True)


def put(data: bytes):
    h = hashlib.sha1(data).hexdigest()
    with open(os.path.join(OUT, h), "wb") as f:
        f.write(data)


LITS = [
    b"", b"a=>b", b" a=>b", b"a =>b", b"a=>b ", b"a=> b",
    b'"a"=>"b"', b' "a"=>"b"', b'"a" =>"b"', b'"a"=>"b" ', b'"a"=> "b"',
    b"aa=>ba,cc=>dd", b"aa=>ba , cc=>dd", b"aa=>ba ,cc=>dd",
    b'"aa"=>"ba","cc"=>"dd"', b'"aa"=>"ba" , "cc"=>"dd"',
    b"aa=>null", b"aa=>NuLl", b'aa=>"NuLl"', b"aa=>\"null\"",
    b'aa=>""', b'""=>aa', b"1-a=>anything at all",
    b"a=>b,", b"a,b", b"a=b", b"a=>b,c", b"=>b", b"a=>b=>c",
    b"a\\=>b=>c", b"\\==>b", b"a=>b, c=>d ,e=>f",
    b"a=>1, b=>2, c=>3", b"cq=>l, cq=>NULL", b"cq=>l, cq=>m, cq=>n",
    b'aa=>1, cq=>l, b=>g, fg=>f, "1"=>NULL',
    b"k=>1,k=>2,k=>3,k=>4,k=>5,k=>6,k=>7,k=>8",
    b'"\xc3\xa9"=>"\xe4\xb8\xad\xf0\x9f\x98\x80"',
    b"a=>t, b=>f, c=>1.5, d=>007, e=>-3e2, f=>true",
    b'esc\\"aped=>va\\\\lue',
    b"   spaced   =>   out   ",
]

for lit in LITS:
    for flags in (0, 1):
        put(bytes([0, flags]) + struct.pack("<Q", 7) + lit)

# recv wires
def wire(pairs, pcount=None):
    w = struct.pack(">i", len(pairs) if pcount is None else pcount)
    for k, v in pairs:
        w += struct.pack(">i", len(k)) + k
        if v is None:
            w += struct.pack(">i", -1)
        else:
            w += struct.pack(">i", len(v)) + v
    return w


WIRES = [
    wire([]),
    wire([(b"a", b"x")]),
    wire([(b"a", b"x"), (b"bb", None)]),
    wire([(b"k", b"1"), (b"k", b"2"), (b"k", b"3"), (b"k", b"4"),
          (b"k", b"5"), (b"k", b"6"), (b"k", b"7"), (b"k", b"8")]),
    wire([(b"\xc3\xa9", b"\xe4\xb8\xad")]),
    wire([(b"bad", b"\xff\xfe")]),          # invalid utf8 value
    wire([(b"nu\x00l", b"x")]),             # embedded NUL key
    wire([(b"a", b"x")], pcount=5),          # count beyond message
    struct.pack(">i", -3),                   # negative count
    struct.pack(">i", 1) + struct.pack(">i", -5),  # negative key len
    struct.pack(">i", 1),                    # truncated
    struct.pack(">i", 70000) + b"trailing",  # clamp arm
]
for w in WIRES:
    put(bytes([1]) + w)

# ctor arms
def pairs_blob(pairs):
    b = bytes([len(pairs)])
    for k, v in pairs:
        b += bytes([len(k)]) + k
        if v is None:
            b += bytes([0, 0])
        else:
            b += bytes([1, len(v)]) + v
    return b


P3 = [(b"a", b"1"), (b"b", None), (b"a", b"3")]
for flags in (0, 4, 8, 12, 1, 5, 9, 17, 33, 49, 2, 6, 34, 66, 10, 18, 98):
    put(bytes([2, flags]) + pairs_blob(P3))

# ops arm
put(bytes([3, 1]) + struct.pack("<Q", 42) + pairs_blob([(b"k", b"v"), (b"k2", b"w")])
    + pairs_blob([(b"k", b"v"), (b"z", None)]) + bytes([1]) + b"k")
put(bytes([3, 0]) + struct.pack("<Q", 0) + pairs_blob([]) + pairs_blob([]) + bytes([0]))

# array-keyed arm
put(bytes([4, 0]) + pairs_blob([(b"a", b"1"), (b"b", None)])
    + bytes([4, 0, 1, 2, 2]) + b"aa" + bytes([3, 1]) + b"q")
put(bytes([4, 1]) + pairs_blob([(b"a", b"1"), (b"b", b"2")])
    + bytes([4, 5, 9, 13, 17]))

print(f"{len(os.listdir(OUT))} seeds in {OUT}", file=sys.stderr)
