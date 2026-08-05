#!/usr/bin/env python3
"""Mechanically harvest jsonpath_diff seeds (p1-laneaa).

Sources (all vendored 18.3 ground truth):
  1. every '...'::jsonpath literal in the regress SQL (jsonpath.sql,
     jsonb_jsonpath.sql, jsonpath_encoding.sql);
  2. every input in the crate's own regress-derived vector tables
     (crates/backend/utils/adt/jsonpath/src/vectors.rs).

Each harvested source text becomes:
  - an arm-0 seed  [0x00][mode=0x00][text]   (in+out, hard mode)
  - an arm-0 seed  [0x00][mode=0x01][text]   (soft mode)   [every 3rd text]
  - an arm-1 seed  [0x01][0x01][text]        (recv/send wire framing)
  - an arm-2 seed  [0x02][varsel][text]      (mutability, rotating varsel)
Plus a small set of hand-written framing/edge seeds.

Usage: python3 fuzz/gen_seeds_jsonpath.py            (writes fuzz/corpus/jsonpath_diff/)
"""
import hashlib
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "fuzz", "corpus", "jsonpath_diff")
VENDOR = "/Users/malisper/dev/pgrust-fabled/vendor/postgres-src/src/test/regress/sql"
REGRESS = ["jsonpath.sql", "jsonb_jsonpath.sql", "jsonpath_encoding.sql"]
VECTORS = os.path.join(ROOT, "crates/backend/utils/adt/jsonpath/src/vectors.rs")

# '<literal>'::jsonpath  — SQL doubles interior quotes
LIT = re.compile(r"'((?:[^']|'')*)'\s*::\s*jsonpath", re.S)
# ("input", "expected")  /  ("input", "msg", None|Some(..)) rows in vectors.rs
ROW = re.compile(r'^\s*\("((?:[^"\\]|\\.)*)"\s*,', re.M)


def unescape_rust(s):
    out, i = [], 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s):
            c = s[i + 1]
            out.append({"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'"}.get(c, "\\" + c))
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def harvest():
    texts = []
    for fn in REGRESS:
        p = os.path.join(VENDOR, fn)
        if not os.path.exists(p):
            print(f"note: {p} missing, skipped", file=sys.stderr)
            continue
        body = open(p, encoding="utf-8", errors="replace").read()
        for m in LIT.finditer(body):
            texts.append(m.group(1).replace("''", "'"))
    if os.path.exists(VECTORS):
        body = open(VECTORS, encoding="utf-8").read()
        for m in ROW.finditer(body):
            texts.append(unescape_rust(m.group(1)))
    # de-dup, preserve order
    seen, uniq = set(), []
    for t in texts:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


HAND = [
    # arm-1 wire framing edges
    b"\x01", b"\x01\x01", b"\x01\x00$", b"\x01\x02$", b"\x01\xff$",
    b"\x01\x01$.a", b"\x01\x01\x80\x80",
    # arm-0 unicode / numeric / regex edges
    b"\x00\x00\"\\ud83d\\ude00\"", b"\x00\x00\"\\ud83d\"", b"\x00\x00\"\\uZZZZ\"",
    b"\x00\x00\"\\u{1F600}\"", "\x00\x00\"é\"".encode(),
    b"\x00\x001e1000000", b"\x00\x001e-1000000",
    b"\x00\x000x7FFFFFFFFFFFFFFF",
    b"\x00\x00$ ? (@ like_regex \"[[:alpha:]]+\" flag \"i\")",
    b"\x00\x00$ ? (@ like_regex \"(bad\")",
    b"\x00\x00$ ? (@ like_regex \"a\" flag \"z\")",
    b"\x00\x01$.a[",
    b"\x00\x00$.decimal(1000,-1000)",
    b"\x00\x00$.a.**{4294967295}.b",
    # arm-2 datetime/zoned mutability shapes across var models
    b"\x02\x00$.datetime()", b"\x02\x05$ ? ($d == $dz)",
    b"\x02\x09$ ? ($ts == $tsz)", b"\x02\x0d$ ? ($a == $d)",
    b"\x02\x11$.datetime(\"HH24 TZH\")", b"\x02\x15$.time_tz()",
    # nesting (recursion plane, within the text cap)
    b"\x00\x00" + b"(" * 40 + b"$.a" + b")" * 40,
    b"\x00\x00$ " + b"? (@.a > 1) " * 20,
]


def main():
    os.makedirs(OUT, exist_ok=True)
    n = 0
    for i, t in enumerate(harvest()):
        b = t.encode("utf-8", errors="replace")
        if b"\x00" in b or len(b) > 512:
            continue
        seeds = [bytes([0x00, 0x00]) + b, bytes([0x01, 0x01]) + b,
                 bytes([0x02, i % 32]) + b]
        if i % 3 == 0:
            seeds.append(bytes([0x00, 0x01]) + b)
        for s in seeds:
            name = "seed-%s" % hashlib.sha1(s).hexdigest()[:16]
            with open(os.path.join(OUT, name), "wb") as f:
                f.write(s)
            n += 1
    for s in HAND:
        name = "seed-%s" % hashlib.sha1(s).hexdigest()[:16]
        with open(os.path.join(OUT, name), "wb") as f:
            f.write(s)
        n += 1
    print(f"wrote {n} seeds to {OUT}")


main()
