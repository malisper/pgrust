#!/usr/bin/env python3
"""Mechanically harvest jsonpathexec_diff seeds (p1-laneaa, adt/jsonpath_exec).

Sources (vendored 18.3 ground truth):
  1. every jsonb_path_*('doc', 'path' [, 'vars' [, silent]]) call in
     regress sql/jsonb_jsonpath.sql — the doc and path literals are paired
     mechanically (best-effort: calls whose first two args are plain
     string literals);
  2. every  'doc'::jsonb @? 'path'  /  @@  expression in the same file;
  3. a witness-pair set generated from a small base matrix, each pair
     differing in exactly ONE dimension (array index / one doc leaf /
     silent flag / lax-vs-strict prefix / vars present-vs-absent), per the
     single-field-difference seeding obligation.

Datetime-family paths are still emitted (they exercise the driver's carve
filter — the input must be SKIPPED, not crash); the carve hit-rate over
the corpus is reported at the end.

Input layout (must match fuzz/core/src/jsonpathexec_diff.rs):
  [sel][s1 u16le][s2 u16le][path][doc][vars]
  where s1 = len(path), s2 = len(doc); sel bits: arm=sel&7, silent=8,
  tz=16, vars_present=32.

Usage: python3 fuzz/gen_seeds_jsonpathexec.py   (writes fuzz/corpus/jsonpathexec_diff/)
"""
import hashlib
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "fuzz", "corpus", "jsonpathexec_diff")
SQL = "/Users/malisper/dev/pgrust-fabled/vendor/postgres-src/src/test/regress/sql/jsonb_jsonpath.sql"

SQLSTR = r"'((?:[^']|'')*)'"
CALL = re.compile(
    r"jsonb_path_(?:exists|match|query|query_array|query_first)(?:_tz)?\s*\(\s*"
    + SQLSTR + r"(?:\s*::\s*jsonb)?\s*,\s*" + SQLSTR
    + r"(?:\s*::\s*jsonpath)?(?:\s*,\s*" + SQLSTR + r"(?:\s*::\s*jsonb)?)?",
    re.S,
)
OPR = re.compile(SQLSTR + r"\s*::\s*jsonb\s*(@\?|@@)\s*" + SQLSTR, re.S)

DT = re.compile(r"\.(datetime|date|time|time_tz|timestamp|timestamp_tz)\s*\(")


def unq(s):
    return s.replace("''", "'")


def enc(sel, path, doc, vars_text=b""):
    p, d = path.encode() if isinstance(path, str) else path, doc.encode() if isinstance(doc, str) else doc
    v = vars_text.encode() if isinstance(vars_text, str) else vars_text
    return bytes([sel]) + len(p).to_bytes(2, "little") + len(d).to_bytes(2, "little") + p + d + v


def main():
    os.makedirs(OUT, exist_ok=True)
    for f in os.listdir(OUT):
        if f.startswith("seed-"):
            os.unlink(os.path.join(OUT, f))
    sql = open(SQL).read()

    seeds = []
    ndt = 0
    pairs = []
    for m in CALL.finditer(sql):
        doc, path, vars_lit = unq(m.group(1)), unq(m.group(2)), m.group(3)
        pairs.append((path, doc, unq(vars_lit) if vars_lit else None))
    for m in OPR.finditer(sql):
        doc, op, path = unq(m.group(1)), m.group(2), unq(m.group(3))
        arm = 4 if op == "@?" else 5
        pairs.append((path, doc, None, arm))

    for i, rec in enumerate(pairs):
        path, doc, vars_text = rec[0], rec[1], rec[2] if len(rec) > 2 else None
        if len(path) > 256 or len(doc) > 512 or (vars_text and len(vars_text) > 256):
            continue
        if DT.search(path):
            ndt += 1
        if len(rec) == 4:
            arms = [rec[3]]
        else:
            # rotate function arms + flag bits so the corpus spans the matrix
            arms = [i % 4]
        for arm in arms:
            sel = arm
            if i % 2 == 1 and arm < 4:
                sel |= 0x08  # silent
            if i % 5 == 0 and arm < 4:
                sel |= 0x10  # tz wrapper
            if vars_text is not None and arm < 4:
                sel |= 0x20
            seeds.append(enc(sel, path, doc, vars_text or ""))
        # arm 6 (query_items) every 4th pair
        if i % 4 == 0 and len(rec) == 3:
            seeds.append(enc(6, path, doc, ""))

    # ---- witness pairs: one dimension changes per neighbor ----
    base_doc = '{"a": [1, 2, 3], "b": "str"}'
    for p in ["$.a[0]", "$.a[1]", "$.a[2]", "$.a[3]"]:
        seeds.append(enc(2, p, base_doc))
    for d in [
        '{"a": [1, 2, 3], "b": "str"}',
        '{"a": [1, 2, 4], "b": "str"}',
        '{"a": [1, 2, 3], "b": "st"}',
        '{"a": [1, 2, 3], "b": null}',
    ]:
        seeds.append(enc(2, "$.a[2]", d))
        seeds.append(enc(0, "$.b", d))
    for sel in [0, 0x08]:  # silent flipped
        seeds.append(enc(sel, "strict $.c", base_doc))
    for p in ["$.c", "lax $.c", "strict $.c"]:
        seeds.append(enc(3, p, base_doc))
    for v in [None, '{"x": 2}', '{"x": 3}', "{}"]:  # vars present/absent + delta
        sel = 1 | (0x20 if v is not None else 0)
        seeds.append(enc(sel, "$.a[*] ? (@ >= $x)", base_doc, v or ""))

    n = 0
    for s in set(seeds):
        h = hashlib.sha1(s).hexdigest()[:16]
        open(os.path.join(OUT, f"seed-{h}"), "wb").write(s)
        n += 1
    print(f"{n} seeds written ({len(pairs)} harvested pairs, {ndt} datetime-family paths exercising the carve filter)")


main()
