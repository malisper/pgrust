#!/usr/bin/env python3
"""Fidelity check: parse src/generated.rs back into tuples and diff against
catalog dumps taken from a live pgrust server (see fidelity_check.sh for the
exact psql queries). Exit 0 = every generated row byte-matches the server row
with the same key; scope restricted to generated oids (the server additionally
carries catalog-rowtype composites genbki synthesizes from pg_class, which the
mock deliberately does not serve)."""
import re, sys, os

GEN = os.path.join(os.path.dirname(__file__), "src", "generated.rs")
src = open(GEN).read()

def rows(table, struct=None):
    m = re.search(rf"pub const {table}: [^=]+= &\[(.*?)\n\];", src, re.S)
    body = m.group(1)
    out = []
    if struct:
        for rm in re.finditer(r"\{ ([^}]*) \}", body):
            d = {}
            for kv in rm.group(1).split(", "):
                k, v = kv.split(": ", 1)
                d[k] = v
            out.append(d)
    else:
        for rm in re.finditer(r"\(([^)]*)\)", body):
            out.append([x.strip() for x in rm.group(1).split(",")])
    return out

def norm(v):
    v = v.strip()
    if v in ("true", "t"): return "t"
    if v in ("false", "f"): return "f"
    if v.startswith('"'): return v[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    try:
        n = int(v)
        if 32 <= n < 127: return n  # caller decides char vs int
    except ValueError:
        pass
    return v

def as_char(v):  # generated stores u8 codes; server prints the char
    return chr(int(v))

def tsv(path):
    return [l.split("\t") for l in open(path).read().splitlines()]

S = sys.argv[1]  # dir with srv_*.tsv
bad = 0

def report(name, key, gval, sval):
    global bad
    bad += 1
    print(f"MISMATCH {name} {key}: generated={gval!r} server={sval!r}")

# pg_type
srv = {r[0]: r for r in tsv(f"{S}/srv_pg_type.tsv")}
for d in rows("PG_TYPE", struct=True):
    oid = d["oid"]
    if oid not in srv:
        report("pg_type", oid, "row", "ABSENT"); continue
    r = srv[oid]
    g = [oid, d["typname"].strip('"'), d["typlen"],
         "t" if d["typbyval"] == "true" else "f",
         as_char(d["typalign"]), as_char(d["typstorage"]), as_char(d["typtype"]),
         as_char(d["typcategory"]),
         "t" if d["typispreferred"] == "true" else "f",
         "t" if d["typisdefined"] == "true" else "f",
         as_char(d["typdelim"]), d["typrelid"], d["typsubscript"], d["typelem"],
         d["typarray"], d["typcollation"], d["typinput"], d["typoutput"],
         d["typreceive"], d["typsend"], d["typmodin"], d["typmodout"]]
    if g != r:
        report("pg_type", oid, g, r)

# tuple tables: (name, server file, char column indexes, key columns)
specs = [
    ("PG_OPCLASS", "srv_pg_opclass.tsv", [], None),
    ("PG_AMOP", "srv_pg_amop.tsv", [4], None),
    ("PG_AMPROC", "srv_pg_amproc.tsv", [], None),
    ("PG_RANGE", "srv_pg_range.tsv", [], None),
    ("PG_CAST", "srv_pg_cast.tsv", [3, 4], None),
]
for name, f, charcols, _ in specs:
    gen = rows(name)
    srvr = tsv(f"{S}/{f}")
    if len(gen) != len(srvr):
        report(name, "rowcount", len(gen), len(srvr))
    for g, r in zip(gen, srvr):
        gg = []
        for i, v in enumerate(g):
            if i in charcols: gg.append(as_char(v))
            elif v in ("true",): gg.append("t")
            elif v in ("false",): gg.append("f")
            else: gg.append(v)
        if gg != r:
            report(name, g[0:4], gg, r)

# pg_operator (struct table)
srv = {r[0]: r for r in tsv(f"{S}/srv_pg_operator.tsv")}
gen_ops = rows("PG_OPERATOR", struct=True)
seen = set()
for d in gen_ops:
    oid = d["oid"]
    seen.add(oid)
    if oid not in srv:
        report("pg_operator", oid, d["oprname"], "ABSENT")
        continue
    r = srv[oid]
    g = [oid, d["oprname"][1:-1].replace('\\"', '"').replace("\\\\", "\\"),
         d["oprleft"], d["oprright"], d["oprresult"], d["oprcom"], d["oprnegate"],
         d["oprcode"], d["oprrest"], d["oprjoin"],
         "t" if d["oprcanmerge"] == "true" else "f",
         "t" if d["oprcanhash"] == "true" else "f"]
    if g != r:
        report("pg_operator", oid, g, r)
for oid in srv:
    if oid not in seen:
        report("pg_operator", oid, "ABSENT", srv[oid][1])

print(f"{'FAIL' if bad else 'PASS'}: {bad} mismatches")
sys.exit(1 if bad else 0)
