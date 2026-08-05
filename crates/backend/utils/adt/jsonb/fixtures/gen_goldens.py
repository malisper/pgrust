#!/usr/bin/env python3
# Regenerates the C-PostgreSQL golden fixtures from corpus.jsonl against a
# live PostgreSQL 18 (C collation, UTF8). Usage:
#   gen_goldens.py <psql-path> <connstr-args...>
# e.g. gen_goldens.py /opt/homebrew/opt/postgresql@18/bin/psql -h /tmp/jbsock -p 54331 -U postgres -d postgres
import subprocess
import sys
import os

HERE = os.path.dirname(os.path.abspath(__file__))
PSQL = sys.argv[1:]


def run_sql(sql):
    p = subprocess.run(
        PSQL + ["-X", "-q", "-At", "-F", "\t", "-P", "null=\\N", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)
    if p.returncode != 0:
        sys.stderr.write(p.stderr)
        sys.exit(1)
    return p.stdout.rstrip("\n").split("\n") if p.stdout else []


def lit(doc):
    assert "$jb$" not in doc
    return f"$jb${doc}$jb$"


def hx(s):
    return s.encode("utf-8").hex()


docs = [l for l in open(os.path.join(HERE, "corpus.jsonl")).read().split("\n") if l.strip()]

setup = ["drop table if exists tjb;",
         "create table tjb(j jsonb);",
         "alter table tjb alter j set storage external;"]
for d in docs:
    setup.append(f"insert into tjb values ({lit(d)}::jsonb);")
run_sql("\n".join(setup))

# Per-document goldens: text out, typeof, hashes.
rows = run_sql(
    "select j::text, jsonb_typeof(j), jsonb_hash(j), jsonb_hash_extended(j,0),"
    " jsonb_hash_extended(j,42) from tjb order by ctid;")
assert len(rows) == len(docs), (len(rows), len(docs))

# Raw datum payload bytes via pageinspect.
run_sql("create extension if not exists pageinspect;")
npages = int(run_sql("select pg_relation_size('tjb')/8192;")[0])
tdatas = []
for p in range(npages):
    tdatas += run_sql(
        f"select encode(t_data,'hex') from heap_page_items(get_raw_page('tjb',{p}))"
        " where t_data is not null order by lp;")
assert len(tdatas) == len(docs), (len(tdatas), len(docs))


def payload_hex(tdata_hex):
    b = bytes.fromhex(tdata_hex)
    if b[0] & 1:
        total = (b[0] >> 1) & 0x7F
        return b[1:total].hex()
    total = int.from_bytes(b[0:4], "little") >> 2
    return b[4:total].hex()


with open(os.path.join(HERE, "golden_docs.tsv"), "w") as f:
    f.write("# input_hex\tout_hex\ttypeof\thash\thash_ext0\thash_ext42\tpayload_hex\n")
    for d, row, td in zip(docs, rows, tdatas):
        out, typeof, h, h0, h42 = row.split("\t")
        f.write("\t".join([hx(d), hx(out), typeof, h, h0, h42, payload_hex(td)]) + "\n")

# Ordering: ORDER BY j gives C's btree order over the corpus (with row ids).
run_sql("alter table tjb add id serial;")
order_rows = run_sql("select id-1 from tjb order by j, id;")
with open(os.path.join(HERE, "golden_order.tsv"), "w") as f:
    f.write("\n".join(order_rows) + "\n")

# Pairwise cmp signs over the first 40 docs.
cmp_rows = run_sql(
    "select a.id-1, b.id-1, sign(jsonb_cmp(a.j, b.j)) from tjb a, tjb b"
    " where a.id <= 40 and b.id <= 40 order by a.id, b.id;")
with open(os.path.join(HERE, "golden_cmp.tsv"), "w") as f:
    f.write("\n".join(cmp_rows) + "\n")

# Containment truth table (both directions) over all docs vs a probe set.
probes = ['{"a": 1}', '{"a": 2}', '{"b": 2}', '{"a": 1, "b": 2}', '[1]',
          '[1, 2]', '[[1]]', '1', '"a"', 'null', '[]', '{}', '{"a": {"b": {"c": [1]}}}',
          '{"a": [1]}', '[{"a": 1}]', '["a", "b"]', '{"foo": {"bar": "baz"}}',
          '[1.5]', '[1.50]', '{"nested": {"x": {}}}', '[{}]', 'true', '[30]',
          '{"k1": 1}', '["s0"]', '[0.1]']
sql = []
for i, d in enumerate(docs):
    for k, pr in enumerate(probes):
        sql.append(f"select {i}, {k}, ({lit(d)}::jsonb @> {lit(pr)}::jsonb),"
                   f" ({lit(d)}::jsonb <@ {lit(pr)}::jsonb);")
rows = run_sql("\n".join(sql))
with open(os.path.join(HERE, "golden_contains.tsv"), "w") as f:
    f.write("# doc_idx\tprobe_hex\tcontains\tcontained\n")
    for r in rows:
        i, k, c1, c2 = r.split("\t")
        f.write("\t".join([i, hx(probes[int(k)]), c1, c2]) + "\n")

# Existence (?) over all docs vs probe keys.
keys = ["a", "b", "c", "", "key", "hello", "s0", "k39", "ékey", "世界", "1",
        "true", "nested", "foo", "long_key_that_is_much_longer_than_the_others"]
sql = []
for i, d in enumerate(docs):
    for k in keys:
        sql.append(f"select {i}, ({lit(d)}::jsonb ? {lit(k)}::text);")
rows = run_sql("\n".join(sql))
with open(os.path.join(HERE, "golden_exists.tsv"), "w") as f:
    f.write("# doc_idx\tkey_hex\texists\n")
    for r, (i, k) in zip(rows, [(i, k) for i in range(len(docs)) for k in keys]):
        _, e = r.split("\t")
        f.write("\t".join([str(i), hx(k), e]) + "\n")

# Field/element access: -> and ->> with keys and indexes, #> / #>> paths.
field_keys = ["a", "b", "nested", "foo", "k39", "", "missing", "ékey", "num_after_string_pad", "w"]
indexes = [-3, -2, -1, 0, 1, 2, 5, 50, -2147483648, 2147483647]
paths = ["{a}", "{a,b}", "{a,b,c}", "{a,b,c,1,d}", "{0}", "{-1}", "{1,0}",
         "{}", "{a,0}", "{foo,bar}", "{w,0,1,0}", "{v,u,0,t,1}", "{a, 1}", "{99}",
         "{nested,x,y,z}"]
sql = []
probes = []
for i, d in enumerate(docs):
    for k in field_keys:
        sql.append(f"select encode(convert_to(({lit(d)}::jsonb -> {lit(k)}::text)::text,'UTF8'),'hex'),"
                   f" encode(convert_to(({lit(d)}::jsonb ->> {lit(k)}::text),'UTF8'),'hex');")
        probes.append(("k", i, k))
    for ix in indexes:
        sql.append(f"select encode(convert_to(({lit(d)}::jsonb -> ({ix})::int)::text,'UTF8'),'hex'),"
                   f" encode(convert_to(({lit(d)}::jsonb ->> ({ix})::int),'UTF8'),'hex');")
        probes.append(("i", i, str(ix)))
    for pth in paths:
        sql.append(f"select encode(convert_to(({lit(d)}::jsonb #> {lit(pth)}::text[])::text,'UTF8'),'hex'),"
                   f" encode(convert_to(({lit(d)}::jsonb #>> {lit(pth)}::text[]),'UTF8'),'hex');")
        probes.append(("p", i, pth))
rows = run_sql("\n".join(sql))
assert len(rows) == len(probes)
with open(os.path.join(HERE, "golden_getfield.tsv"), "w") as f:
    f.write("# kind\tdoc_idx\targ_hex\tarrow_hex_or_N\tarrow_text_hex_or_N\n")
    for (kind, i, arg), r in zip(probes, rows):
        a, at = r.split("\t")
        ah = "N" if a == "\\N" else a
        ath = "N" if at == "\\N" else at
        f.write("\t".join([kind, str(i), hx(arg), ah, ath]) + "\n")

print("goldens written")
