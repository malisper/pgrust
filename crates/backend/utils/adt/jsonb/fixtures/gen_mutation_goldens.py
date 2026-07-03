#!/usr/bin/env python3
# Regenerates golden_mutations.tsv from live PostgreSQL 18: the on-disk datum
# payload (pageinspect) of each mutation result. Usage:
#   gen_mutation_goldens.py <psql-path> <connstr-args...>
import os
import subprocess
import sys

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


def lit(s):
    assert "$q$" not in s
    return f"$q${s}$q$"


def hx(s):
    return s.encode("utf-8").hex()


BIG_ARR = "[" + ",".join(str(i) for i in range(70)) + "]"
BIG_OBJ = "{" + ",".join(f'"k{i:02}":{i}' for i in range(40)) + "}"
DEEP = '{"a":{"b":{"c":[1,{"d":[2,3]},null],"e":1.50},"f":"é中"}}'

# op \t target \t a1 \t a2 \t a3 — mirrored verbatim by tests.rs.
CASES = [
    ("concat", '{"a":1}', '{"b":2}', "", ""),
    ("concat", '{"a":1,"b":{"x":1}}', '{"b":{"y":2},"c":[1]}', "", ""),
    ("concat", "[1,2]", "[3,4]", "", ""),
    ("concat", "[1,2]", '"x"', "", ""),
    ("concat", '"x"', "[1,2]", "", ""),
    ("concat", '"a"', '"b"', "", ""),
    ("concat", '{"a":1}', "[1]", "", ""),
    ("concat", "[1]", '{"a":1}', "", ""),
    ("concat", "{}", '{"a":1}', "", ""),
    ("concat", '{"a":1}', "{}", "", ""),
    ("concat", "[]", "[1]", "", ""),
    ("concat", "3", "[]", "", ""),
    ("concat", "3", "4", "", ""),
    ("concat", BIG_ARR, BIG_ARR, "", ""),
    ("concat", BIG_OBJ, DEEP, "", ""),
    ("del_key", '{"a":1,"b":2}', "a", "", ""),
    ("del_key", '{"a":1,"b":2}', "z", "", ""),
    ("del_key", '["a","b","a",1]', "a", "", ""),
    ("del_key", "{}", "a", "", ""),
    ("del_key", '{"a":{"a":1}}', "a", "", ""),
    ("del_key", BIG_OBJ, "k17", "", ""),
    ("del_key", DEEP, "f", "", ""),
    ("del_idx", "[1,2,3]", "0", "", ""),
    ("del_idx", "[1,2,3]", "2", "", ""),
    ("del_idx", "[1,2,3]", "3", "", ""),
    ("del_idx", "[1,2,3]", "-1", "", ""),
    ("del_idx", "[1,2,3]", "-3", "", ""),
    ("del_idx", "[1,2,3]", "-4", "", ""),
    ("del_idx", "[]", "0", "", ""),
    ("del_idx", BIG_ARR, "33", "", ""),
    ("del_keys", '{"a":1,"b":2,"c":3}', "a,c", "", ""),
    ("del_keys", '["a","b","c"]', "a,c", "", ""),
    ("del_keys", BIG_OBJ, "k00,k39", "", ""),
    ("del_path", '{"a":{"b":1,"c":2}}', "a,b", "", ""),
    ("del_path", '{"a":{"b":1}}', "a", "", ""),
    ("del_path", '{"a":[1,2,3]}', "a,1", "", ""),
    ("del_path", '{"a":[1,2,3]}', "a,-1", "", ""),
    ("del_path", '{"a":[1,2,3]}', "a,-4", "", ""),
    ("del_path", '{"a":1}', "z,b", "", ""),
    ("del_path", '{"a":{"b":1}}', "a,b,c", "", ""),
    ("del_path", "[[1,2],[3]]", "0,1", "", ""),
    ("del_path", DEEP, "a,b,c", "", ""),
    ("set", '{"a":1,"b":2}', "a", "9", "true"),
    ("set", '{"a":1}', "b", "2", "true"),
    ("set", '{"a":1}', "b", "2", "false"),
    ("set", '{"a":{"b":1}}', "a,b", '"x"', "true"),
    ("set", '{"a":{"b":1}}', "a,c", "[1,2]", "true"),
    ("set", '{"a":{"b":1}}', "a,c,d", "1", "true"),
    ("set", "[1,2,3]", "1", '"two"', "true"),
    ("set", "[1,2,3]", "-1", "99", "true"),
    ("set", "[1,2,3]", "5", "99", "true"),
    ("set", "[1,2,3]", "5", "99", "false"),
    ("set", "[1,2,3]", "-5", "99", "true"),
    ("set", "[1,2,3]", "-5", "99", "false"),
    ("set", '{"a":[1,2]}', "a,0", '{"x":true}', "true"),
    ("set", "{}", "a", "1", "true"),
    ("set", "{}", "a", "1", "false"),
    ("set", "[]", "0", "1", "true"),
    ("set", "[]", "0", "1", "false"),
    ("set", '{"a":1}', "a,b", "2", "true"),
    ("set", BIG_ARR, "40", "null", "true"),
    ("set", BIG_OBJ, "k20", DEEP, "true"),
    ("set", DEEP, "a,b,c,1", '{"deep":true}', "true"),
    ("insert", "[1,3]", "1", "2", "false"),
    ("insert", "[1,3]", "1", "2", "true"),
    ("insert", "[1,3]", "-1", "2", "false"),
    ("insert", "[1,3]", "-1", "2", "true"),
    ("insert", "[1,3]", "0", "0", "false"),
    ("insert", "[1,3]", "99", "9", "false"),
    ("insert", "[1,3]", "-99", "9", "false"),
    ("insert", '{"a":{"b":[1,2]}}', "a,b,1", '"new"', "false"),
    ("insert", '{"a":1}', "b", "2", "false"),
    ("insert", "{}", "a", "1", "false"),
    ("insert", "[]", "0", "1", "false"),
    ("insert", BIG_ARR, "-35", '"mid"', "true"),
]


def path_lit(p):
    inner = ",".join('"' + e + '"' for e in p.split(","))
    return lit("{" + inner + "}") + "::text[]"


def expr(case):
    op, target, a1, a2, a3 = case
    t = lit(target) + "::jsonb"
    if op == "concat":
        return f"{t} || {lit(a1)}::jsonb"
    if op == "del_key":
        return f"{t} - {lit(a1)}::text"
    if op == "del_idx":
        return f"{t} - ({a1})::int4"
    if op == "del_keys":
        return f"{t} - {path_lit(a1)}"
    if op == "del_path":
        return f"{t} #- {path_lit(a1)}"
    if op == "set":
        return f"jsonb_set({t}, {path_lit(a1)}, {lit(a2)}::jsonb, {a3})"
    if op == "insert":
        return f"jsonb_insert({t}, {path_lit(a1)}, {lit(a2)}::jsonb, {a3})"
    raise AssertionError(op)


setup = ["drop table if exists tmut;",
         "create table tmut(j jsonb);",
         "alter table tmut alter j set storage external;"]
for c in CASES:
    setup.append(f"insert into tmut values ({expr(c)});")
run_sql("\n".join(setup))

run_sql("create extension if not exists pageinspect;")
npages = int(run_sql("select pg_relation_size('tmut')/8192;")[0])
tdatas = []
for p in range(npages):
    tdatas += run_sql(
        f"select encode(t_data,'hex') from heap_page_items(get_raw_page('tmut',{p}))"
        " where t_data is not null order by lp;")
assert len(tdatas) == len(CASES), (len(tdatas), len(CASES))


def payload_hex(tdata_hex):
    b = bytes.fromhex(tdata_hex)
    if b[0] & 1:
        total = (b[0] >> 1) & 0x7F
        return b[1:total].hex()
    total = int.from_bytes(b[0:4], "little") >> 2
    return b[4:total].hex()


with open(os.path.join(HERE, "golden_mutations.tsv"), "w") as f:
    f.write("# op\ttarget_hex\ta1_hex\ta2_hex\ta3\tpayload_hex\n")
    for c, td in zip(CASES, tdatas):
        op, target, a1, a2, a3 = c
        f.write("\t".join([op, hx(target), hx(a1), hx(a2), a3, payload_hex(td)]) + "\n")
print(f"wrote {len(CASES)} mutation goldens")
