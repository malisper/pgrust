#!/usr/bin/env python3
"""Generate src/generated.rs: copyObject arms for every types_nodes struct
that C 18.3 copyfuncs supports (copyfuncs.switch.c case list). The Rust analog
of gen_node_support.pl's copyfuncs.funcs.c. Rerun after vocabulary changes:

    python3 generate.py [path-to-C-src-root]

Cross-checks each generated arm's field list against the C generated file and
prints drift (informational; the copy must cover the Rust struct in full)."""

import re
import sys
import os
from collections import OrderedDict

HERE = os.path.dirname(os.path.abspath(__file__))
NODES_SRC = os.path.join(HERE, "../../../_support/types/nodes/src")
C_ROOT = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("PG_C_ROOT", "./postgres-18.3")
C_NODES = os.path.join(C_ROOT, "src/backend/nodes")

MODULES = ["primnodes", "parsenodes", "rawnodes", "plannodes"]

# Hand-written in lib.rs (C also hand-writes these: copyfuncs.c proper).
HAND_WRITTEN = {"Const", "A_Const"}

STRUCT_RE = re.compile(r"^pub struct (\w+)(<'mcx>)?\s*\{")
FIELD_RE = re.compile(r"^    pub (r#)?(\w+): (.+?),(?:\s*//.*)?$")
VARIANT_RE = re.compile(
    r"unsafe impl(?:<'mcx>)? NodeVariant<'(?:mcx|_)> for (\w+)(?:<'mcx>)?\s*\{\s*\n\s*const TAG: NodeTag = NodeTag::(T_\w+);"
)


def parse_module(name):
    path = os.path.join(NODES_SRC, name + ".rs")
    src = open(path).read()
    structs = OrderedDict()
    lines = src.split("\n")
    i = 0
    while i < len(lines):
        m = STRUCT_RE.match(lines[i])
        if m:
            sname = m.group(1)
            has_lt = m.group(2) is not None
            fields = []
            if lines[i].rstrip().endswith("}"):
                structs[sname] = (has_lt, fields)
                i += 1
                continue
            i += 1
            while i < len(lines) and not lines[i].startswith("}"):
                fm = FIELD_RE.match(lines[i])
                if fm:
                    fields.append(((fm.group(1) or "") + fm.group(2), fm.group(3).strip()))
                elif lines[i].strip() and not lines[i].strip().startswith("//") and not lines[i].strip().startswith("#["):
                    # Non-pub field or multiline type: refuse silently-wrong output.
                    raise SystemExit(f"unparsed struct line in {name}::{sname}: {lines[i]!r}")
                i += 1
            structs[sname] = (has_lt, fields)
        i += 1
    variants = dict(VARIANT_RE.findall(src))
    return structs, variants


def parse_c_fields():
    """node name -> [field names copied by C]"""
    out = {}
    src = open(os.path.join(C_NODES, "copyfuncs.funcs.c")).read()
    for m in re.finditer(r"_copy(\w+)\(const \w+ \*from\)\n\{(.*?)\n\}", src, re.S):
        out[m.group(1)] = re.findall(r"COPY_\w+_FIELD\((\w+)[,)]", m.group(2))
    return out


def parse_c_switch():
    src = open(os.path.join(C_NODES, "copyfuncs.switch.c")).read()
    return set(re.findall(r"case T_(\w+):", src))


mod_structs = {}
mod_variants = {}
struct_mod = {}   # name -> module chosen for codegen
all_variants = {} # name -> tag
for mod in MODULES:
    structs, variants = parse_module(mod)
    mod_structs[mod] = structs
    mod_variants[mod] = variants
    for s in structs:
        # Duplicate struct names across modules (CollateClause): first module
        # in MODULES order wins; NodeVariant tags must agree.
        if s not in struct_mod:
            struct_mod[s] = mod
    for s, tag in variants.items():
        if s in all_variants and all_variants[s] != tag:
            raise SystemExit(f"conflicting tags for {s}")
        all_variants[s] = tag

c_fields = parse_c_fields()
c_switch = parse_c_switch()


def fields_of(name):
    return mod_structs[struct_mod[name]][name][1]


def has_lifetime(name):
    return mod_structs[struct_mod[name]][name][0]


def norm(ty):
    ty = re.sub(r"crate::(\w+::)*", "", ty)
    ty = ty.replace("types_core::xact::", "").replace("types_core::", "")
    ty = ty.replace("::mcx::", "").replace("mcx::", "")
    return ty.strip()


needed = set()  # structs needing a copy fn


def field_expr(owner, fname, ty):
    t = norm(ty)
    acc = f"s.{fname}"
    if t == "NodeList<'mcx>":
        return f"copy_node_list(mcx, &{acc})?"
    if t == "OptNodeList<'mcx>":
        return f"copy_opt_node_list(mcx, &{acc})?"
    if t == "IntList<'mcx>":
        return f"IntList::from_slice(mcx, {acc}.as_slice())?"
    if t == "OidList<'mcx>":
        return f"OidList::from_slice(mcx, {acc}.as_slice())?"
    if t == "XidList<'mcx>":
        return f"XidList::from_slice(mcx, {acc}.as_slice())?"
    if t == "Bitmapset<'mcx>":
        return f"copy_bms(mcx, &{acc})?"
    if t == "Node<'mcx>":
        return f"copy_node(mcx, {acc})?"
    if t == "Option<Node<'mcx>>":
        return f"copy_node_opt(mcx, {acc})?"
    if t == "&'mcx str":
        return f"str_in(mcx, {acc})?"
    if t == "Option<&'mcx str>":
        return f"opt_str_in(mcx, {acc})?"
    if t == "DistinctClause<'mcx>":
        return (f"match &{acc} {{ DistinctClause::None => DistinctClause::None, "
                "DistinctClause::All => DistinctClause::All, "
                "DistinctClause::On(l) => DistinctClause::On(copy_node_list(mcx, l)?) }")
    m = re.fullmatch(r"&'mcx \[(\w+)\]", t)
    if m:
        return f"copy_slice(mcx, {acc})?"
    m = re.fullmatch(r"Option<&'mcx (\w+)(?:<'mcx>)?>", t)
    if m:
        inner = m.group(1)
        needed.add(inner)
        mk = "mk_ref" if inner in all_variants else "alloc_ref"
        return (f"match {acc} {{ Some(v) => Some({mk}(mcx, copy_{inner}(mcx, v)?)?), None => None }}")
    m = re.fullmatch(r"&'mcx (\w+)(?:<'mcx>)?", t)
    if m:
        inner = m.group(1)
        needed.add(inner)
        mk = "mk_ref" if inner in all_variants else "alloc_ref"
        return f"{mk}(mcx, copy_{inner}(mcx, {acc})?)?"
    m = re.fullmatch(r"(\w+)<'mcx>", t)
    if m and m.group(1) in struct_mod:
        inner = m.group(1)
        needed.add(inner)
        return f"copy_{inner}(mcx, &{acc})?"
    if t == "Datum":
        raise SystemExit(f"Datum field outside hand-written arms: {owner}.{fname}")
    if re.fullmatch(r"\w+", t):
        return acc  # Copy scalar/enum
    raise SystemExit(f"unclassified field type {owner}.{fname}: {ty!r}")


# Arms: structs with a NodeVariant tag that C's copy switch covers.
arm_structs = sorted(
    s for s in struct_mod
    if s in all_variants and s in c_switch and s not in HAND_WRITTEN
)
needed.update(arm_structs)

# Transitive closure over embedded/ref struct fields.
frontier = list(needed)
bodies = {}
while frontier:
    name = frontier.pop()
    if name in bodies:
        continue
    before = set(needed)
    lines = [f"        {fname}: {field_expr(name, fname, ty)}," for fname, ty in fields_of(name)]
    bodies[name] = lines
    for n in needed - before:
        frontier.append(n)

fn_structs = sorted(bodies)

out = []
out.append("""\
//! GENERATED by generate.py — DO NOT EDIT BY HAND. The Rust analog of C's
//! copyfuncs.funcs.c (gen_node_support.pl output): one deep-copy arm per
//! types_nodes struct in C 18.3's copyObject switch. Const/A_Const stay
//! hand-written in lib.rs (C hand-writes them in copyfuncs.c too).

#![allow(non_snake_case)]
#![allow(unused_imports)]

use mcx::{alloc_in, leak_in, Mcx};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, OidList, OptNodeList, XidList};
use types_nodes::rawnodes::DistinctClause;
use types_nodes::{Node, NodeList, NodeTag, NodeVariant};

use crate::{copy_node, opt_str_in, str_in};
""")
for mod in MODULES:
    names = sorted(n for n in fn_structs if struct_mod[n] == mod)
    if names:
        chunks = [f"use types_nodes::{mod}::{{"]
        line = "    "
        for n in names:
            if len(line) + len(n) + 2 > 96:
                chunks.append(line.rstrip())
                line = "    "
            line += n + ", "
        chunks.append(line.rstrip())
        chunks.append("};")
        out.append("\n".join(chunks))
out.append("""
pub(crate) fn copy_generated<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Option<Node<'d>>> {
    let copy = match node.node_tag() {""")
for name in arm_structs:
    tag = all_variants[name]
    out.append(f"""        NodeTag::{tag} => {{
            let s = node.as_variant::<{name}>().expect("{name}");
            Node::mk(mcx, copy_{name}(mcx, s)?)?
        }}""")
out.append("""        _ => return Ok(None),
    };
    Ok(Some(copy))
}

fn copy_node_opt<'d>(mcx: Mcx<'d>, node: Option<Node<'_>>) -> PgResult<Option<Node<'d>>> {
    match node {
        Some(n) => Ok(Some(copy_node(mcx, n)?)),
        None => Ok(None),
    }
}

fn copy_node_list<'d>(mcx: Mcx<'d>, list: &NodeList<'_>) -> PgResult<NodeList<'d>> {
    // Exact-length preallocation (C list_copy's new_list sizing): the copy
    // knows its final length, so it never rides the lappend growth curve.
    let mut out = NodeList::with_capacity(mcx, list.len())?;
    for cell in list.iter() {
        out.lappend(mcx, copy_node(mcx, cell)?)?;
    }
    Ok(out)
}

fn copy_opt_node_list<'d>(mcx: Mcx<'d>, list: &OptNodeList<'_>) -> PgResult<OptNodeList<'d>> {
    let mut out = OptNodeList::with_capacity(mcx, list.len())?;
    for cell in list.iter() {
        out.lappend(mcx, copy_node_opt(mcx, cell)?)?;
    }
    Ok(out)
}

fn copy_slice<'d, T: Copy>(mcx: Mcx<'d>, s: &[T]) -> PgResult<&'d [T]> {
    Ok(mcx::slice_in(mcx, s)?.leak())
}

// bms_copy; member-at-a-time rebuild because clone_in ties the target arena
// to the source lifetime. Cold path (per-cached-plan copy, never per-row).
pub(crate) fn copy_bms<'d>(mcx: Mcx<'d>, b: &Bitmapset<'_>) -> PgResult<Bitmapset<'d>> {
    let mut out = Bitmapset::empty();
    let mut x = b.next_member(-1);
    while x >= 0 {
        out.add_member(mcx, x)?;
        x = b.next_member(x);
    }
    Ok(out)
}

fn mk_ref<'d, T: NodeVariant<'d>>(mcx: Mcx<'d>, v: T) -> PgResult<&'d T> {
    Ok(Node::mk(mcx, v)?.as_variant::<T>().expect("fresh node tag"))
}

fn alloc_ref<'d, T>(mcx: Mcx<'d>, v: T) -> PgResult<&'d T> {
    Ok(leak_in(alloc_in(mcx, v)?))
}""")
for name in fn_structs:
    body = "\n".join(bodies[name])
    sname = "s" if fields_of(name) else "_s"
    if has_lifetime(name):
        sig = f"pub(crate) fn copy_{name}<'d>(mcx: Mcx<'d>, {sname}: &{name}<'_>) -> PgResult<{name}<'d>>"
    else:
        sig = f"pub(crate) fn copy_{name}(_mcx: Mcx<'_>, {sname}: &{name}) -> PgResult<{name}>"
    out.append(f"""
{sig} {{
    Ok({name} {{
{body}
    }})
}}""")

open(os.path.join(HERE, "src/generated.rs"), "w").write("\n".join(out) + "\n")

# ---- cross-check vs C ----
missing_struct = sorted(c_switch - set(struct_mod) - {"Integer", "Float", "Boolean", "String", "BitString", "Bitmapset", "List", "IntList", "OidList", "XidList", "ExtensibleNode"})
print(f"arms generated: {len(arm_structs)}  copy fns: {len(fn_structs)}")
print(f"C switch cases: {len(c_switch)}  no-Rust-struct (stay loud): {len(missing_struct)}")
print("  " + " ".join(missing_struct))
drift = []
for name in arm_structs:
    if name not in c_fields:
        drift.append(f"{name}: no C _copy fn parsed")
        continue
    rf = [f.removeprefix("r#") for f, _ in fields_of(name)]
    cf = c_fields[name]
    # embedded supers appear as one Rust field (plan/scan/join) vs flattened C list
    if set(rf) - set(cf) - {"plan", "scan", "join", "sort"}:
        drift.append(f"{name}: rust-only fields {sorted(set(rf) - set(cf) - {'plan','scan','join','sort'})}")
    if set(cf) - set(rf):
        drift.append(f"{name}: C-only fields {sorted(set(cf) - set(rf))}")
print(f"field drift entries: {len(drift)}")
for d in drift:
    print("  " + d)
