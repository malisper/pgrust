//! Family: **value + core** — `nodes/value.c` plus the `newNode`/`makeNode`/
//! `nodeTag`/`IsA` infrastructure from `nodes/nodes.h`.
//!
//! In the owned-tree model a value node is a fixed-size owned Rust value
//! (`Integer`/`Float`/`Boolean`/`StringNode`/`BitString` in `parsenodes`),
//! so the C `makeNode` (= `palloc0` + tag write) collapses to a total,
//! infallible constructor — no allocator, no seam. This family is the shared
//! node-identity foundation the `read`/`print`/`makefuncs` families build on.
//!
//! Depends on the keystone only for the `NodeTag` identity it shares.
//!
//! C functions: `makeInteger`, `makeFloat`, `makeBoolean`, `makeString`,
//! `makeBitString` (value.c) + the `newNode` infra (nodes.h). The C functions
//! each return a typed value-node pointer (`Integer *`, `Float *`, …); since a
//! value node is just a tagged member of the raw-parser `Node` enum, the owned
//! constructors return the `Node` variant directly so node identity
//! (`IsA`/`nodeTag`) is preserved.

#![allow(unused)]

use parsenodes::{BitString, Boolean, Float, Integer, Node, StringNode};

// makeInteger / makeFloat / makeBoolean / makeString / makeBitString and the
// newNode core land here; signatures are total (no Mcx, no PgResult) per the
// owned-value model.
//
// The C `newNode(size, tag)` / `makeNode(_type_)` macro pair is `palloc0` +
// tag write; in the owned model the tag is the `Node` enum discriminant, so
// "allocate a zeroed node of this type and stamp the tag" is exactly building
// the corresponding `Node` variant from its (default-initialised) carrier.

/// `makeInteger(i)` (value.c):
///
/// ```c
/// Integer *v = makeNode(Integer);
/// v->ival = i;
/// return v;
/// ```
pub fn make_integer(i: i32) -> Node {
    Node::Integer(Integer { ival: i })
}

/// `makeFloat(numericStr)` (value.c) — takes ownership of the string (C: the
/// caller passes a palloc'd `char *`, stored directly in `v->fval`).
///
/// ```c
/// Float *v = makeNode(Float);
/// v->fval = numericStr;
/// return v;
/// ```
pub fn make_float(numeric_str: String) -> Node {
    Node::Float(Float {
        fval: Some(numeric_str),
    })
}

/// `makeBoolean(val)` (value.c):
///
/// ```c
/// Boolean *v = makeNode(Boolean);
/// v->boolval = val;
/// return v;
/// ```
pub fn make_boolean(val: bool) -> Node {
    Node::Boolean(Boolean { boolval: val })
}

/// `makeString(str)` (value.c) — takes ownership of the string (C: the caller
/// passes a palloc'd `char *`, stored directly in `v->sval`).
///
/// ```c
/// String *v = makeNode(String);
/// v->sval = str;
/// return v;
/// ```
pub fn make_string(s: String) -> Node {
    Node::String(StringNode { sval: Some(s) })
}

/// `makeBitString(str)` (value.c) — takes ownership of the string (C: the
/// caller passes a palloc'd `char *`, stored directly in `v->bsval`).
///
/// ```c
/// BitString *v = makeNode(BitString);
/// v->bsval = str;
/// return v;
/// ```
pub fn make_bit_string(s: String) -> Node {
    Node::BitString(BitString { bsval: Some(s) })
}
