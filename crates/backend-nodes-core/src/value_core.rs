//! Family: **value + core** — `nodes/value.c` plus the `newNode`/`makeNode`/
//! `nodeTag`/`IsA` infrastructure from `nodes/nodes.h`.
//!
//! In the owned-tree model a value node is a fixed-size owned Rust value
//! (`Integer`/`Float`/`Boolean`/`StringNode`/`BitString` in `types_nodes`),
//! so the C `makeNode` (= `palloc0` + tag write) collapses to a total,
//! infallible constructor — no allocator, no seam. This family is the shared
//! node-identity foundation the `read`/`print`/`makefuncs` families build on.
//!
//! Depends on the keystone only for the `NodeTag` identity it shares.
//!
//! C functions: `makeInteger`, `makeFloat`, `makeBoolean`, `makeString`,
//! `makeBitString` (value.c) + the `newNode` infra (nodes.h). Skeleton: the
//! value-node structs already live in `types_nodes`; the constructors land
//! when this family is filled.

#![allow(unused)]

// makeInteger / makeFloat / makeBoolean / makeString / makeBitString and the
// newNode core land here; signatures are total (no Mcx, no PgResult) per the
// owned-value model. Stubbed until this family is filled.

/// `makeInteger(i)` (value.c).
pub fn make_integer(_i: i32) {
    todo!("value_core: makeInteger")
}

/// `makeFloat(numericStr)` (value.c) — takes ownership of the string.
pub fn make_float(_numeric_str: String) {
    todo!("value_core: makeFloat")
}

/// `makeBoolean(val)` (value.c).
pub fn make_boolean(_val: bool) {
    todo!("value_core: makeBoolean")
}

/// `makeString(str)` (value.c) — takes ownership of the string.
pub fn make_string(_s: String) {
    todo!("value_core: makeString")
}

/// `makeBitString(str)` (value.c) — takes ownership of the string.
pub fn make_bit_string(_s: String) {
    todo!("value_core: makeBitString")
}
