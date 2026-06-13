//! Family: **makefuncs** — `nodes/makefuncs.c`, the node constructors.
//!
//! ~40 `make*` constructors (`makeVar`, `makeConst`, `makeBoolExpr`,
//! `makeTargetEntry`, `makeFuncExpr`, `makeRangeVar`, `makeTypeName*`,
//! `makeIndexInfo`, the JSON `makeJson*` family, …) that palloc and populate a
//! node. Allocating constructors take `Mcx`/return `PgResult` where they build
//! an `mcx`-allocated plan/expr node; the raw-parser node constructors build
//! owned plain-Rust parse nodes.
//!
//! Owns the existing canonical `backend-nodes-makefuncs-seams`
//! (`make_const_node`, `make_and_boolexpr`, `make_type_name_from_name_list`) —
//! installs them in `init_seams()` when this family is filled.
//!
//! Builds on the keystone (Bitmapset) and the value+core family. Skeleton:
//! constructors land when filled.

#![allow(unused)]

/// Family marker — the makefuncs constructors land here. See module docs.
pub fn makefuncs_family_unimplemented() -> ! {
    todo!("makefuncs: nodes/makefuncs.c not yet ported (decomp family)")
}
