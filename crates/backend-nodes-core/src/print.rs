//! Family: **print** — `nodes/print.c`, debug/EXPLAIN node dumping.
//!
//! `print` / `pprint` / `elog_node_display` / `format_node_dump` /
//! `pretty_format_node_dump` / `print_rt` / `print_expr` / `print_pathkeys` /
//! `print_tl` / `print_slot`. These render a node tree (via the `outfuncs`
//! serializer + ad-hoc expr printers) to stderr / a StringInfo. Allocating
//! formatters take `Mcx`/return `PgResult`.
//!
//! Builds on value+core (node identity) and outfuncs (rendering). Skeleton:
//! the printers land when filled.

#![allow(unused)]

/// Family marker — the node-printing routines land here. See module docs.
pub fn print_family_unimplemented() -> ! {
    todo!("print: nodes/print.c not yet ported (decomp family)")
}
