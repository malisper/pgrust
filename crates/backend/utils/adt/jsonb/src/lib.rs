//! jsonb core + tier 2: the on-disk JEntry tree, I/O via the shared JSON
//! lexer, the operator slice, btree comparison and hash opclass support, the
//! mutation family (set/insert/delete/concat), jsonb_pretty, scalar casts.
//! Loud lanes (unported-OID fmgr panic): GIN jsonb_path_ops + jsonpath
//! strategies, the *_strict/_unique aggregate variants. The jsonpath executor
//! lives in adt_jsonpath_exec; subscripting primitives are in subs.

pub mod aggs;
pub mod build;
pub mod builtins;
pub mod container;
pub mod getfield;
pub mod gin;
pub mod io;
pub mod iter;
pub mod iterate;
pub mod mutate;
pub mod ops;
pub mod populate;
pub mod srfs;
pub mod subs;
pub mod tojsonb;
#[cfg(test)]
mod tests;

pub fn init_seams() {}
