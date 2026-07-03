//! jsonb core: the on-disk JEntry tree, I/O via the shared JSON lexer, the
//! operator slice, btree comparison and hash opclass support.
//! Loud lanes (unported-OID fmgr panic): the jsonb_set/insert/delete/concat
//! mutation family, jsonpath, subscripting, jsonb_agg/jsonb_object_agg, GIN,
//! to_jsonb/jsonb_build_*, jsonb-to-scalar casts, jsonb_pretty, jbvDatetime.

pub mod build;
pub mod builtins;
pub mod container;
pub mod getfield;
pub mod io;
pub mod iter;
pub mod ops;
#[cfg(test)]
mod tests;

pub fn init_seams() {}
