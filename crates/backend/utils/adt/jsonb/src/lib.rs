//! jsonb core + tier 2: the on-disk JEntry tree, I/O via the shared JSON
//! lexer, the operator slice, btree comparison and hash opclass support, the
//! mutation family (set/insert/delete/concat), jsonb_pretty, scalar casts.
//! Loud lanes (unported-OID fmgr panic): jsonpath, subscripting, GIN,
//! jbvDatetime, the *_strict/_unique aggregate variants.

pub mod aggs;
pub mod build;
pub mod builtins;
pub mod container;
pub mod getfield;
pub mod io;
pub mod iter;
pub mod mutate;
pub mod ops;
pub mod srfs;
pub mod tojsonb;
#[cfg(test)]
mod tests;

pub fn init_seams() {}
