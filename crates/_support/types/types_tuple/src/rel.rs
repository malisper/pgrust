//! The open-relation carrier (`utils/rel.h`'s `RelationData`), trimmed to the
//! fields ported consumers read.
//!
//! C's `Relation` is a typed pointer into the relcache; per docs/types.md
//! rule 6 the Rust type is the real struct, populated incrementally. The
//! relation-open seams (`relation_open` / `table_open`) return an owned copy
//! of the consumed fields allocated in the caller's `mcx`; the matching close
//! (`relation_close` / `table_close`) consumes the carrier, mirroring the C
//! contract that the pointer is dead after close.

use mcx::PgString;
use types_core::primitive::Oid;

use crate::heaptuple::TupleDesc;

/// `FormData_pg_class` (`catalog/pg_class.h`) as held in `rd_rel`, trimmed to
/// the fields ported consumers read.
///
/// NOTE: the richer ~12-field `FormData_pg_class` lives in `types-rel`, but
/// `types-rel` depends on `types-tuple` (not the reverse), so this trim cannot
/// re-export the canonical without a dependency cycle. The two structs share
/// the same field names/types for the fields present here (no drift); this is a
/// deliberate layering trim, not an accidental duplicate.
#[derive(Debug)]
pub struct FormData_pg_class<'mcx> {
    /// `NameData relname` — the relation name.
    pub relname: PgString<'mcx>,
    /// `char relkind` — see the `RELKIND_*` constants.
    pub relkind: u8,
}

/// `RelationData` (`utils/rel.h`), trimmed.
#[derive(Debug)]
pub struct RelationData<'mcx> {
    /// `Oid rd_id` — the relation's object id (`RelationGetRelid`).
    pub rd_id: Oid,
    /// `Form_pg_class rd_rel` — the RELATION (pg_class) tuple.
    pub rd_rel: FormData_pg_class<'mcx>,
    /// `TupleDesc rd_att` — the tuple descriptor (`RelationGetDescr`).
    pub rd_att: TupleDesc<'mcx>,
}
