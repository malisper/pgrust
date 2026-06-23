//! `FormData_pg_sequence` (`catalog/pg_sequence.h`) — the "sequence" system
//! catalog row, trimmed to the fields sequence.c consumes (all of them: the
//! catalog has exactly these eight columns).
//!
//! `init_params` fills this struct on the stack as it processes a CREATE/ALTER
//! SEQUENCE option list, so it derives `Default` + `Clone`.

use ::types_core::primitive::Oid;

/// `FormData_pg_sequence` (`catalog/pg_sequence.h`):
///
/// ```c
/// CATALOG(pg_sequence,2224,SequenceRelationId)
/// {
///     Oid     seqrelid BKI_LOOKUP(pg_class);
///     Oid     seqtypid BKI_LOOKUP(pg_type);
///     int64   seqstart;
///     int64   seqincrement;
///     int64   seqmax;
///     int64   seqmin;
///     int64   seqcache;
///     bool    seqcycle;
/// } FormData_pg_sequence;
/// ```
#[derive(Default, Clone, Debug)]
pub struct FormData_pg_sequence {
    /// `seqrelid` — pg_class OID of the sequence relation.
    pub seqrelid: Oid,
    /// `seqtypid` — pg_type OID of the sequence's data type.
    pub seqtypid: Oid,
    /// `seqstart` — START value.
    pub seqstart: i64,
    /// `seqincrement` — INCREMENT BY value.
    pub seqincrement: i64,
    /// `seqmax` — MAXVALUE.
    pub seqmax: i64,
    /// `seqmin` — MINVALUE.
    pub seqmin: i64,
    /// `seqcache` — CACHE value.
    pub seqcache: i64,
    /// `seqcycle` — CYCLE flag.
    pub seqcycle: bool,
}
