//! `pg_event_trigger` catalog row layout, attribute numbers, and the INSERT
//! carrier (`catalog/pg_event_trigger.h`, PostgreSQL 18.3).
//!
//! `pg_event_trigger` records event triggers. `CreateEventTrigger`
//! (`commands/event_trigger.c`) forms the row — `oid`, `evtname`, `evtevent`,
//! `evtowner`, `evtfoid`, `evtenabled`, and the nullable `evttags` `text[]`
//! column — and `CatalogTupleInsert`s it.  The catalog-indexing owner forms the
//! heap tuple from [`PgEventTriggerInsertRow`].

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_event_trigger.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `EventTriggerRelationId` — `pg_event_trigger` (OID 3466).
pub const EventTriggerRelationId: Oid = 3466;
/// `EventTriggerNameIndexId` — `pg_event_trigger_evtname_index` (OID 3467).
pub const EventTriggerNameIndexId: Oid = 3467;
/// `EventTriggerOidIndexId` — `pg_event_trigger_oid_index` (OID 3468).
pub const EventTriggerOidIndexId: Oid = 3468;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_event_trigger).
 * ======================================================================== */

pub const Anum_pg_event_trigger_oid: i16 = 1;
pub const Anum_pg_event_trigger_evtname: i16 = 2;
pub const Anum_pg_event_trigger_evtevent: i16 = 3;
pub const Anum_pg_event_trigger_evtowner: i16 = 4;
pub const Anum_pg_event_trigger_evtfoid: i16 = 5;
pub const Anum_pg_event_trigger_evtenabled: i16 = 6;
pub const Anum_pg_event_trigger_evttags: i16 = 7;

/// `Natts_pg_event_trigger` — number of columns (pg_event_trigger.h).
pub const Natts_pg_event_trigger: usize = 7;

/* ==========================================================================
 * Row carrier.
 * ======================================================================== */

/// The values `CreateEventTrigger` (`commands/event_trigger.c`) builds for
/// `heap_form_tuple` + `CatalogTupleInsert`. The `oid` column is freshly
/// allocated by the owner via `GetNewOidWithIndex`, so it is NOT carried here.
/// `evtenabled` is the on-disk firing-configuration byte (always
/// `TRIGGER_FIRES_ON_ORIGIN` at creation). `evttags` is the per-tag,
/// ASCII-uppercased command-tag list to encode as a `text[]` array
/// (`construct_array_builtin(..., TEXTOID)`); `None` => stored NULL.
#[derive(Clone, Debug)]
pub struct PgEventTriggerInsertRow {
    pub evtname: String,
    pub evtevent: String,
    pub evtowner: Oid,
    pub evtfoid: Oid,
    pub evtenabled: i8,
    pub evttags: Option<Vec<String>>,
}
