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

use ::types_core::primitive::Oid;

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

/// The descriptive fields `EventTriggerSQLDropAddObject` (`event_trigger.c`)
/// computes for a dropped object before pushing the `SQLDropObject` onto the
/// current event-trigger state's `SQLDropList` — i.e. the result of
/// `obtain_object_name_namespace` (schema/name + the temp-namespace filter
/// decision), `getObjectIdentityParts` (`objidentity` + `addrnames`/`addrargs`)
/// and `getObjectTypeDescription` (`objecttype`).
///
/// `pg_event_trigger_dropped_objects` later reports these. The owning crate
/// (`backend-catalog-objectaddress`) computes this bundle natively via a seam,
/// because that is where the `ObjectProperty` table / `get_object_identity_parts`
/// / temp-namespace machinery lives; `event_trigger.c`'s caller only owns the
/// `currentEventTriggerState` list it appends to.
///
/// `report == false` mirrors the C early `return` in `obtain_object_name_namespace`
/// (an "any temp namespace" object that is not my own temp schema): the object
/// must NOT be recorded at all.
#[derive(Clone, Debug, Default)]
pub struct SqlDropObjectInfo {
    /// `obj->report` — whether the object should be recorded (`false` = skip).
    pub report: bool,
    pub schemaname: Option<String>,
    pub objname: Option<String>,
    pub objidentity: Option<String>,
    pub objecttype: Option<String>,
    /// `obj->addrnames` — `getObjectIdentityParts` qualified-name components.
    pub addrnames: Option<Vec<String>>,
    /// `obj->addrargs` — `getObjectIdentityParts` argument-type components.
    pub addrargs: Option<Vec<String>>,
    pub istemp: bool,
}
