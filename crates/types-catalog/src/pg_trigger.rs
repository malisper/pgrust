//! `pg_trigger` catalog row layout and constants (`catalog/pg_trigger.h`,
//! PostgreSQL 18.3), trimmed to what `RelationBuildTriggers` (the relcache
//! trigger-descriptor build, commands/trigger.c) reads.

use types_core::primitive::{AttrNumber, Oid};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_trigger.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `TriggerRelationId` — `pg_trigger` (OID 2620).
pub const TriggerRelationId: Oid = 2620;
/// `TriggerConstraintIndexId` — `pg_trigger_tgconstraint_index` (OID 2699),
/// btree on `(tgconstraint)`.
pub const TriggerConstraintIndexId: Oid = 2699;
/// `TriggerRelidNameIndexId` — `pg_trigger_tgrelid_tgname_index` (OID 2701),
/// the unique index on `(tgrelid, tgname)`. `RelationBuildTriggers` scans the
/// catalog under this index so triggers come back in name order.
pub const TriggerRelidNameIndexId: Oid = 2701;
/// `TriggerOidIndexId` — `pg_trigger_oid_index` (OID 2702), unique pkey on
/// `(oid)`.
pub const TriggerOidIndexId: Oid = 2702;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_trigger).
 * ======================================================================== */

pub const Anum_pg_trigger_oid: AttrNumber = 1;
pub const Anum_pg_trigger_tgrelid: AttrNumber = 2;
pub const Anum_pg_trigger_tgparentid: AttrNumber = 3;
pub const Anum_pg_trigger_tgname: AttrNumber = 4;
pub const Anum_pg_trigger_tgfoid: AttrNumber = 5;
pub const Anum_pg_trigger_tgtype: AttrNumber = 6;
pub const Anum_pg_trigger_tgenabled: AttrNumber = 7;
pub const Anum_pg_trigger_tgisinternal: AttrNumber = 8;
pub const Anum_pg_trigger_tgconstrrelid: AttrNumber = 9;
pub const Anum_pg_trigger_tgconstrindid: AttrNumber = 10;
pub const Anum_pg_trigger_tgconstraint: AttrNumber = 11;
pub const Anum_pg_trigger_tgdeferrable: AttrNumber = 12;
pub const Anum_pg_trigger_tginitdeferred: AttrNumber = 13;
pub const Anum_pg_trigger_tgnargs: AttrNumber = 14;
pub const Anum_pg_trigger_tgattr: AttrNumber = 15;
pub const Anum_pg_trigger_tgargs: AttrNumber = 16;
pub const Anum_pg_trigger_tgqual: AttrNumber = 17;
pub const Anum_pg_trigger_tgoldtable: AttrNumber = 18;
pub const Anum_pg_trigger_tgnewtable: AttrNumber = 19;

/// `Natts_pg_trigger` — number of columns.
pub const Natts_pg_trigger: usize = 19;

/* ==========================================================================
 * tgtype bits + matching macros (pg_trigger.h EXPOSE_TO_CLIENT_CODE).
 * ======================================================================== */

/// `TRIGGER_TYPE_ROW` — `1 << 0`.
pub const TRIGGER_TYPE_ROW: i16 = 1 << 0;
/// `TRIGGER_TYPE_BEFORE` — `1 << 1`.
pub const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
/// `TRIGGER_TYPE_INSERT` — `1 << 2`.
pub const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
/// `TRIGGER_TYPE_DELETE` — `1 << 3`.
pub const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
/// `TRIGGER_TYPE_UPDATE` — `1 << 4`.
pub const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
/// `TRIGGER_TYPE_TRUNCATE` — `1 << 5`.
pub const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
/// `TRIGGER_TYPE_INSTEAD` — `1 << 6`.
pub const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

/// `TRIGGER_TYPE_LEVEL_MASK` — `TRIGGER_TYPE_ROW`.
pub const TRIGGER_TYPE_LEVEL_MASK: i16 = TRIGGER_TYPE_ROW;
/// `TRIGGER_TYPE_STATEMENT` — `0`.
pub const TRIGGER_TYPE_STATEMENT: i16 = 0;

/// `TRIGGER_TYPE_TIMING_MASK` — `TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD`
/// (note the timing bits are not adjacent).
pub const TRIGGER_TYPE_TIMING_MASK: i16 = TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD;
/// `TRIGGER_TYPE_AFTER` — `0`.
pub const TRIGGER_TYPE_AFTER: i16 = 0;

/// `TRIGGER_TYPE_EVENT_MASK` — INSERT | DELETE | UPDATE | TRUNCATE.
pub const TRIGGER_TYPE_EVENT_MASK: i16 =
    TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_TRUNCATE;

/// `TRIGGER_TYPE_MATCHES(type, level, timing, event)`:
/// `((type) & (LEVEL_MASK | TIMING_MASK | event)) == (level | timing | event)`.
#[inline]
pub fn TRIGGER_TYPE_MATCHES(tgtype: i16, level: i16, timing: i16, event: i16) -> bool {
    (tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | event))
        == (level | timing | event)
}

/// `TRIGGER_FOR_ROW(type)` — `(type) & TRIGGER_TYPE_ROW`.
#[inline]
pub fn TRIGGER_FOR_ROW(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_ROW) != 0
}
/// `TRIGGER_FOR_INSERT(type)` — `(type) & TRIGGER_TYPE_INSERT`.
#[inline]
pub fn TRIGGER_FOR_INSERT(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_INSERT) != 0
}
/// `TRIGGER_FOR_DELETE(type)` — `(type) & TRIGGER_TYPE_DELETE`.
#[inline]
pub fn TRIGGER_FOR_DELETE(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_DELETE) != 0
}
/// `TRIGGER_FOR_UPDATE(type)` — `(type) & TRIGGER_TYPE_UPDATE`.
#[inline]
pub fn TRIGGER_FOR_UPDATE(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_UPDATE) != 0
}

/* ==========================================================================
 * tgenabled firing-config codes (pg_trigger.h / trigger.h).
 * ======================================================================== */

/// `TRIGGER_FIRES_ON_ORIGIN` — `'O'`, the default `tgenabled`.
pub const TRIGGER_FIRES_ON_ORIGIN: i8 = b'O' as i8;
/// `TRIGGER_FIRES_ALWAYS` — `'A'`.
pub const TRIGGER_FIRES_ALWAYS: i8 = b'A' as i8;
/// `TRIGGER_FIRES_ON_REPLICA` — `'R'`.
pub const TRIGGER_FIRES_ON_REPLICA: i8 = b'R' as i8;
/// `TRIGGER_DISABLED` — `'D'`.
pub const TRIGGER_DISABLED: i8 = b'D' as i8;
