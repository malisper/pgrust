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

/* ==========================================================================
 * pg_trigger INSERT row (the typed carrier for `CreateTrigger`'s
 * `heap_form_tuple` + `CatalogTupleInsert`, mirroring policy.c's
 * `PgPolicyInsertRow`).
 * ======================================================================== */

/// The values `CreateTrigger`/`CreateTriggerFiringOn` (commands/trigger.c)
/// writes into a `pg_trigger` row. Carried across the typed
/// `catalog_tuple_insert_pg_trigger` seam; the owner forms the heap tuple
/// against the live `pg_trigger` descriptor and `CatalogTupleInsert`s it.
///
/// `tgargs` is the *raw* bytea payload C builds: each argument's bytes
/// followed by a single NUL (`arg1\0arg2\0...`), exactly what
/// `RelationBuildTriggers`' `split_tgargs` reads back. `tgattr` is the
/// `int2vector` element list (empty for a non-column-specific trigger, which
/// stores a zero-length `int2vector`). `tgqual`/`tgoldtable`/`tgnewtable` are
/// `None` for the SQL NULL.
#[derive(Clone, Debug)]
pub struct PgTriggerInsertRow {
    /// When `Some`, the trigger already exists (OR REPLACE / internal update):
    /// the `oid` and the `t_self` TID of the row to `CatalogTupleUpdate`.
    /// `None` means a fresh INSERT (the owner allocates the OID).
    pub existing: Option<(Oid, types_tuple::heaptuple::ItemPointerData)>,
    pub tgrelid: Oid,
    pub tgparentid: Oid,
    pub tgname: String,
    pub tgfoid: Oid,
    pub tgtype: i16,
    pub tgenabled: i8,
    pub tgisinternal: bool,
    pub tgconstrrelid: Oid,
    pub tgconstrindid: Oid,
    pub tgconstraint: Oid,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: i16,
    pub tgattr: Vec<i16>,
    pub tgargs: Vec<u8>,
    pub tgqual: Option<String>,
    pub tgoldtable: Option<String>,
    pub tgnewtable: Option<String>,
}

/// The columns the in-place `pg_trigger` mutator (`renametrig_internal`,
/// commands/trigger.c) scribbles on a copied tuple before re-storing it. The
/// owner re-forms the tuple at `tid` from the existing row with these fields
/// overwritten. Currently only `tgname` (the 64-byte NUL-padded `NameData`
/// image produced by `namestrcpy`) — the rename path's single mutated column.
#[derive(Clone, Debug)]
pub struct TriggerFieldUpdate {
    /// `tgname` — the new trigger name as a zero-filled `NameData` image.
    /// `None` when the mutator does not touch the name (e.g. the ALTER
    /// CONSTRAINT deferrability path).
    pub tgname: Option<[u8; 64]>,
    /// `tgdeferrable` — set by `AlterConstrTriggerDeferrability` (ALTER
    /// CONSTRAINT). `None` when the mutator does not touch deferrability.
    pub tgdeferrable: Option<bool>,
    /// `tginitdeferred` — set by `AlterConstrTriggerDeferrability`. `None` when
    /// the mutator does not touch deferrability.
    pub tginitdeferred: Option<bool>,
}
