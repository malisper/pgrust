//! Signature vocabulary for the `backend-utils-adt-ri-triggers` port
//! (`utils/adt/ri_triggers.c`): the foreign-owned object handles the RI trigger
//! procs hand back and forth across seams, plus the projected catalog-row and
//! SPI-result value types those seams produce.
//!
//! Relations cross as the real `types_rel::RelationData`/`Relation`; `Datum`,
//! `Oid` and `SnapshotData` are real types from their own crates. What remains
//! genuinely opaque here are objects whose payload belongs to subsystems
//! `ri_triggers.c` never reaches into directly: a `TupleTableSlot *` it only
//! passes to the executor's slot accessors, a `Trigger *` / `TriggerData *` the
//! trigger manager owns, and the `SPIPlanPtr` SPI keeps as `struct _SPI_plan *`
//! (opaque even in C). These are foreign handles the installing owner
//! interprets — the sanctioned semantic-opacity case.

#![no_std]
extern crate alloc;

use mcx::{PgString, PgVec};
use types_core::Oid;

/// `TupleTableSlot *`: a slot the trigger manager / executor owns. RI only ever
/// passes it to the slot accessors (`slot_getattr`, `slot_attisnull`, …); it
/// never dereferences the payload, so the handle stays opaque (the installing
/// executor owner interprets it).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TupleTableSlotRef(pub u64);

/// `Trigger *`: the `pg_trigger`-derived descriptor the trigger manager owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TriggerRef(pub u64);

/// `TriggerData *`: the `fcinfo->context` for an RI trigger invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TriggerDataRef(pub u64);

/// `SPIPlanPtr` (`struct _SPI_plan *`): a saved SPI plan. Opaque in C as well.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SpiPlanPtr(pub u64);

/// The user-id / security-context pair `Get/SetUserIdAndSecContext` round-trip
/// (`utils/init/miscinit.c`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UserContext {
    /// The current user OID (`save_userid`).
    pub userid: Oid,
    /// The security-context bitmask (`save_sec_context`).
    pub sec_context: i32,
}

/// Result of a single SPI execution: the return code plus `SPI_processed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpiExecResult {
    /// The SPI return code (`SPI_OK_SELECT`, `SPI_OK_DELETE`, …, or `< 0`).
    pub code: i32,
    /// `SPI_processed` after the call.
    pub processed: u64,
}

/// The PERIOD operators `FindFKPeriodOpers` resolves from the PK opclass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeriodOpers {
    /// `anyrange <@ anyrange`.
    pub period_contained_by_oper: Oid,
    /// `fkattr <@ range_agg(pkattr)`.
    pub agged_period_contained_by_oper: Oid,
    /// `anyrange * anyrange`.
    pub period_intersect_oper: Oid,
}

/// The FK `pg_constraint` row fields `DeconstructFkConstraintRow` fills in, plus
/// the identity/hash fields `ri_LoadConstraintInfo` reads from the catalog
/// tuple. Allocated in the caller's `Mcx` (the syscache copy), so it carries
/// `'mcx`. `conname` is the raw name bytes (no terminating NUL); the on-disk
/// layout is `Form_pg_constraint`.
#[derive(Debug)]
pub struct FkConstraintRow<'mcx> {
    /// `NameStr(conForm->conname)` — the FK constraint name bytes.
    pub conname: PgVec<'mcx, u8>,
    /// `conForm->confrelid` — referenced (PK) relation OID.
    pub pk_relid: Oid,
    /// `conForm->conrelid` — referencing (FK) relation OID.
    pub fk_relid: Oid,
    /// `conForm->confupdtype` — ON UPDATE action char.
    pub confupdtype: i8,
    /// `conForm->confdeltype` — ON DELETE action char.
    pub confdeltype: i8,
    /// `conForm->confmatchtype` — match-type char.
    pub confmatchtype: i8,
    /// `conForm->conperiod` — does the FK use PERIOD?
    pub hasperiod: bool,
    /// `conForm->conparentid` (`InvalidOid` if a root constraint).
    pub conparentid: Oid,
    /// `conForm->conindid` — the supporting unique index (for PERIOD opclass).
    pub conindid: Oid,
    /// `GetSysCacheHashValue1(CONSTROID, constraintOid)`.
    pub oid_hash_value: u32,
    /// Number of key columns (`DeconstructFkConstraintRow`'s `numfks`).
    pub nkeys: i32,
    /// FK column attnums (`conkey`).
    pub fk_attnums: PgVec<'mcx, i16>,
    /// PK column attnums (`confkey`).
    pub pk_attnums: PgVec<'mcx, i16>,
    /// PK = FK equality operators (`conpfeqop`).
    pub pf_eq_oprs: PgVec<'mcx, Oid>,
    /// PK = PK equality operators (`conppeqop`).
    pub pp_eq_oprs: PgVec<'mcx, Oid>,
    /// FK = FK equality operators (`conffeqop`).
    pub ff_eq_oprs: PgVec<'mcx, Oid>,
    /// Number of ON DELETE SET columns.
    pub ndelsetcols: i32,
    /// ON DELETE SET column attnums (`confdelsetcols`).
    pub confdelsetcols: PgVec<'mcx, i16>,
}

/// A column of a result tuple as `ri_ReportViolation` and the
/// `RI_Initial_Check` / `RI_PartitionRemove_Check` paths consume it: the
/// attribute name plus the already-rendered output text (or NULL). Allocated in
/// the caller's `Mcx`.
#[derive(Debug)]
pub struct ResultColumn<'mcx> {
    /// `NameStr(att->attname)`.
    pub name: PgVec<'mcx, u8>,
    /// `OidOutputFunctionCall(foutoid, datum)` text, or `None` when the value
    /// is NULL (C prints the literal string `"null"`).
    pub value: Option<PgString<'mcx>>,
}
