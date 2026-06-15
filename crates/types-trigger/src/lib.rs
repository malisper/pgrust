//! Relation trigger vocabulary (`utils/reltrigger.h`): the `Trigger` and
//! `TriggerDesc` value types.
//!
//! C deliberately keeps these in their own header `reltrigger.h` (rather than
//! `commands/trigger.h`) "so that it can be cleanly included in rel.h and other
//! places." We mirror that: this is a leaf crate (only `mcx` + `types-core`) so
//! both `types-rel` (for `RelationData.rd_trigdesc`) and `types-nodes` (for
//! `ResultRelInfo.ri_TrigDesc` and the firing-context `TriggerData`) can name
//! these without a dependency cycle.
//!
//! These are the F0 value types only — field-for-field vs C. The trigger firing
//! logic (`commands/trigger.c`) and the relcache `RelationBuildTriggers` build
//! are separate (F1) and live with their owner.

#![no_std]
extern crate alloc;

use mcx::{PgString, PgVec};
use types_core::primitive::Oid;

/// `Trigger` (`utils/reltrigger.h`): one trigger as the relcache materializes it
/// from a `pg_trigger` row. The leading `tgoid` is the trigger's own OID; the
/// remaining fields are copied from `pg_trigger` (see `pg_trigger.h`).
///
/// C pointers become the owned idiom: `char *tgname` / `char *tgqual` /
/// `char *tgoldtable` / `char *tgnewtable` are [`PgString`] (`None` is the C
/// NULL); `int16 *tgattr` and `char **tgargs` are [`PgVec`]; `char tgenabled`
/// is `i8` (the `TRIGGER_FIRES_*` char), `int16` fields are `i16`.
#[derive(Debug)]
pub struct Trigger<'mcx> {
    /// `Oid tgoid` — OID of trigger (`pg_trigger` row).
    pub tgoid: Oid,
    /// `char *tgname` — the trigger's name.
    pub tgname: PgString<'mcx>,
    /// `Oid tgfoid` — OID of the function to call.
    pub tgfoid: Oid,
    /// `int16 tgtype` — `TRIGGER_TYPE_*` bitmask (BEFORE/AFTER, ROW/STATEMENT,
    /// INSERT/DELETE/UPDATE/TRUNCATE).
    pub tgtype: i16,
    /// `char tgenabled` — replication-role firing control
    /// (`TRIGGER_FIRES_ON_ORIGIN`, `_ALWAYS`, `_ON_REPLICA`, `DISABLED`).
    pub tgenabled: i8,
    /// `bool tgisinternal` — internally generated (e.g. for a constraint)?
    pub tgisinternal: bool,
    /// `bool tgisclone` — cloned from a partitioned-table parent?
    pub tgisclone: bool,
    /// `Oid tgconstrrelid` — the other table referenced by an RI constraint
    /// trigger, or `InvalidOid`.
    pub tgconstrrelid: Oid,
    /// `Oid tgconstrindid` — the supporting unique/exclusion index, or
    /// `InvalidOid`.
    pub tgconstrindid: Oid,
    /// `Oid tgconstraint` — owning `pg_constraint` entry, or `InvalidOid`.
    pub tgconstraint: Oid,
    /// `bool tgdeferrable` — constraint trigger is deferrable?
    pub tgdeferrable: bool,
    /// `bool tginitdeferred` — constraint trigger is initially deferred?
    pub tginitdeferred: bool,
    /// `int16 tgnargs` — number of arguments to pass to the function.
    pub tgnargs: i16,
    /// `int16 tgnattr` — number of columns in `tgattr` (UPDATE OF list size).
    pub tgnattr: i16,
    /// `int16 *tgattr` — the column numbers the UPDATE OF clause names. Empty
    /// (the C NULL / zero-length array) when not an UPDATE OF trigger.
    pub tgattr: PgVec<'mcx, i16>,
    /// `char **tgargs` — the textual trigger arguments. Empty (the C NULL) when
    /// `tgnargs == 0`.
    pub tgargs: PgVec<'mcx, PgString<'mcx>>,
    /// `char *tgqual` — the WHEN qualification as nodeToString text, or `None`
    /// (the C NULL).
    pub tgqual: Option<PgString<'mcx>>,
    /// `char *tgoldtable` — OLD transition-table name, or `None` (C NULL).
    pub tgoldtable: Option<PgString<'mcx>>,
    /// `char *tgnewtable` — NEW transition-table name, or `None` (C NULL).
    pub tgnewtable: Option<PgString<'mcx>>,
}

/// `TriggerDesc` (`utils/reltrigger.h`): the per-relation trigger set the
/// relcache hangs off `rd_trigdesc`. `triggers` is the array of [`Trigger`]
/// structs; the `trig_*` booleans say whether the array contains at least one
/// trigger of each kind, so the executor can skip searching when it does not.
///
/// `Trigger *triggers` + `int numtriggers` become an owned [`PgVec`] plus the
/// retained count (kept field-for-field vs C; `numtriggers` always equals
/// `triggers.len()`).
#[derive(Debug)]
pub struct TriggerDesc<'mcx> {
    /// `Trigger *triggers` — array of [`Trigger`] structs.
    pub triggers: PgVec<'mcx, Trigger<'mcx>>,
    /// `int numtriggers` — number of array entries.
    pub numtriggers: i32,

    /// `bool trig_insert_before_row`.
    pub trig_insert_before_row: bool,
    /// `bool trig_insert_after_row`.
    pub trig_insert_after_row: bool,
    /// `bool trig_insert_instead_row`.
    pub trig_insert_instead_row: bool,
    /// `bool trig_insert_before_statement`.
    pub trig_insert_before_statement: bool,
    /// `bool trig_insert_after_statement`.
    pub trig_insert_after_statement: bool,
    /// `bool trig_update_before_row`.
    pub trig_update_before_row: bool,
    /// `bool trig_update_after_row`.
    pub trig_update_after_row: bool,
    /// `bool trig_update_instead_row`.
    pub trig_update_instead_row: bool,
    /// `bool trig_update_before_statement`.
    pub trig_update_before_statement: bool,
    /// `bool trig_update_after_statement`.
    pub trig_update_after_statement: bool,
    /// `bool trig_delete_before_row`.
    pub trig_delete_before_row: bool,
    /// `bool trig_delete_after_row`.
    pub trig_delete_after_row: bool,
    /// `bool trig_delete_instead_row`.
    pub trig_delete_instead_row: bool,
    /// `bool trig_delete_before_statement`.
    pub trig_delete_before_statement: bool,
    /// `bool trig_delete_after_statement`.
    pub trig_delete_after_statement: bool,
    /// `bool trig_truncate_before_statement` (there are no row-level TRUNCATE
    /// triggers).
    pub trig_truncate_before_statement: bool,
    /// `bool trig_truncate_after_statement`.
    pub trig_truncate_after_statement: bool,
    /// `bool trig_insert_new_table` — at least one trigger needs the INSERT NEW
    /// transition relation?
    pub trig_insert_new_table: bool,
    /// `bool trig_update_old_table`.
    pub trig_update_old_table: bool,
    /// `bool trig_update_new_table`.
    pub trig_update_new_table: bool,
    /// `bool trig_delete_old_table`.
    pub trig_delete_old_table: bool,
}

impl<'mcx> TriggerDesc<'mcx> {
    /// An empty `TriggerDesc` (no triggers, all flags false) allocated in
    /// `mcx` — the shape `RelationBuildTriggers` starts from before it populates
    /// the array and flags. (C `palloc0`s the struct.)
    pub fn new_in(mcx: mcx::Mcx<'mcx>) -> Self {
        TriggerDesc {
            triggers: PgVec::new_in(mcx),
            numtriggers: 0,
            trig_insert_before_row: false,
            trig_insert_after_row: false,
            trig_insert_instead_row: false,
            trig_insert_before_statement: false,
            trig_insert_after_statement: false,
            trig_update_before_row: false,
            trig_update_after_row: false,
            trig_update_instead_row: false,
            trig_update_before_statement: false,
            trig_update_after_statement: false,
            trig_delete_before_row: false,
            trig_delete_after_row: false,
            trig_delete_instead_row: false,
            trig_delete_before_statement: false,
            trig_delete_after_statement: false,
            trig_truncate_before_statement: false,
            trig_truncate_after_statement: false,
            trig_insert_new_table: false,
            trig_update_old_table: false,
            trig_update_new_table: false,
            trig_delete_old_table: false,
        }
    }
}
