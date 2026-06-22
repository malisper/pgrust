//! Seam declarations for the genuinely-external boundaries of
//! `utils/adt/tsvector_op.c` whose owning units are not yet ported: the
//! `utils/array.h` element I/O of the array<->tsvector bridges, the `funcapi.h`
//! set-returning-function row emission (`tsvector_unnest`/`ts_stat1`/`ts_stat2`),
//! the `executor/spi.h` cursor used by `ts_stat_sql`, the `catalog`/`regproc`
//! text-search-configuration resolution, and the trigger-manager / `make_tsvector`
//! primitives driven by `tsvector_update_trigger`.
//!
//! Every branch decision, SQLSTATE and message text of `tsvector_op.c` stays in
//! the owner crate (`backend-utils-adt-tsvector-core`); only these cross-crate
//! calls are seamed. The owning units (funcapi SRF dispatch, the tsearch
//! `to_tsany.c` dictionary pipeline, the trigger manager) install these from
//! their own `init_seams()` when they land; until then a call panics loudly.
//!
//! The trigger-manager seams below carry an explicit [`TriggerDataRef`] handle
//! (the foreign `TriggerData *` from `fcinfo->context`, owned by the unported
//! trigger manager — same model as `backend-commands-trigger-seams` /
//! `lsn-trigfuncs`) instead of reading ambient `fcinfo`/`TriggerData` thread-local
//! state. The `SPI_fnumber` / `SPI_gettypeid` / `SPI_getbinval` column reads are
//! no longer seamed: the consumer obtains the relation's [`TupleDescData`]
//! (`rel->rd_att`) and the row [`FormedTuple`] (`tg_trigtuple` / `tg_newtuple`)
//! through the carrier seams here and calls the real `backend-executor-spi`
//! accessors directly.

use types_error::PgResult;
use types_ri_triggers::TriggerDataRef;
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::TupleDescData;

use backend_tsearch_parse::ts_parse::ParsedText;

// ===========================================================================
// Carrier types appearing in the seam signatures below.
// ===========================================================================

/// One decoded text/`"char"`-array element: its bytes, and whether it was SQL
/// NULL. Mirrors a single output of `deconstruct_array_builtin`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArrayElem {
    /// The element's value bytes (lexeme text / single `"char"` byte);
    /// meaningless if `is_null`.
    pub value: Vec<u8>,
    /// Whether this array element was SQL NULL.
    pub is_null: bool,
}

/// One row emitted by a tsvector / `ts_stat` set-returning function
/// (`tsvector_unnest`, `ts_stat1`, `ts_stat2`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SrfRow {
    /// Column 1 (`lexeme` text, or `word` for `ts_stat`).
    pub col0: Vec<u8>,
    /// Optional column 2 (`positions` int2[] datum / `ts_stat` `ndoc` text).
    pub col1: Option<Vec<u8>>,
    /// Optional column 3 (`weights` text[] datum / `ts_stat` `nentry` text).
    pub col2: Option<Vec<u8>>,
}

/// The decoded trigger-event state inspected by the deterministic checks at the
/// top of `tsvector_update_trigger`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TriggerEvent {
    /// `CALLED_AS_TRIGGER(fcinfo)`.
    pub called_as_trigger: bool,
    /// `TRIGGER_FIRED_FOR_ROW(tg_event)`.
    pub fired_for_row: bool,
    /// `TRIGGER_FIRED_BEFORE(tg_event)`.
    pub fired_before: bool,
    /// `TRIGGER_FIRED_BY_INSERT(tg_event)`.
    pub fired_by_insert: bool,
    /// `TRIGGER_FIRED_BY_UPDATE(tg_event)`.
    pub fired_by_update: bool,
}

/// Which tuple the trigger should read/modify (`tg_trigtuple` / `tg_newtuple`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TupleSource {
    /// INSERT: `trigdata->tg_trigtuple`.
    TrigTuple,
    /// UPDATE: `trigdata->tg_newtuple`.
    NewTuple,
}

// ===========================================================================
// Array element I/O (utils/array.h) — the array<->tsvector bridges.
//
// The four `deconstruct_array_builtin` / `construct_array_builtin` bridges are
// homed on the `backend-utils-adt-array-more` seams crate; only the shared
// [`ArrayElem`] carrier lives here.
// ===========================================================================

// ===========================================================================
// funcapi.h — set-returning-function row emission.
//
// `tsvector_unnest` / `ts_stat1` / `ts_stat2` return their result set one row
// per fmgr call through the `SRF_RETURN_NEXT` ValuePerCall protocol. That fmgr
// SRF dispatch substrate (build a `FuncCallContext`, materialize one result
// `Datum` per fmgr re-entry) is not yet ported; this seam stands in for the
// per-row emission until it lands. STOP/handoff: see the crate note.
// ===========================================================================

seam_core::seam!(
    /// Emit one result row of the current set-returning function.
    pub fn srf_return_next(row: SrfRow) -> PgResult<()>
);

// ===========================================================================
// executor/spi.h — the cursor execution for `ts_stat_sql`.
// ===========================================================================

seam_core::seam!(
    /// Run the `ts_stat` SQL, validate it returns a single `tsvector` column,
    /// and yield each non-null result tsvector datum's detoasted bytes.
    pub fn exec_stat_query(sql: &[u8]) -> PgResult<Vec<Vec<u8>>>
);

// ===========================================================================
// catalog/namespace.h + utils/regproc.h — config-name resolution.
// ===========================================================================

seam_core::seam!(
    /// `get_ts_config_oid(stringToQualifiedNameList(name), false)`.
    pub fn lookup_ts_config(name: &[u8]) -> PgResult<u32>
);

// ===========================================================================
// commands/trigger.h — the trigger-manager state the ported
// `tsvector_update_trigger` body reads, keyed on the explicit `TriggerData *`
// handle (owned by the unported trigger manager).
// ===========================================================================

seam_core::seam!(
    /// Decode the trigger-event state from the trigger's call context:
    /// `CALLED_AS_TRIGGER(fcinfo)` and the `TRIGGER_FIRED_*` predicates over
    /// `trigdata->tg_event`.
    pub fn trigger_event(trigdata: TriggerDataRef) -> TriggerEvent
);

seam_core::seam!(
    /// `trigdata->tg_trigger->tgnargs`.
    pub fn tgnargs(trigdata: TriggerDataRef) -> i32
);

seam_core::seam!(
    /// `trigdata->tg_trigger->tgargs[i]` — the `i`th trigger argument C-string
    /// bytes (including the terminating NUL).
    pub fn tgarg(trigdata: TriggerDataRef, i: i32) -> Vec<u8>
);

seam_core::seam!(
    /// `trigdata->tg_relation->rd_att` — the trigger relation's tuple
    /// descriptor, copied into `mcx`. The real `SPI_fnumber` / `SPI_gettypeid` /
    /// `SPI_getbinval` accessors operate over this descriptor.
    pub fn tg_relation_tupdesc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigdata: TriggerDataRef,
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    /// The trigger's working tuple — `trigdata->tg_trigtuple` (INSERT) or
    /// `trigdata->tg_newtuple` (UPDATE) — materialized into `mcx` as the
    /// [`FormedTuple`] the real `SPI_getbinval` reads.
    pub fn tg_rettuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigdata: TriggerDataRef,
        which: TupleSource,
    ) -> PgResult<FormedTuple<'mcx>>
);

seam_core::seam!(
    /// `bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber,
    /// trigdata->tg_updatedcols)`.
    pub fn updated_col(trigdata: TriggerDataRef, attnum: i32) -> bool
);

// ===========================================================================
// tsearch/to_tsany.c + access/htup_details.h — build the tsvector from the
// accumulated parse state and install it in the target column.
//
// `make_tsvector` lives in the unported `to_tsany.c`; `heap_modify_tuple_by_cols`
// is ported but the surrounding `make_tsvector(&prs)` + result-tuple plumbing
// is owned by the dictionary pipeline, so this stays seamed.
// ===========================================================================

seam_core::seam!(
    /// `rettuple = heap_modify_tuple_by_cols(rettuple, rel->rd_att, 1,
    /// &tsvector_attr_num, &TSVectorGetDatum(make_tsvector(&prs)), &false)` —
    /// build the tsvector from `prs` and install it in column
    /// `tsvector_attr_num` of the trigger's working tuple.
    pub fn make_and_install_tsvector(
        trigdata: TriggerDataRef,
        which: TupleSource,
        tsvector_attr_num: i32,
        prs: ParsedText,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `return PointerGetDatum(rettuple)` for the `update_needed == false` leg —
    /// the C function returns the *unmodified* `rettuple` (`tg_trigtuple` for
    /// INSERT / `tg_newtuple` for UPDATE) when no indexed column changed.  The
    /// fmgr frame deposits this on the BEFORE-trigger return-tuple channel before
    /// running the body, so the firing path always has a row to take back even if
    /// `make_and_install_tsvector` never fires.  When the body *does* rebuild the
    /// tsvector, `make_and_install_tsvector` overwrites this deposit.
    pub fn deposit_unmodified_rettuple(
        trigdata: TriggerDataRef,
        which: TupleSource,
    ) -> PgResult<()>
);
