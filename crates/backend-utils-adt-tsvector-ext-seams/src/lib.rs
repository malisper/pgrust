//! Seam declarations for the genuinely-external boundaries of
//! `utils/adt/tsvector_op.c` whose owning units are not yet ported: the
//! `utils/array.h` element I/O of the array<->tsvector bridges, the `funcapi.h`
//! set-returning-function row emission (`tsvector_unnest`/`ts_stat1`/`ts_stat2`),
//! the `executor/spi.h` cursor used by `ts_stat_sql`, the `catalog`/`regproc`
//! text-search-configuration resolution, and the trigger-manager / `SPI_*` /
//! dictionary-pipeline primitives driven by `tsvector_update_trigger`.
//!
//! Every branch decision, SQLSTATE and message text of `tsvector_op.c` stays in
//! the owner crate (`backend-utils-adt-tsvector-core`); only these cross-crate
//! calls are seamed. The owning units (SPI, funcapi, the tsearch dictionary
//! pipeline, the trigger manager) install these from their own `init_seams()`
//! when they land; until then a call panics loudly.

use types_error::PgResult;

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

/// A non-null `SPI_getbinval` payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinDatum {
    /// `DatumGetObjectId(datum)` — a `regconfig` OID.
    Oid(u32),
    /// `VARDATA_ANY(DatumGetTextPP(datum))` — the detoasted text bytes.
    Text(Vec<u8>),
}

/// One decoded `SPI_getbinval` result: the (possibly external) value, or NULL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinVal {
    /// `isnull == true`.
    Null,
    /// `isnull == false`; the raw datum.
    NotNull(BinDatum),
}

/// The accumulating parse state, mirroring `ParsedText prs`. The dictionary
/// pipeline owns the actual `ParsedWord` storage behind the opaque handle,
/// modelled here as an owning box of the backend's state.
#[derive(Default)]
pub struct ParseState {
    /// Opaque handle to the backend's `ParsedText`; `None` for the initial,
    /// freshly-allocated empty state.
    pub handle: Option<Box<dyn core::any::Any>>,
}

impl core::fmt::Debug for ParseState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ParseState")
            .field("handle", &self.handle.as_ref().map(|_| "<opaque>"))
            .finish()
    }
}

/// `SPI_ERROR_NOATTRIBUTE` (executor/spi.h) — returned by `SPI_fnumber` for an
/// unknown column name.
pub const SPI_ERROR_NOATTRIBUTE: i32 = -9;

// ===========================================================================
// Array element I/O (utils/array.h) — the array<->tsvector bridges.
//
// The four `deconstruct_array_builtin` / `construct_array_builtin` bridges
// (`deconstruct_text_array` / `deconstruct_char_array` / `construct_text_array`
// / `construct_int2_array`) are homed on the `backend-utils-adt-array-more`
// seams crate (the array varlena subsystem, `arrayfuncs.c`, which owns those
// primitives and now installs them); the `tsvector_op.c` consumer calls them
// through that crate. Only the shared [`ArrayElem`] carrier lives here.
// ===========================================================================

// ===========================================================================
// funcapi.h — set-returning-function row emission.
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
// commands/trigger.h + executor/spi.h + access/htup_details.h — the primitives
// the ported `tsvector_update_trigger` body drives.
// ===========================================================================

seam_core::seam!(
    /// Decode the trigger event state from the current call context.
    pub fn trigger_event() -> TriggerEvent
);

seam_core::seam!(
    /// `trigger->tgnargs`.
    pub fn tgnargs() -> i32
);

seam_core::seam!(
    /// `trigger->tgargs[i]` — the `i`th trigger argument C-string bytes
    /// (including the terminating NUL).
    pub fn tgarg(i: i32) -> Vec<u8>
);

seam_core::seam!(
    /// `SPI_fnumber(rel->rd_att, name)` — attribute number or
    /// `SPI_ERROR_NOATTRIBUTE`.
    pub fn spi_fnumber(name: &[u8]) -> i32
);

seam_core::seam!(
    /// `SPI_gettypeid(rel->rd_att, attnum)`.
    pub fn spi_gettypeid(attnum: i32) -> u32
);

seam_core::seam!(
    /// `IsBinaryCoercible(srctype, targettype)`.
    pub fn is_binary_coercible(srctype: u32, targettype: u32) -> bool
);

seam_core::seam!(
    /// `SPI_getbinval(...)` for a `regconfig` column (returns the OID datum).
    pub fn spi_getbinval_oid(tuple: TupleSource, attnum: i32) -> PgResult<BinVal>
);

seam_core::seam!(
    /// `SPI_getbinval(...)` + `DatumGetTextPP` + detoast for a text column.
    pub fn spi_getbinval_text(tuple: TupleSource, attnum: i32) -> PgResult<BinVal>
);

seam_core::seam!(
    /// `bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber,
    /// trigdata->tg_updatedcols)`.
    pub fn updated_col(attnum: i32) -> bool
);

seam_core::seam!(
    /// Allocate the initial `ParsedText` (`lenwords = 32`, `curwords = 0`).
    pub fn new_parse_state() -> ParseState
);

seam_core::seam!(
    /// `parsetext(cfgId, &prs, txt, txtlen)` — run the text-search parser +
    /// dictionary pipeline, appending the lexemes to `prs`.
    pub fn parsetext(cfg_id: u32, prs: &mut ParseState, txt: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_modify_tuple_by_cols(...)` — build the tsvector via
    /// `make_tsvector(&prs)` and install it in the target column.
    pub fn make_and_install_tsvector(
        tuple: TupleSource,
        tsvector_attr_num: i32,
        prs: ParseState,
    ) -> PgResult<()>
);
