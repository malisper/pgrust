//! Seam declarations for the COPY-FROM module (`backend/commands/copyfrom.c`
//! plus `copyfromparse.c` — one PG module). The format codec is ground in the
//! ported `backend-commands-copyfromparse` parser crate; the boundaries it
//! reaches across — reads/services off the `CopyFromStateData` that the
//! not-yet-ported `copyfrom.c` owner constructs and owns — cross a seam here:
//!
//! * reading bytes from the COPY data source (`CopyGetData` legs),
//! * encoding verification / conversion,
//! * pgstat progress reporting,
//! * the list / tuple-descriptor accessors over the un-ported parse/catalog
//!   objects,
//! * the fmgr / `Datum` value layer (input/receive functions, default-expr
//!   evaluation),
//! * the libpq frontend (`ReceiveCopyBegin`).
//!
//! `backend-commands-copyfrom` (the owner) installs every one of these from its
//! `init_seams()` when it lands — delegating the deep encoding / fmgr / pgstat
//! legs to those subsystems via direct deps it can then take; calls panic
//! loudly until then. The seam-signature types live in `types_copy`.
//!
//! Allocating C functions (every leg can `palloc`, every conversion / input
//! function can `ereport(ERROR)`) return `::types_error::PgResult<T>`, matching
//! the C failure surface; the infallible read accessors (`relation_natts` etc.)
//! still carry `PgResult` because the underlying `RelationGetDescr` /
//! `list_length` are reached through catalog state whose lookup can error in
//! the owner.

#![allow(non_snake_case)]

use ::types_copy::{
    AttrInfo, AttrValue, CopyGetDataResult, CopyParseState, EncodingConversionResult,
};
use ::types_core::primitive::Oid;
use types_tuple::heaptuple::Datum;
use ::types_error::PgResult;
use ::rel::Relation;

/* ===========================================================================
 * Data source read (CopyGetData) — copyfromparse.c:244-349.
 * =========================================================================== */

seam_core::seam!(
    /// `CopyGetData` `COPY_FILE` leg (copyfromparse.c:251-259).
    pub fn copy_get_data_file<'mcx>(cstate: &CopyParseState<'mcx>, maxread: i32) -> PgResult<CopyGetDataResult>
);

seam_core::seam!(
    /// `CopyGetData` `COPY_FRONTEND` leg (copyfromparse.c:260-342).
    pub fn copy_get_data_frontend<'mcx>(cstate: &CopyParseState<'mcx>, minread: i32, maxread: i32) -> PgResult<CopyGetDataResult>
);

seam_core::seam!(
    /// `CopyGetData` `COPY_CALLBACK` leg (copyfromparse.c:343-345).
    pub fn copy_get_data_callback<'mcx>(cstate: &CopyParseState<'mcx>, minread: i32, maxread: i32) -> PgResult<CopyGetDataResult>
);

/* ===========================================================================
 * Encoding verification / conversion — copyfromparse.c:399-581.
 * =========================================================================== */

seam_core::seam!(
    /// `pg_encoding_verifymbstr(encoding, mbstr, len)`.
    pub fn pg_encoding_verifymbstr(encoding: i32, mbstr: &[u8]) -> i32
);

seam_core::seam!(
    /// `pg_encoding_max_length(encoding)`.
    pub fn pg_encoding_max_length(encoding: i32) -> i32
);

// `GetDatabaseEncoding()` is the generic mbutils accessor consumed from
// `backend-utils-mb-mbutils-seams`; it is not redeclared here.

seam_core::seam!(
    /// `pg_do_encoding_conversion_buf(proc, src_enc, dest_enc, src, dst_capacity,
    /// noError=true)`.
    pub fn pg_do_encoding_conversion_buf(conversion_proc: Oid, src_encoding: i32, dest_encoding: i32, src: &[u8], dst_capacity: i32) -> PgResult<EncodingConversionResult>
);

seam_core::seam!(
    /// `report_invalid_encoding(encoding, mbstr, len)` — always raises.
    pub fn report_invalid_encoding(encoding: i32, mbstr: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_verifymbstr(mbstr, len, noError=false)` — raises if invalid.
    pub fn pg_verifymbstr(mbstr: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_do_encoding_conversion_buf(..., noError=false)` re-run by
    /// `CopyConversionError`. Always raises.
    pub fn conversion_error_raise(conversion_proc: Oid, src_encoding: i32, dest_encoding: i32, src: &[u8], dst_capacity: i32) -> PgResult<()>
);

/* ===========================================================================
 * pgstat progress — copyfromparse.c:634.
 * =========================================================================== */

seam_core::seam!(
    /// `pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, value)`.
    pub fn pgstat_progress_update_bytes_processed(value: i64) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_progress_start_command(PROGRESS_COMMAND_COPY, relid)` (copyfrom.c:1824).
    pub fn pgstat_progress_start_command_copy(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_progress_update_param(index, val)` (copyfrom.c TUPLES_PROCESSED/
    /// TUPLES_EXCLUDED/TUPLES_SKIPPED).
    pub fn pgstat_progress_update_param(index: i32, val: i64) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_progress_update_multi_param(nparam, index[], val[])`
    /// (copyfrom.c:1901 — COMMAND/TYPE/BYTES_TOTAL).
    pub fn pgstat_progress_update_multi_param(index: &[i32], val: &[i64]) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_progress_end_command()` (copyfrom.c:1933).
    pub fn pgstat_progress_end_command() -> PgResult<()>
);

/* ===========================================================================
 * Tuple-descriptor / relcache accessors.
 *
 * NOTE: the former `list_length` / `list_nth_int` / `attnumlist_ints` seams are
 * retired — `cstate.attnumlist` is now the real `PgVec<AttrNumber>`, so the
 * codec reads its length / nth / iterates it directly.
 * =========================================================================== */

seam_core::seam!(
    /// `RelationGetDescr(rel)->natts`.
    pub fn relation_natts<'mcx>(rel: &Relation<'mcx>) -> PgResult<i32>
);

seam_core::seam!(
    /// `Form_pg_attribute att = TupleDescAttr(RelationGetDescr(rel), m)` projected
    /// to `NameStr(att->attname)` and `att->atttypmod`.
    pub fn attr_info<'mcx>(rel: &Relation<'mcx>, m: i32) -> PgResult<AttrInfo>
);

seam_core::seam!(
    /// `namestrcmp(&TupleDescAttr(RelationGetDescr(rel), m)->attname, col_name)`.
    pub fn namestrcmp_attr<'mcx>(rel: &Relation<'mcx>, m: i32, col_name: &str) -> PgResult<i32>
);

/* ===========================================================================
 * fmgr / Datum value layer.
 *
 * These re-model away the former `FmgrInfoSlot` / `ExprStateHandle` /
 * `ExprContextHandle` / `EscontextHandle` opaque tokens: the seam now takes the
 * `cstate` plus the physical-attribute index `m`, and the owner (`copyfrom.c`,
 * which holds the per-query `Mcx`, the `FmgrResolution`s, and the `EState`)
 * resolves `&cstate.in_functions[m]` / `cstate.typioparams[m]` /
 * `cstate.escontext` / `cstate.defexprs[m]` / `cstate.econtext` and dispatches
 * the real `InputFunctionCallSafe` / `ReceiveFunctionCall` /
 * `ExecEvalExprSwitchContext`. The seam owns the borrow of `&mut cstate` so it
 * can resolve `&mut cstate.escontext` for the soft-error trap.  The `_typed`
 * notes on `input_function_call_safe` are because the owner uses the Option-4
 * cstring/bytea-typed fmgr surface (`input_function_call_safe_typed`).
 *
 * The `typioparam` / `in_function_slot` / `defexpr` accessor seams are retired:
 * the codec reads `cstate.defexprs[m].is_some()` directly to detect a default,
 * and the owner reads the per-attribute arrays straight off `cstate`.
 * =========================================================================== */

seam_core::seam!(
    /// `InputFunctionCallSafe(&cstate->in_functions[m], string,
    /// cstate->typioparams[m], typmod, cstate->escontext, &result)` — returns
    /// `None` when a soft error was trapped (`Ok(false)` in the C).
    pub fn input_function_call_safe<'mcx>(cstate: &mut CopyParseState<'mcx>, m: i32, string: Option<&str>, typmod: i32) -> PgResult<Option<Datum<'mcx>>>
);

seam_core::seam!(
    /// `ReceiveFunctionCall(&cstate->in_functions[m], buf,
    /// cstate->typioparams[m], typmod)`.
    pub fn receive_function_call<'mcx>(cstate: &mut CopyParseState<'mcx>, m: i32, buf: Option<&[u8]>, typmod: i32) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `ExecEvalExpr(cstate->defexprs[m], cstate->econtext, &isnull)` — evaluate
    /// the default expression for physical attr `m` in the per-tuple context.
    pub fn exec_eval_expr<'mcx>(cstate: &mut CopyParseState<'mcx>, m: i32) -> PgResult<AttrValue<'mcx>>
);

seam_core::seam!(
    /// Emit the ON_ERROR IGNORE verbose `NOTICE` (copyfromparse.c:1055-1066).
    /// `relname` carries the COPY error-context relation name (the message is
    /// emitted while `relname_only` is set, so the CONTEXT line is `COPY rel`).
    pub fn notice_skipping_row(
        relname: &str,
        lineno: u64,
        attname: &str,
        attval: Option<&str>,
    ) -> PgResult<()>
);

/* ===========================================================================
 * libpq frontend — ReceiveCopyBegin (copyfromparse.c:169-187).
 * =========================================================================== */

seam_core::seam!(
    /// `ReceiveCopyBegin(cstate)`: build/send the `CopyInResponse`.
    pub fn receive_copy_begin<'mcx>(cstate: &mut CopyParseState<'mcx>, natts: i32, binary: bool) -> PgResult<()>
);
