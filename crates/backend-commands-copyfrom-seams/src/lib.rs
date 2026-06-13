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
//! function can `ereport(ERROR)`) return `types_error::PgResult<T>`, matching
//! the C failure surface; the infallible read accessors (`relation_natts` etc.)
//! still carry `PgResult` because the underlying `RelationGetDescr` /
//! `list_length` are reached through catalog state whose lookup can error in
//! the owner.

#![allow(non_snake_case)]

use types_copy::{
    AttrInfo, AttrValue, CopyGetDataResult, CopyParseState, EncodingConversionResult,
    EscontextHandle, ExprContextHandle, ExprStateHandle, FmgrInfoSlot, ListHandle,
};
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rel::Relation;

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

/* ===========================================================================
 * List / tuple-descriptor / relcache accessors.
 * =========================================================================== */

seam_core::seam!(
    /// `list_length(cstate->attnumlist)`.
    pub fn list_length(list: ListHandle) -> PgResult<i32>
);

seam_core::seam!(
    /// `list_nth_int(cstate->attnumlist, n)`.
    pub fn list_nth_int(list: ListHandle, n: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `foreach(cur, cstate->attnumlist) { lfirst_int(cur) }`.
    pub fn attnumlist_ints(list: ListHandle) -> PgResult<Vec<i32>>
);

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
 * =========================================================================== */

seam_core::seam!(
    /// `InputFunctionCallSafe(&in_functions[m], string, typioparam, typmod,
    /// escontext, &result)`.
    pub fn input_function_call_safe(flinfo: FmgrInfoSlot, string: Option<&str>, typioparam: Oid, typmod: i32, escontext: Option<EscontextHandle>) -> PgResult<Option<Datum>>
);

seam_core::seam!(
    /// `ReceiveFunctionCall(&in_functions[m], buf, typioparam, typmod)`.
    pub fn receive_function_call(flinfo: FmgrInfoSlot, buf: Option<&[u8]>, typioparam: Oid, typmod: i32) -> PgResult<Datum>
);

seam_core::seam!(
    /// `ExecEvalExpr(defexprs[m], econtext, &isnull)`.
    pub fn exec_eval_expr(exprstate: ExprStateHandle, econtext: ExprContextHandle) -> PgResult<AttrValue>
);

seam_core::seam!(
    /// `&in_functions[m]` — the input/receive `FmgrInfo` slot for physical attr
    /// `m`.
    pub fn in_function_slot<'mcx>(cstate: &CopyParseState<'mcx>, m: i32) -> PgResult<FmgrInfoSlot>
);

seam_core::seam!(
    /// `typioparams[m]`.
    pub fn typioparam<'mcx>(cstate: &CopyParseState<'mcx>, m: i32) -> PgResult<Oid>
);

seam_core::seam!(
    /// `defexprs[m]` — `None` when the C pointer is NULL.
    pub fn defexpr<'mcx>(cstate: &CopyParseState<'mcx>, m: i32) -> PgResult<Option<ExprStateHandle>>
);

seam_core::seam!(
    /// Emit the ON_ERROR IGNORE verbose `NOTICE` (copyfromparse.c:1055-1066).
    pub fn notice_skipping_row(lineno: u64, attname: &str, attval: Option<&str>) -> PgResult<()>
);

/* ===========================================================================
 * libpq frontend — ReceiveCopyBegin (copyfromparse.c:169-187).
 * =========================================================================== */

seam_core::seam!(
    /// `ReceiveCopyBegin(cstate)`: build/send the `CopyInResponse`.
    pub fn receive_copy_begin<'mcx>(cstate: &mut CopyParseState<'mcx>, natts: i32, binary: bool) -> PgResult<()>
);
