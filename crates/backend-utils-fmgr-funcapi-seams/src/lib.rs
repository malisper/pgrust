//! Seam declarations for the `backend-utils-fmgr-funcapi` unit
//! (`utils/fmgr/funcapi.c`): the materialized set-returning-function
//! plumbing, over the owned `FunctionCallInfoBaseData` / `ReturnSetInfo`
//! shapes in `types_nodes`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_namespace::FuncArgInfo;
// The canonical unified value type (Datum-unification keystone). The seam
// signatures below take/return it (`ByVal`/`ByRef`) with the call frame's
// `'mcx` lifetime; the bare-word `types_datum::Datum` shim is retained
// elsewhere until cleanup.
use types_tuple::Datum;

seam_core::seam!(
    /// `InitMaterializedSRF(fcinfo, flags)` (funcapi.c) — set up the calling
    /// function's materialize-mode tuplestore and descriptor in the
    /// `ReturnSetInfo` at `fcinfo->resultinfo` (`setResult`/`setDesc`). Can
    /// `ereport(ERROR)` (allocation, tupledesc lookup), carried on `Err`.
    pub fn InitMaterializedSRF<'mcx>(
        fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        flags: u32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The C call `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
    /// values, nulls)` against an `InitMaterializedSRF`-prepared
    /// `ReturnSetInfo`: resolving `setResult`/`setDesc` is funcapi-owned; the
    /// append delegates to tuplestore. `values`/`nulls` mirror the C stack
    /// arrays (borrowed, no allocation at the call site). Can
    /// `ereport(ERROR)` (palloc inside tuple forming/append), carried on
    /// `Err`.
    pub fn materialized_srf_putvalues<'mcx>(
        rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes)`
    /// (funcapi.c) for the pg_proc row of `func_oid` (the C caller holds the
    /// tuple; the owned marshal re-fetches it by OID). The arrays are
    /// allocated in `mcx` (C: palloc in the current context). `Err` carries
    /// cache-lookup / deform `elog(ERROR)`s and OOM from the copies.
    pub fn get_func_arg_info<'mcx>(
        mcx: Mcx<'mcx>,
        func_oid: Oid,
    ) -> PgResult<FuncArgInfo<'mcx>>
);

seam_core::seam!(
    /// Build an anonymous record `Datum` from a row of `values`/`nulls` whose
    /// columns have the given type OIDs (`coltypes[i]` is column `i+1`): the C
    /// `CreateTemplateTupleDesc(n)` + per-column `TupleDescInitEntry(..., typ,
    /// -1, 0)` + `BlessTupleDesc` + `heap_form_tuple` + `HeapTupleGetDatum`
    /// idiom used by record-returning builtins (e.g. `pg_stat_file`). The
    /// tupledesc / tuple machinery is funcapi/heaptuple/tupdesc-owned; the
    /// result is allocated in `mcx`. `Err` carries OOM from forming the tuple.
    pub fn record_from_values<'mcx>(
        mcx: Mcx<'mcx>,
        coltypes: &[Oid],
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// The value-per-call set-returning-function machinery
    /// (`SRF_IS_FIRSTCALL`/`SRF_FIRSTCALL_INIT`/`SRF_PERCALL_SETUP`/
    /// `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` over a `FuncCallContext` with a
    /// `multi_call_memory_ctx` and `user_fctx`) is funcapi-owned and not yet
    /// modeled (only the materialize-mode tuplestore path is). The
    /// `pg_partition_tree` / `pg_partition_ancestors` / `pg_lock_status`
    /// value-SRFs cross here until that machinery lands. funcapi (the owner)
    /// INSTALLS this seam in its `init_seams()` with an EXPLICIT honest
    /// seam-and-panic body (mirror-pg-and-panic): the call panics loudly,
    /// owner-rooted, naming the missing value-per-call protocol instead of
    /// silently degrading the SRF or aborting on an uninstalled seam. Replace
    /// the body with the real per-call `FuncCallContext` machinery when ported.
    pub fn value_srf_unported() -> ()
);

seam_core::seam!(
    /// `PG_ARGISNULL(0) ? None : Some(PG_GETARG_OID(0))` against the call
    /// frame: read the optional leading `Oid` argument of a SQL-callable
    /// function (used by `pg_stat_get_subscription`'s subid filter). fmgr owns
    /// the trimmed `args`/`isnull` arrays, so the read is seamed.
    pub fn srf_arg0_oid<'mcx>(
        fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    ) -> Option<Oid>
);

seam_core::seam!(
    /// `CStringGetTextDatum(s)` (builtins.h / varlena): build a `text *`
    /// Datum from a C string, allocated in `mcx` (C: the current context). Used
    /// by `pg_stat_get_subscription` for the worker-type text column. `Err`
    /// carries OOM.
    pub fn cstring_get_text_datum<'mcx>(
        mcx: Mcx<'mcx>,
        s: &str,
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `get_expr_result_tupdesc(expr, noError)` (funcapi.c): get the tuple
    /// descriptor describing the result of an expression of composite type. Used
    /// by `ParseComplexProjection` (parse_func.c). With `no_error = true` an
    /// unresolvable result type yields `Ok(None)` (the C `NULL` return); with
    /// `no_error = false` it raises. The descriptor is owned in `mcx`.
    pub fn get_expr_result_tupdesc<'mcx>(
        mcx: Mcx<'mcx>,
        expr: Option<&types_nodes::nodes::Node<'mcx>>,
        no_error: bool,
    ) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);
