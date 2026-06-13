//! Seam declarations for the `backend-utils-fmgr-fmgr` unit
//! (`utils/fmgr/fmgr.c`), the function-call manager.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! C's `FmgrInfo` embeds the resolved C function pointer and so cannot cross
//! a seam; callers keep the function's OID and dispatch by OID. The eager
//! lookup-failure surface of `fmgr_info` is preserved by [`fmgr_info_check`].

use types_cache::DefElemString;
use types_core::{AttrNumber, Oid};
use types_datum::varlena::Bytea;
use types_datum::Datum;
use types_error::PgResult;
use types_array::ArrayElementDatum;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::TupleValue;

seam_core::seam!(
    /// `(fcinfo->flinfo->fn_oid, fcinfo->flinfo->fn_expr)` — the function OID
    /// and call-expression node `get_call_result_type` (funcapi.c) hands to
    /// `internal_get_result_type`. Both live on the `FmgrInfo` frame the fmgr
    /// owner widens (the trimmed `FunctionCallInfoBaseData` here has no
    /// `flinfo`), so the read is seamed. `fn_expr` is `None` for the C `NULL`
    /// (no call expression — polymorphics then unresolvable); the borrow lives
    /// in the call's context. Pure read, no allocation.
    pub fn fn_oid_and_expr<'mcx>(
        fcinfo: &'mcx FunctionCallInfoBaseData<'mcx>,
    ) -> (types_core::Oid, Option<&'mcx types_nodes::nodes::Node<'mcx>>)
);

seam_core::seam!(
    /// The call's current memory context (C: `CurrentMemoryContext` at fmgr
    /// dispatch). `convert_*` helpers behind the `has_*_privilege` family
    /// allocate their transient name-list / `RangeVar` / pstrdup'd outputs in
    /// it, mirroring the C palloc. The fmgr owner derives it from the widened
    /// frame; `Mcx` is a `Copy` context handle, so this is a pure read.
    pub fn pg_call_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> mcx::Mcx<'mcx>
);

seam_core::seam!(
    /// `PG_GETARG_NAME(n)` (fmgr.h): decode argument `n` of the call frame as
    /// a `Name` and return its NUL-trimmed text. The fmgr owner widens the
    /// frame with the `args`/`isnull` fields this reads.
    pub fn pg_getarg_name(fcinfo: &mut FunctionCallInfoBaseData<'_>, n: usize) -> String
);

seam_core::seam!(
    /// `PG_GETARG_OID(n)` (fmgr.h): decode argument `n` as an `Oid`.
    pub fn pg_getarg_oid(fcinfo: &mut FunctionCallInfoBaseData<'_>, n: usize) -> Oid
);

seam_core::seam!(
    /// `PG_GETARG_INT16(n)` (fmgr.h): decode argument `n` as an `int2`
    /// (`AttrNumber` at the column-privilege call sites).
    pub fn pg_getarg_int16(fcinfo: &mut FunctionCallInfoBaseData<'_>, n: usize) -> AttrNumber
);

seam_core::seam!(
    /// `PG_NARGS()` (fmgr.h): the number of arguments in the call frame
    /// (`fcinfo->nargs`). Pure read of the widened frame. Used by
    /// `extract_variadic_args` for the non-VARIADIC argument count.
    pub fn pg_nargs(fcinfo: &FunctionCallInfoBaseData<'_>) -> i32
);

seam_core::seam!(
    /// `PG_ARGISNULL(n)` (fmgr.h): the per-argument NULL flag
    /// (`fcinfo->args[n].isnull`) of the widened frame.
    pub fn pg_argisnull(fcinfo: &FunctionCallInfoBaseData<'_>, n: usize) -> bool
);

seam_core::seam!(
    /// `PG_GETARG_DATUM(n)` (fmgr.h): the raw argument `Datum`
    /// (`fcinfo->args[n].value`) of the widened frame, taken as given with no
    /// detoasting. Used by `extract_variadic_args` for the VARIADIC array
    /// argument and the as-given non-VARIADIC datums.
    pub fn pg_getarg_datum(fcinfo: &FunctionCallInfoBaseData<'_>, n: usize) -> Datum
);

seam_core::seam!(
    /// `PG_GETARG_POINTER(n)` interpreted as the `cstring` an `unknown`-typed
    /// literal arrives as (fmgr.h / funcapi.c `extract_variadic_args`): read
    /// argument `n` as a NUL-terminated C string and return its text. Only the
    /// fmgr owner can dereference the pointer-shaped `Datum` of the widened
    /// frame.
    pub fn pg_getarg_cstring<'mcx>(
        fcinfo: &FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> &'mcx str
);

seam_core::seam!(
    /// `get_fn_expr_variadic(fcinfo->flinfo)` (fmgr.h): whether the function
    /// was called with an explicit VARIADIC argument (the flattened
    /// `fn_expr`-derived flag). The fmgr owner reads it from the widened
    /// frame's `flinfo`.
    pub fn get_fn_expr_variadic(fcinfo: &FunctionCallInfoBaseData<'_>) -> bool
);

seam_core::seam!(
    /// `get_fn_expr_argtype(fcinfo->flinfo, argnum)` (fmgr.h): the actual
    /// declared type OID of call-expression argument `argnum`, or `InvalidOid`
    /// when not determinable. Derived from the widened frame's `flinfo`.
    pub fn get_fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData<'_>, argnum: i32) -> Oid
);

seam_core::seam!(
    /// `get_fn_expr_arg_stable(fcinfo->flinfo, argnum)` (fmgr.h): whether
    /// call-expression argument `argnum` is a stable constant (so an
    /// `unknown`-typed literal can be coerced to `text`). Derived from the
    /// widened frame's `flinfo`.
    pub fn get_fn_expr_arg_stable(fcinfo: &FunctionCallInfoBaseData<'_>, argnum: i32) -> bool
);

seam_core::seam!(
    /// `PG_GETARG_TEXT_PP(n)` (fmgr.h): decode argument `n` as a (possibly
    /// detoasted) `text`, returning the varlena image allocated in the call's
    /// current context (the owner derives it from the widened frame). `Err`
    /// carries detoast OOM / `ereport(ERROR)`.
    pub fn pg_getarg_text_pp<'mcx>(
        fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> PgResult<Bytea<'mcx>>
);

seam_core::seam!(
    /// `PG_RETURN_BOOL(b)` (fmgr.h): clear `fcinfo->isnull` and return the
    /// boolean as a `Datum`.
    pub fn pg_return_bool(fcinfo: &mut FunctionCallInfoBaseData<'_>, b: bool) -> Datum
);

seam_core::seam!(
    /// `PG_RETURN_NULL()` (fmgr.h): set `fcinfo->isnull = true` and return a
    /// zero `Datum`. The owner widens the frame with the `isnull` flag this
    /// sets.
    pub fn pg_return_null(fcinfo: &mut FunctionCallInfoBaseData<'_>) -> Datum
);

seam_core::seam!(
    /// `FunctionCall1Coll(flinfo, collation, arg1)` (fmgr.c): invoke a
    /// one-argument function whose `FmgrInfo` is already resolved (the C
    /// caller keeps a resolved `FmgrInfo *`; the owned model dispatches by
    /// `fn_oid` and re-resolves at call time, as elsewhere here). Used by
    /// `ExecHashBuildSkewHash` to hash each MCV through
    /// `hashstate->skew_hashfunction` under `hashstate->skew_collation`, and by
    /// Memoize's `MemoizeHash_hash` to invoke a cache key's hash function
    /// (`DatumGetUInt32` applied by the caller). The
    /// C strict-null `elog(ERROR, "function %u returned NULL")` and whatever
    /// the function raises are carried on `Err`.
    pub fn function_call1_coll(
        function_id: Oid,
        collation: Oid,
        arg1: Datum,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `DatumGetCString(OidFunctionCall1(typmodout, Int32GetDatum(typmod)))`
    /// (the `printTypmod` invocation shape in format_type.c): call a type's
    /// `typmodout` proc on a single `int4` typmod and return the resulting
    /// cstring, copied into `mcx`. `Err` carries the strict-null
    /// `elog(ERROR, "function %u returned NULL")` and whatever the proc raises.
    pub fn typmod_out<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typmodout: Oid,
        typmod: i32,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `fmgr_info(functionId, &finfo)` (fmgr.c), lookup half only: resolve
    /// the function and fail exactly where C would (`elog(ERROR, "cache
    /// lookup failed for function %u")`, unsupported language, etc.). The
    /// owned model re-resolves at call time, so no handle is returned.
    pub fn fmgr_info_check(function_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `OidFunctionCall0(functionId)` — a zero-argument fmgr dispatch
    /// (lsyscache.c's `getSubscriptingRoutines`, which calls the type's
    /// subscripting handler with no arguments). The returned `Datum` is the
    /// handler's `const struct SubscriptRoutines *` pointer word, kept opaque
    /// (the C `DatumGetPointer` cast targets a struct whose definition lives in
    /// `nodes/subscripting.h`, outside this TU; the c2rust translation likewise
    /// types it as `*const c_void`). `Err` carries whatever the called function
    /// raises.
    pub fn oid_function_call0(function_id: Oid) -> PgResult<Datum>
);

seam_core::seam!(
    /// `OidFunctionCall1(functionId,
    /// PointerGetDatum(deserialize_deflist(...)))` — the dictionary-init
    /// invocation shape (ts_cache.c): the argument is a `List` of
    /// string-valued `DefElem`s, crossing as typed rows the owner re-forms
    /// into the node list. The returned `Datum` is the dictionary's private
    /// `dictData` pointer word — genuinely heterogeneous per-template state
    /// (the C `void *`), kept opaque. `Err` carries whatever the called
    /// function raises plus the C `elog(ERROR, "function %u returned NULL")`
    /// strict-null check.
    pub fn oid_function_call_1_deflist(
        function_id: Oid,
        options: &[DefElemString<'_>],
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `InputFunctionCallSafe(&finfo, str, typioparam, typmod, escontext,
    /// &result)` (fmgr.c) for a hard-error caller (escontext == NULL, where it
    /// is equivalent to `InputFunctionCall`): look up `function_id`'s text
    /// input function and run it on `str` (`None` is C's `NULL` cstring, which
    /// non-strict input functions accept). The C `FmgrInfo` cannot cross, so
    /// the owner re-resolves by OID. `Err` carries the lookup failure and
    /// whatever the input function raises.
    pub fn input_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        str: Option<&str>,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `ReceiveFunctionCall(&finfo, buf, typioparam, typmod)` (fmgr.c): look
    /// up `function_id`'s binary receive function and run it on `buf` (the
    /// `StringInfo` payload). The C `FmgrInfo` cannot cross, so the owner
    /// re-resolves by OID. `Err` carries the lookup failure and whatever the
    /// receive function raises.
    pub fn receive_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        buf: &[u8],
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `OutputFunctionCall(flinfo, val)` (fmgr.c): invoke a type's text output
    /// function through an already-resolved `FmgrInfo`. The owned `FmgrInfo`
    /// carries only the resolved function's OID (the lookup key), so the owner
    /// re-resolves and calls. The argument crosses as the owned per-attribute
    /// value model (`TupleValue`, as the deformed-slot readers produce). The C
    /// `char *` result crosses as its NUL-excluded bytes allocated in `mcx`.
    /// `Err` carries the strict-null `elog` and whatever the output function
    /// raises.
    pub fn output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        flinfo: &types_core::fmgr::FmgrInfo,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `SendFunctionCall(flinfo, val)` (fmgr.c): invoke a type's binary send
    /// function through an already-resolved `FmgrInfo`. The argument crosses as
    /// the owned per-attribute value model. The C `bytea *` result crosses as
    /// its payload bytes with the varlena header already stripped (`VARDATA`,
    /// `VARSIZE - VARHDRSZ` bytes) allocated in `mcx`. `Err` carries the
    /// strict-null `elog` and whatever the send function raises.
    pub fn send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        flinfo: &types_core::fmgr::FmgrInfo,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidSendFunctionCall(functionId, val)` (fmgr.c): one-shot lookup +
    /// call of a type's binary send function. The C argument `Datum` crosses
    /// as the owned per-attribute value model
    /// ([`types_tuple::backend_access_common_heaptuple::TupleValue`]); the C
    /// `bytea *` result crosses as its payload bytes with the varlena header
    /// already stripped (`VARDATA`, `VARSIZE - VARHDRSZ` bytes), allocated in
    /// `mcx`. `Err` carries the lookup failure, the strict-null `elog`, and
    /// whatever the send function raises.
    pub fn oid_send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// rowtypes.c `record_in` per-column conversion: `getTypeInputInfo(coltype,
    /// &typiofunc, &typioparam)` + `fmgr_info_cxt` + `InputFunctionCallSafe(...,
    /// column_data, typioparam, atttypmod, escontext, &result)`. `column_data`
    /// is `None` for a SQL NULL field (C passes a NULL cstring). The soft-error
    /// path is modelled by the `Option` result: `Ok(None)` means the input
    /// function reported a soft error via the `escontext` (C's
    /// `InputFunctionCallSafe` returned `false`), so the caller bails the same
    /// way C does at its `goto fail`. `Ok(Some(v))` is the converted column
    /// value (allocated in `mcx`). `Err` carries hard `ereport(ERROR)`s
    /// (catalog lookups, OOM).
    pub fn record_column_input<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        column_data: Option<&str>,
        atttypmod: i32,
        escontext: Option<&mut types_error::SoftErrorContext>,
    ) -> PgResult<Option<TupleValue<'mcx>>>
);

seam_core::seam!(
    /// rowtypes.c `record_recv` per-column conversion:
    /// `getTypeBinaryInputInfo(coltype, &typiofunc, &typioparam)` +
    /// `fmgr_info_cxt` + `ReceiveFunctionCall(..., buf, typioparam, atttypmod)`.
    /// `item` is the column's binary payload bytes, or `None` for a -1-length
    /// NULL field (C passes a NULL `StringInfo`). The owner verifies the
    /// receive function consumed the whole item buffer, raising
    /// `errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)` ("improper binary
    /// format in record column %d") with the 1-based `colno` otherwise. Result
    /// is allocated in `mcx`. `Err` carries the catalog lookups and the receive
    /// function's `ereport(ERROR)`s.
    pub fn record_column_receive<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        item: Option<&[u8]>,
        atttypmod: i32,
        colno: i32,
    ) -> PgResult<TupleValue<'mcx>>
);

seam_core::seam!(
    /// rowtypes.c `record_out` per-column conversion: `getTypeOutputInfo(coltype,
    /// &typiofunc, &typisvarlena)` + `fmgr_info_cxt` +
    /// `OutputFunctionCall(&proc, attr)`. The C `char *` result crosses as its
    /// NUL-excluded bytes allocated in `mcx`. `val` is the non-null column
    /// value. `Err` carries the catalog lookups and the output function's
    /// `ereport(ERROR)`s.
    pub fn record_column_output<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        val: &TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// rowtypes.c `record_send` per-column conversion:
    /// `getTypeBinaryOutputInfo(coltype, &typiofunc, &typisvarlena)` +
    /// `fmgr_info_cxt` + `SendFunctionCall(&proc, attr)`. The C `bytea *`
    /// result crosses as its payload bytes with the varlena header already
    /// stripped (`VARDATA`, `VARSIZE - VARHDRSZ` bytes), allocated in `mcx`.
    /// `val` is the non-null column value. `Err` carries the catalog lookups
    /// and the send function's `ereport(ERROR)`s.
    pub fn record_column_send<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        val: &TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(functionId, val)` (fmgr.c): one-shot lookup +
    /// call of a type's text output function. The C `char *` result crosses
    /// as its NUL-excluded bytes allocated in `mcx`. `Err` carries the lookup
    /// failure, the strict-null `elog`, and whatever the output function
    /// raises.
    pub fn oid_output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// The `BackgroundWorkerMain` entry-point dispatch: resolve the worker's
    /// `(bgw_library_name, bgw_function_name)` to a `bgworker_main_type` —
    /// either an internal core entry (library "postgres":
    /// `ParallelWorkerMain`/`ApplyLauncherMain`/`ApplyWorkerMain`/
    /// `ParallelApplyWorkerMain`/`TablesyncWorkerMain`) or one loaded via
    /// `load_external_function` — and call it with `worker.bgw_main_arg`. The
    /// fn-pointers live in core / loadable libraries owned by other
    /// subsystems, so the resolution and call are the loader's job. `Err`
    /// carries the FATAL "internal function not found" and any error the
    /// worker body raises.
    pub fn call_bgworker_entrypoint(
        worker: types_bgworker::BackgroundWorker,
        main_arg: types_datum::Datum,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `OidInputFunctionCall(functionId, str, typioparam, typmod)` (fmgr.c) as
    /// used by bootstrap's `InsertOneValue`: one-shot lookup + call of a type's
    /// text input function on the NUL-terminated C string `str_` (`typmod` is
    /// `-1` at bootstrap). Returns the resulting `Datum`. `Err` carries invalid
    /// input syntax, cache-lookup failure, and OOM.
    pub fn oid_input_function_call(
        function_id: Oid,
        str_: &str,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `InputFunctionCall(flinfo, str, typioparam, typmod)` (fmgr.c) on a
    /// caller-cached `FmgrInfo`, returning the result classified as a
    /// [`TupleValue`] ready for `heap_form_tuple`. Call a type's text input
    /// function on the NUL-terminated C string `str_` (`None` is C's
    /// `str == NULL`, supported so the call still happens for NULL fields to
    /// support domains), then package the resulting `Datum` as `ByVal` when
    /// `attbyval` else `ByRef` (the owner materializes the by-reference payload
    /// bytes from its registry). Used by `BuildTupleFromCStrings`, which
    /// pre-resolves the per-attribute input functions into its `AttInMetadata`;
    /// the owned `FmgrInfo` carries only `fn_oid`, so it crosses by OID and the
    /// owner re-resolves. `Err` carries invalid input syntax, the strict-NULL
    /// `elog`, and OOM.
    pub fn input_function_call_for_heap_form<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        fn_oid: Oid,
        str_: Option<&str>,
        typioparam: Oid,
        typmod: i32,
        attbyval: bool,
    ) -> PgResult<types_tuple::backend_access_common_heaptuple::TupleValue<'mcx>>
);

seam_core::seam!(
    /// `FunctionCall3(flinfo, arg1, arg2, arg3)` (fmgr.c): call the function
    /// identified by `function_id` (the caller's cached `FmgrInfo`, which
    /// cannot cross the seam, so we re-resolve by OID) with three
    /// non-collation arguments under the default (invalid) collation, returning
    /// its `Datum` result. Used by `ri_CompareWithCast` to apply a cast
    /// function `(value, typmod=-1, implicit=false)`. The C path asserts the
    /// result is non-null. Can `ereport(ERROR)`.
    pub fn function_call3(
        function_id: Oid,
        arg1: Datum,
        arg2: Datum,
        arg3: Datum,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(functionId, val)` (fmgr.c), raw-`Datum` form used
    /// by bootstrap's `InsertOneValue` DEBUG4 trace: one-shot lookup + call of
    /// a type's text output function on the bare `Datum` it just built (the
    /// typed `TupleValue` form is unavailable there). The C `char *` result
    /// crosses as its NUL-excluded bytes in `mcx`. `Err` carries the lookup
    /// failure, the strict-null `elog`, and whatever the output function raises.
    pub fn oid_output_function_call_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: Datum,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `FunctionCall2Coll(flinfo, collation, arg1, arg2)` (fmgr.c): call the
    /// function identified by `function_id` (the caller's cached `FmgrInfo`,
    /// re-resolved by OID) with two arguments under the given input
    /// `collation`, returning its `Datum` result. Used by `ri_CompareWithCast`
    /// to apply the equality/contained-by operator. The C path asserts the
    /// result is non-null. Can `ereport(ERROR)`.
    pub fn function_call2_coll(
        function_id: Oid,
        collation: Oid,
        arg1: Datum,
        arg2: Datum,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// Render the given (1-based, relation) `attnums` of a violator
    /// `TupleTableSlot` into printable [`ResultColumn`]s for
    /// `ri_ReportViolation` (`getTypeOutputInfo` + `OidOutputFunctionCall`;
    /// NULL → C's `"null"`). Allocated into `mcx`. Can `ereport(ERROR)`.
    pub fn render_slot_columns<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: types_ri_triggers::TupleTableSlotRef,
        attnums: &[i16],
    ) -> PgResult<mcx::PgVec<'mcx, types_ri_triggers::ResultColumn<'mcx>>>
);

// ---------------------------------------------------------------------------
// Element-type I/O and comparison/hash seams driven by
// `utils/adt/arrayfuncs.c` (backend-utils-adt-arrayfuncs). The array functions
// are element-type polymorphic: an element value crosses as the safe
// `ArrayElementDatum` (by-value Datum or on-disk bytes) so the fmgr owner can
// build the real `FunctionCallInfo` without aliasing the array buffer.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `InputFunctionCallSafe(&inputproc, str, typioparam, typmod, escontext,
    /// &result)` (fmgr.c) as `array_in` drives it: call the element type's
    /// input function on the NUL-excluded element substring `str_`. Returns
    /// `Ok(Some(datum))` on success, `Ok(None)` when the soft-error context
    /// caught a conversion error (C: returns `false`), or `Err` for a hard
    /// `ereport(ERROR)`.
    pub fn input_function_call_safe(
        function_id: Oid,
        str_: &str,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<Option<Datum>>
);

seam_core::seam!(
    /// `OutputFunctionCall(&outputproc, value)` (fmgr.c) as `array_out` drives
    /// it: call the element type's text output function on a materialized
    /// element value, returning the printable bytes (NUL excluded) in `mcx`.
    /// `Err` carries the strict-null `elog` and whatever the output function
    /// raises. (Array-element form; distinct from the `TupleValue`-based
    /// `output_function_call` above.)
    pub fn array_output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        value: ArrayElementDatum<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `ReceiveFunctionCall(&receiveproc, buf, typioparam, typmod)` (fmgr.c) as
    /// `array_recv` drives it: call the element type's binary receive function
    /// on the element's wire bytes, returning the element `Datum`. `Err`
    /// carries whatever the receive function raises.
    pub fn array_receive_function_call(
        function_id: Oid,
        buf: &[u8],
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `SendFunctionCall(&sendproc, value)` (fmgr.c) as `array_send` drives it:
    /// call the element type's binary send function on a materialized element
    /// value, returning the `bytea` payload (varlena header stripped) in
    /// `mcx`. `Err` carries the strict-null `elog` and whatever the send
    /// function raises. (Array-element form.)
    pub fn array_send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        value: ArrayElementDatum<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// The element equality dispatch `array_eq` / `arrayoverlap` /
    /// `array_contain_compare` use: `FunctionCall2Coll(typentry->eq_opr_finfo,
    /// collation, a, b)` (the cached equality operator finfo from
    /// `lookup_type_cache(elmtype, TYPECACHE_EQ_OPR_FINFO)`). Returns the
    /// boolean result. `function_id` is the resolved `eq_opr` proc OID. `Err`
    /// carries whatever the comparator raises.
    pub fn element_eq(
        function_id: Oid,
        collation: Oid,
        a: ArrayElementDatum<'_>,
        b: ArrayElementDatum<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// The element comparison dispatch `array_cmp` / `btarraycmp` use:
    /// `FunctionCall2Coll(typentry->cmp_proc_finfo, collation, a, b)` (the
    /// cached btree comparison proc from `lookup_type_cache(elmtype,
    /// TYPECACHE_CMP_PROC_FINFO)`). Returns the 3-way `int32` result. `Err`
    /// carries whatever the comparator raises.
    pub fn element_cmp(
        function_id: Oid,
        collation: Oid,
        a: ArrayElementDatum<'_>,
        b: ArrayElementDatum<'_>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// The element hash dispatch `hash_array` uses:
    /// `FunctionCall1Coll(typentry->hash_proc_finfo, collation, elt)` (the
    /// cached hash proc from `lookup_type_cache(elmtype,
    /// TYPECACHE_HASH_PROC_FINFO)`). Returns the `uint32` hash. `Err` carries
    /// whatever the hash function raises.
    pub fn element_hash(
        function_id: Oid,
        collation: Oid,
        value: ArrayElementDatum<'_>,
    ) -> PgResult<u32>
);

seam_core::seam!(
    /// The element extended-hash dispatch `hash_array_extended` uses:
    /// `FunctionCall2Coll(typentry->hash_extended_proc_finfo, collation, elt,
    /// seed)` (the cached extended hash proc from `lookup_type_cache(elmtype,
    /// TYPECACHE_HASH_EXTENDED_PROC_FINFO)`). Returns the `uint64` hash. `Err`
    /// carries whatever the hash function raises.
    pub fn element_hash_extended(
        function_id: Oid,
        collation: Oid,
        value: ArrayElementDatum<'_>,
        seed: u64,
    ) -> PgResult<u64>
);
