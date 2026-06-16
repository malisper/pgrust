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
use types_error::PgResult;
use types_array::ArrayElementDatum;
use types_nodes::fmgr::FunctionCallInfoBaseData;
// Migration target: the canonical per-attribute value enum. The former
// transitional `Datum<'mcx>` alias resolved to this exact type, so every
// seam that carried a typed per-attribute value (the deformed-slot/`Datum`
// readers) now names the canonical `Datum<'mcx>` enum directly.
use types_tuple::backend_access_common_heaptuple::Datum;
// The bare-word `Datum` shim (`types_datum::Datum(usize)`), still named at the
// raw-fmgr-ABI dispatch seams (`FunctionCallN` / `Oid*FunctionCall`) whose owner
// `backend-utils-fmgr-core` and all consumers have NOT yet migrated off the
// shim word. Those seams carry the literal call-frame `Datum` word — a by-value
// scalar or a pointer token decoded by the owner — an
// audited ABI/storage edge that must stay a bare word until its owner migrates.
// Migrating the contract here ahead of the owner would diverge it from the
// landed `types_datum::Datum`-typed `set()` closures and every consumer.
use types_datum::Datum as DatumWord;

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
    pub fn pg_getarg_datum(fcinfo: &FunctionCallInfoBaseData<'_>, n: usize) -> DatumWord
);

seam_core::seam!(
    /// The conversion-function empty-input self-test of `CreateConversionCommand`
    /// (conversioncmds.c):
    /// `OidFunctionCall6(funcoid, Int32GetDatum(from_encoding),
    /// Int32GetDatum(to_encoding), CStringGetDatum(""), CStringGetDatum(result),
    /// Int32GetDatum(0), BoolGetDatum(false))`, returning `DatumGetInt32` of the
    /// result. The fmgr owner builds the two `cstring` `Datum`s (an empty source
    /// string and a 1-byte destination buffer — the pointer-shaped framing only
    /// fmgr can synthesize) and dispatches the call. `Err` carries any
    /// `ereport(ERROR)` the conversion function raises for an unsupported
    /// encoding pair.
    pub fn conversion_proc_empty_input_test(
        funcoid: Oid,
        from_encoding: i32,
        to_encoding: i32,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// The `OidFunctionCall6` invocation of an encoding-conversion procedure in
    /// `pg_do_encoding_conversion` / `perform_default_encoding_conversion`
    /// (mbutils.c):
    /// `OidFunctionCall6(proc, Int32GetDatum(src_encoding),
    /// Int32GetDatum(dest_encoding), CStringGetDatum(src),
    /// CStringGetDatum(result), Int32GetDatum(len), BoolGetDatum(noError))`.
    ///
    /// In C the conversion function writes the NUL-terminated converted string
    /// into the caller-allocated `result` buffer (sized
    /// `len * MAX_CONVERSION_GROWTH + 1`); the fmgr owner synthesizes the two
    /// pointer-shaped `cstring` `Datum`s (only fmgr can build them) and dispatches
    /// the call. The seam returns the converted bytes (without the trailing NUL,
    /// i.e. C's `strlen(result)` worth) allocated in `mcx`. `Err` carries any
    /// `ereport(ERROR)` the conversion function raises (untranslatable character,
    /// invalid byte sequence) and palloc out-of-memory.
    pub fn convert_via_proc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        proc: Oid,
        src_encoding: i32,
        dest_encoding: i32,
        src: &[u8],
        no_error: bool,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// Like [`convert_via_proc`] but also returns the conversion function's
    /// `int4` result — the number of source bytes it consumed
    /// (`DatumGetInt32(OidFunctionCall6(...))`). `pg_unicode_to_server_noerror`
    /// dispatches with `noError = true` and tests whether the whole input was
    /// consumed. Returns `(consumed_src_bytes, converted_bytes)`.
    pub fn convert_via_proc_counted<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        proc: Oid,
        src_encoding: i32,
        dest_encoding: i32,
        src: &[u8],
        no_error: bool,
    ) -> PgResult<(i32, mcx::PgVec<'mcx, u8>)>
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
    /// `PG_GETARG_ARRAYTYPE_P(n)` / generic detoasted varlena fetch (fmgr.h):
    /// decode argument `n` as a (possibly detoasted) varlena — the on-disk
    /// image of an array / `text[]` / `bytea` argument — returning the full
    /// varlena bytes (header included) allocated in the call's current context.
    /// `untransformRelOptions` (`pg_options_to_table`,
    /// `postgresql_fdw_validator`) consumes the `text[]` image this returns.
    /// `Err` carries detoast OOM / `ereport(ERROR)`.
    pub fn pg_getarg_varlena_pp<'mcx>(
        fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> PgResult<Bytea<'mcx>>
);

seam_core::seam!(
    /// `PG_GETARG_INT64(n)` (fmgr.h): decode argument `n` as an `int8`.
    pub fn pg_getarg_int64(fcinfo: &mut FunctionCallInfoBaseData<'_>, n: usize) -> i64
);

seam_core::seam!(
    /// `PG_GETARG_BOOL(n)` (fmgr.h): decode argument `n` as a `bool`.
    pub fn pg_getarg_bool(fcinfo: &mut FunctionCallInfoBaseData<'_>, n: usize) -> bool
);

seam_core::seam!(
    /// `PG_RETURN_INT64(v)` (fmgr.h): clear `fcinfo->isnull` and return the
    /// 64-bit integer as a `Datum`.
    pub fn pg_return_int64(fcinfo: &mut FunctionCallInfoBaseData<'_>, v: i64) -> DatumWord
);

seam_core::seam!(
    /// `PG_RETURN_DATUM(v)` (fmgr.h): clear `fcinfo->isnull` and return the
    /// `Datum` unchanged (used for composite-row results
    /// `HeapTupleGetDatum(...)`).
    pub fn pg_return_datum(fcinfo: &mut FunctionCallInfoBaseData<'_>, v: DatumWord) -> DatumWord
);

seam_core::seam!(
    /// `PG_RETURN_BOOL(b)` (fmgr.h): clear `fcinfo->isnull` and return the
    /// boolean as a `Datum`.
    pub fn pg_return_bool(fcinfo: &mut FunctionCallInfoBaseData<'_>, b: bool) -> DatumWord
);

seam_core::seam!(
    /// `PG_RETURN_NULL()` (fmgr.h): set `fcinfo->isnull = true` and return a
    /// zero `Datum`. The owner widens the frame with the `isnull` flag this
    /// sets.
    pub fn pg_return_null(fcinfo: &mut FunctionCallInfoBaseData<'_>) -> DatumWord
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
        arg1: DatumWord,
    ) -> PgResult<DatumWord>
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
    /// `fmgr_info(functionId, &finfo)` (fmgr.c) — the resolving form that fills
    /// the lookup metadata the caller reads to *plan* a call (as opposed to
    /// [`fmgr_info_check`], which only verifies). Returns the populated
    /// `types-core::FmgrInfo`: `fn_oid`, `fn_nargs`, `fn_strict`, `fn_retset`,
    /// `fn_stats`, and `fn_addr` (the resolved call address as an opaque
    /// pointer word — the typed `PGFunction` is re-derived by the step-payload
    /// layer that owns that type).
    ///
    /// The executor's expression compiler (`ExecInitFunc` /
    /// agg-trans / agg-deserialize / hash / IO-coerce / rowcompare / minmax /
    /// scalararrayop builders) reads `finfo.fn_strict` / `finfo.fn_stats` to
    /// pick the `EEOP_FUNCEXPR{,_STRICT,_FUSAGE}` (etc.) opcode and stamps
    /// `finfo.fn_addr` onto the step payload. `Err` carries the C
    /// `ereport(ERROR)` (cache lookup failure, unsupported language, ...).
    /// `mcx` is the context the resolution allocates handler state in
    /// (`fmgr_info_cxt`'s `mcxt`).
    pub fn fmgr_info<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
    ) -> PgResult<types_core::fmgr::FmgrInfo>
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
    pub fn oid_function_call0(function_id: Oid) -> PgResult<DatumWord>
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
    ) -> PgResult<DatumWord>
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
    ) -> PgResult<DatumWord>
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
    ) -> PgResult<DatumWord>
);

seam_core::seam!(
    /// `OutputFunctionCall(flinfo, val)` (fmgr.c): invoke a type's text output
    /// function through an already-resolved `FmgrInfo`. The owned `FmgrInfo`
    /// carries only the resolved function's OID (the lookup key), so the owner
    /// re-resolves and calls. The argument crosses as the owned per-attribute
    /// value model (`Datum`, as the deformed-slot readers produce). The C
    /// `char *` result crosses as its NUL-excluded bytes allocated in `mcx`.
    /// `Err` carries the strict-null `elog` and whatever the output function
    /// raises.
    pub fn output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        flinfo: &types_core::fmgr::FmgrInfo,
        val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
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
        val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidSendFunctionCall(functionId, val)` (fmgr.c): one-shot lookup +
    /// call of a type's binary send function. The C argument `Datum` crosses
    /// as the owned per-attribute value model
    /// ([`types_tuple::backend_access_common_heaptuple::Datum`]); the C
    /// `bytea *` result crosses as its payload bytes with the varlena header
    /// already stripped (`VARDATA`, `VARSIZE - VARHDRSZ` bytes), allocated in
    /// `mcx`. `Err` carries the lookup failure, the strict-null `elog`, and
    /// whatever the send function raises.
    pub fn oid_send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
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
    ) -> PgResult<Option<Datum<'mcx>>>
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
    ) -> PgResult<Datum<'mcx>>
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
        val: &Datum<'_>,
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
        val: &Datum<'_>,
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
        val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
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
    ///
    /// `main_arg` is the raw `Datum` word `BackgroundWorker.bgw_main_arg`
    /// carries through the postmaster's DSM worker slot (`bgw_main_arg
    /// (Datum)`, an 8-byte stored word in that ABI layout). It stays the
    /// bare-word `types_datum::Datum` at this storage/ABI edge: the
    /// `types-bgworker` model owns the field and is not part of this
    /// migration, and the loader hands the word straight to a C-ABI
    /// `bgworker_main_type` entry point with no enum classification available.
    pub fn call_bgworker_entrypoint(
        worker: types_bgworker::BackgroundWorker,
        main_arg: types_datum::Datum,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `OidInputFunctionCall(functionId, str, typioparam, typmod)` (fmgr.c) as
    /// used by bootstrap's `InsertOneValue`: one-shot lookup + call of a type's
    /// text input function on the NUL-terminated C string `str_` (`typmod` is
    /// `-1` at bootstrap). Returns the resulting value as the canonical
    /// `Datum<'mcx>` (a by-value scalar is `ByVal`; a by-reference result is an
    /// owned `ByRef` over the input function's flattened payload bytes in `mcx`,
    /// C's `PointerGetDatum(palloc'd result)`). `Err` carries invalid input
    /// syntax, cache-lookup failure, and OOM.
    pub fn oid_input_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        str_: &str,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>
);

seam_core::seam!(
    /// `InputFunctionCall(flinfo, str, typioparam, typmod)` (fmgr.c) on a
    /// caller-cached `FmgrInfo`, returning the result classified as a
    /// [`Datum`] ready for `heap_form_tuple`. Call a type's text input
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
    ) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>
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
        arg1: DatumWord,
        arg2: DatumWord,
        arg3: DatumWord,
    ) -> PgResult<DatumWord>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(functionId, val)` (fmgr.c), raw-`Datum` form used
    /// by bootstrap's `InsertOneValue` DEBUG4 trace: one-shot lookup + call of
    /// a type's text output function on the canonical `Datum<'mcx>` it just
    /// built (its `ByVal` arm is the by-value word; its `ByRef` arm carries the
    /// referent bytes — no per-backend registry token). The C `char *` result
    /// crosses as its NUL-excluded bytes in `mcx`. `Err` carries the lookup
    /// failure, the strict-null `elog`, and whatever the output function raises.
    pub fn oid_output_function_call_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
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
        arg1: DatumWord,
        arg2: DatumWord,
    ) -> PgResult<DatumWord>
);

seam_core::seam!(
    /// `FunctionCall1Coll(flinfo, collation, arg1)` (fmgr.c) over the canonical
    /// per-attribute [`Datum`] lane, for callers that hold values as by-value
    /// scalars OR by-reference byte images (the BRIN inclusion opclass' R-tree
    /// support procedures over geometric/network/range values). A `ByVal` arg
    /// crosses as the bare machine word; a `ByRef` arg crosses as its owned
    /// detoasted bytes through the fmgr by-reference side channel. The result is
    /// the callee's `Datum` — a by-value scalar (`ByVal`) or a by-reference value
    /// allocated in `mcx` (`ByRef`). The C strict-null
    /// `elog(ERROR, "function %u returned NULL")` and whatever the function
    /// raises are carried on `Err`.
    pub fn function_call1_coll_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        collation: Oid,
        arg1: Datum<'mcx>,
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `FunctionCall2Coll(flinfo, collation, arg1, arg2)` (fmgr.c) over the
    /// canonical per-attribute [`Datum`] lane — see [`function_call1_coll_datum`].
    /// Used by the BRIN inclusion opclass (`brin_inclusion_add_value` /
    /// `_consistent` / `_union`) to invoke the cached R-tree comparison and
    /// `merge`/`mergeable`/`contains` support procedures on by-reference union
    /// and query values, receiving the merged union (also by-reference) back.
    pub fn function_call2_coll_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        collation: Oid,
        arg1: Datum<'mcx>,
        arg2: Datum<'mcx>,
    ) -> PgResult<Datum<'mcx>>
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
    ) -> PgResult<Option<DatumWord>>
);

seam_core::seam!(
    /// `pg_input_is_valid_common()` post-parse work (misc.c:804-814):
    /// `getTypeInputInfo(typoid, &typiofunc, &typioparam)` + `fmgr_info_cxt` +
    /// `InputFunctionCallSafe(&inputproc, str, typioparam, typmod, escontext,
    /// &converted)`. Attempts the soft conversion of `str` to the already-
    /// resolved type `typoid` with `typmod`, recording any soft error into
    /// `escontext`. Returns the C `bool` (true = the value is valid input).
    /// `Err` carries any hard `ereport(ERROR)` from the type-I/O resolution.
    pub fn input_is_valid_by_type(
        typoid: Oid,
        typmod: i32,
        str_: &[u8],
        escontext: &mut types_error::SoftErrorContext,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `OutputFunctionCall(&outputproc, value)` (fmgr.c) as `array_out` drives
    /// it: call the element type's text output function on a materialized
    /// element value, returning the printable bytes (NUL excluded) in `mcx`.
    /// `Err` carries the strict-null `elog` and whatever the output function
    /// raises. (Array-element form; distinct from the `Datum`-based
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
    ) -> PgResult<DatumWord>
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

// ---------------------------------------------------------------------------
// Generic raw-`Datum` fmgr dispatch driven by `tcop/fastpath.c`
// (backend-tcop-fastpath, the server side of `PQfn`). Unlike the
// record/rowtypes consumers above (which classify values as `Datum`),
// the fastpath path marshals raw argument `Datum` words straight through
// `FunctionCallInvoke`, so these seams keep the values as opaque `Datum`s.
// C's `FmgrInfo` cannot cross a seam, so each call dispatches by OID and the
// owner re-resolves (matching `oid_input_function_call` above).

seam_core::seam!(
    /// `OidInputFunctionCall(typinput, str, typioparam, typmod)` (fmgr.c) for
    /// the fastpath text-format argument path. `str_` is `None` for a NULL
    /// argument (C's `pstring == NULL`, where `argsize == -1`); the call still
    /// happens to support domains, exactly as C does. Returns the raw result
    /// `Datum`. `Err` carries invalid-input-syntax, cache-lookup failure, and
    /// OOM.
    pub fn fastpath_input_function_call(
        typinput: Oid,
        str_: Option<&str>,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<DatumWord>
);

seam_core::seam!(
    /// `OidReceiveFunctionCall(typreceive, buf, typioparam, typmod)` (fmgr.c)
    /// for the fastpath binary-format argument path. `buf` is the argument's
    /// raw payload bytes, or `None` for a NULL argument (C's `bufptr == NULL`,
    /// where `argsize == -1`). Returns the raw result `Datum` together with the
    /// number of bytes the receive function consumed from `buf`, so the caller
    /// can reproduce C's `buf->cursor != buf->len` "incorrect binary data
    /// format" check. `Err` carries the receive function's `ereport(ERROR)`s.
    pub fn fastpath_receive_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typreceive: Oid,
        buf: Option<&[u8]>,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<(DatumWord, usize)>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(typoutput, retval)` (fmgr.c) for the fastpath
    /// text-format result path: one-shot lookup + call of a type's text output
    /// function on the raw result `Datum`. The C `char *` result crosses as its
    /// NUL-excluded bytes allocated in `mcx`. `Err` carries the lookup failure,
    /// the strict-null `elog`, and whatever the output function raises.
    pub fn fastpath_output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typoutput: Oid,
        retval: DatumWord,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidSendFunctionCall(typsend, retval)` (fmgr.c) for the fastpath
    /// binary-format result path: one-shot lookup + call of a type's binary
    /// send function on the raw result `Datum`. The C `bytea *` result crosses
    /// as its payload bytes with the varlena header already stripped
    /// (`VARDATA`, `VARSIZE - VARHDRSZ` bytes), allocated in `mcx`. `Err`
    /// carries the lookup failure, the strict-null `elog`, and whatever the
    /// send function raises.
    pub fn fastpath_send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typsend: Oid,
        retval: DatumWord,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `FunctionCallInvoke(fcinfo)` (fmgr.h) for the fastpath call path: invoke
    /// the function identified by `fn_oid` (its resolved `FmgrInfo` cannot
    /// cross, so the owner re-resolves by OID) on the raw `args` under
    /// `collation` (fastpath passes `InvalidOid`). Returns the raw result
    /// `Datum` and the callee's `fcinfo->isnull` flag. fastpath has already
    /// applied the strict-null short-circuit, so this is only called when the
    /// function is to run. `Err` carries whatever the called function raises.
    pub fn fastpath_function_call_invoke(
        fn_oid: Oid,
        collation: Oid,
        args: &[types_datum::NullableDatum],
    ) -> PgResult<(DatumWord, bool)>
);

seam_core::seam!(
    /// `FunctionCallInvoke(fcinfo)` (fmgr.h) — the general arbitrary-`nargs`
    /// dispatch the executor's `EEOP_FUNCEXPR[_STRICT[_1|_2]]` /
    /// `EEOP_FUNCEXPR_FUSAGE` and the analogous fmgr-call expression steps drive
    /// (execExprInterp.c `op->d.func.fn_addr(fcinfo)` over `fcinfo->args[0..nargs]`).
    /// The resolved `FmgrInfo` the step caches cannot cross a seam, so the owner
    /// re-resolves by `fn_oid` (as the other `FunctionCallN` seams here do) and
    /// invokes it under `collation` (`fcinfo->fncollation`) on the built `args`
    /// frame. The caller (the interpreter) has already applied the strict-null
    /// short-circuit for the `_STRICT` opcodes, so this is only entered when the
    /// function is to run; `fcinfo->isnull` is cleared by the owner before the
    /// call. Returns the raw result `Datum` word and the callee's read-back
    /// `fcinfo->isnull`. `Err` carries whatever the called function raises.
    pub fn function_call_invoke(
        fn_oid: Oid,
        collation: Oid,
        args: &[types_datum::NullableDatum],
    ) -> PgResult<(DatumWord, bool)>
);

seam_core::seam!(
    /// `construct_array_builtin(datums, n, CSTRINGOID)` +
    /// `DatumGetInt32(OidFunctionCall1(typmodin, PointerGetDatum(arrtypmod)))`
    /// (parse_type.c `typenameTypeMod`): apply a type's `typmodin` function to
    /// the cstring array distilled from a `TypeName`'s typmod expressions,
    /// returning the resolved typmod. `location` is the parse location used to
    /// tag a failure (the C `setup_parser_errposition_callback` around the
    /// call). `Err` carries whatever the `typmodin` function raises.
    pub fn typmodin(typmodin: Oid, cstrings: &[String], location: i32) -> PgResult<i32>
);
