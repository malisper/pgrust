//! Generic GIN opclass-support dispatch — the catalog-driven (`pg_amproc` +
//! fmgr) fallback that reaches ANY registered opclass support function, built-in
//! or extension-provided, when the typed by-OID match in [`crate::dispatch`]
//! does not own the proc OID.
//!
//! This restores C's `index_getprocinfo` → `FunctionCallNColl(&ginstate->…Fn,
//! …)` for an arbitrary opclass: the GIN AM resolved the support proc into an
//! `FmgrInfo` (here we re-resolve `flinfo.fn_oid` through [`fmgr_core::fmgr_info`]
//! to recover the real `PGFunction` resolution — built-ins are memoised, and an
//! extension C function rides its `CFuncHash` cache exactly as C's
//! `fmgr_info_C_lang`), build a real fmgr call frame, and invoke it. The C
//! `internal`-typed out-parameters cross through the frame's `internal`
//! side-channel as the [`::gin::extproc`] protocol structs; the value/query
//! argument rides the by-ref lane at slot 0 (C's `PG_GETARG_*(0)`). This is the
//! generic path that makes `gin_trgm_ops`, `btree_gin`, `hstore` GIN opclasses,
//! etc. reachable without per-opclass hardcoding.

use ::fmgr::boundary::RefPayload;
use ::fmgr::{FmgrInfo as CallFmgrInfo, FunctionCallInfoBaseData};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::primitive::Oid;
use ::types_error::{PgError, PgResult};
use ::types_tuple::heaptuple::Datum;

use ::datum::NullableDatum;
use ::gin::extproc::{
    GinConsistentInOut, GinExtractQueryOut, GinExtractValueOut, GinKey,
    GinTriConsistentInOut, GIN_EXTPROC_INTERNAL_SLOT,
};

/// Resolve `fn_oid` to its `PGFunction` resolution and run it with a call frame
/// that carries `value` (the detoasted by-ref argument) at slot 0 and the boxed
/// `internal` protocol `state` at [`GIN_EXTPROC_INTERNAL_SLOT`]. The protocol
/// struct is moved back out and returned (the body mutated it in place).
fn invoke_support<S: ::core::any::Any>(
    mcx: Mcx<'_>,
    fn_oid: Oid,
    collation: Oid,
    nargs: i16,
    value: Option<&[u8]>,
    state: S,
) -> PgResult<S> {
    // C: fmgr_info(fn_oid, &flinfo) — recover the resolved PGFunction. Built-ins
    // hit the registry; an extension C function rides load_external_function +
    // the CFuncHash cache (the dynamic-loader ported-library registry).
    let resolved = ::fmgr_core::fmgr_info(mcx, fn_oid)?;

    // Build the call frame. `flinfo.fn_oid` lets a body that branches on its own
    // OID (or reads `PG_NARGS`) behave faithfully.
    let mut finfo = CallFmgrInfo::empty();
    finfo.fn_oid = fn_oid;
    finfo.fn_nargs = nargs;
    finfo.fn_strict = resolved.finfo.fn_strict;
    finfo.fn_retset = resolved.finfo.fn_retset;

    let mut fcinfo = FunctionCallInfoBaseData::new(
        Some(Box::new(finfo)),
        nargs,
        collation,
        None,
        None,
    );
    // The flexible args[] array: one slot per declared argument. The opclass
    // support procs read arg 0 (the value/query) off the by-ref lane and the
    // remaining `internal` args off the internal lane; the bare words stay 0.
    fcinfo.args = (0..nargs.max(0)).map(|_| NullableDatum::null()).collect();
    // Arg 0 (value/query) is non-NULL when present (C's `PG_GETARG_*(0)`).
    if let Some(bytes) = value {
        if let Some(slot) = fcinfo.args.first_mut() {
            slot.isnull = false;
        }
        fcinfo.set_ref_arg(0, RefPayload::Varlena(bytes.to_vec()));
    }
    // The `internal` out-parameter protocol struct.
    fcinfo.set_internal_arg(GIN_EXTPROC_INTERNAL_SLOT, Box::new(state));

    // C: FunctionCallNColl(&flinfo, collation, …). The owned model invokes the
    // resolution through the shared dispatch chokepoint.
    let _ = ::fmgr_core::function_call_invoke(mcx, &resolved.resolution, &mut fcinfo)?;

    // Move the (mutated-in-place) protocol struct back out.
    match fcinfo.take_internal_arg(GIN_EXTPROC_INTERNAL_SLOT) {
        Some(boxed) => match boxed.downcast::<S>() {
            Ok(s) => Ok(*s),
            Err(_) => Err(PgError::error(format!(
                "GIN opclass support function (OID {fn_oid}) did not preserve its \
                 internal protocol state (extension wiring bug)"
            ))),
        },
        None => Err(PgError::error(format!(
            "GIN opclass support function (OID {fn_oid}) consumed its internal \
             protocol state without returning it (extension wiring bug)"
        ))),
    }
}

/// Wrap a [`GinKey`] back into the canonical by-value / by-ref `Datum` the GIN
/// core indexes (the opclass `opckeytype`: a by-value `int4` or a by-ref
/// varlena).
fn key_to_datum<'mcx>(mcx: Mcx<'mcx>, key: &GinKey) -> PgResult<Datum<'mcx>> {
    match key {
        GinKey::Int4(v) => Ok(Datum::from_i32(*v)),
        GinKey::Varlena(bytes) => Datum::from_byref_bytes_in(mcx, bytes),
    }
}

/// Generic `extractValue` (`extractValueFn`): nargs = 3 (`value`, `internal`,
/// `internal`) — `index_getprocinfo` resolves whichever signature the opclass
/// declared; the body reads only arg 0 + the internal lane.
pub fn extract_value<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    value: &[u8],
) -> PgResult<Option<(::mcx::PgVec<'mcx, Datum<'mcx>>, ::mcx::PgVec<'mcx, bool>)>> {
    let out = invoke_support(
        mcx,
        fn_oid,
        collation,
        3,
        Some(value),
        GinExtractValueOut::default(),
    )?;

    let mut elems: ::mcx::PgVec<'mcx, Datum<'mcx>> = ::mcx::PgVec::new_in(mcx);
    for k in &out.keys {
        elems.push(key_to_datum(mcx, k)?);
    }
    let mut nulls: ::mcx::PgVec<'mcx, bool> = ::mcx::PgVec::new_in(mcx);
    for &b in &out.null_flags {
        nulls.push(b);
    }
    Ok(Some((elems, nulls)))
}

/// Generic `extractQuery` (`extractQueryFn`): nargs = 7 (`query`, `internal`,
/// `int2`, `internal`, `internal`, `internal`, `internal`).
pub fn extract_query<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    query: &[u8],
    strategy: u16,
) -> PgResult<::ginutil_seams::GinExtractQueryResult<'mcx>> {
    let out = invoke_support(
        mcx,
        fn_oid,
        collation,
        7,
        Some(query),
        GinExtractQueryOut::new(strategy),
    )?;

    let mut query_values: ::mcx::PgVec<'mcx, Datum<'mcx>> = ::mcx::PgVec::new_in(mcx);
    for k in &out.keys {
        query_values.push(key_to_datum(mcx, k)?);
    }
    let mut null_flags: ::mcx::PgVec<'mcx, bool> = ::mcx::PgVec::new_in(mcx);
    for &b in &out.null_flags {
        null_flags.push(b);
    }
    let mut partial_matches: ::mcx::PgVec<'mcx, bool> = ::mcx::PgVec::new_in(mcx);
    for &b in &out.partial_matches {
        partial_matches.push(b);
    }
    let mut extra_data: ::mcx::PgVec<'mcx, Option<::mcx::PgVec<'mcx, u8>>> =
        ::mcx::PgVec::new_in(mcx);
    for slot in &out.extra_data {
        match slot {
            Some(bytes) => {
                let mut v: ::mcx::PgVec<'mcx, u8> = ::mcx::PgVec::new_in(mcx);
                for &b in bytes {
                    v.push(b);
                }
                extra_data.push(Some(v));
            }
            None => extra_data.push(None),
        }
    }

    Ok(::ginutil_seams::GinExtractQueryResult {
        query_values,
        null_flags,
        partial_matches,
        extra_data,
        search_mode: out.search_mode,
    })
}

/// Generic boolean `consistent` (`consistentFn`): nargs = 8 (`internal check`,
/// `int2 strategy`, `<query>`, `int4 nkeys`, `internal extra_data`,
/// `internal recheck`, `internal queryKeys`, `internal nullFlags`).
pub fn consistent_bool(
    fn_oid: Oid,
    collation: Oid,
    check: Vec<bool>,
    strategy: u16,
    nkeys: i32,
    extra_data: Vec<Option<Vec<u8>>>,
    query_categories: Vec<::gin::GinNullCategory>,
    query: &[u8],
) -> PgResult<(bool, bool)> {
    let scratch = MemoryContext::new("gin_ext_consistent");
    let state = GinConsistentInOut {
        check,
        strategy,
        nkeys,
        extra_data,
        query_categories,
        // C `directBoolConsistentFn` pre-seeds *recheck = true.
        recheck: true,
        matched: false,
    };
    let out = invoke_support(scratch.mcx(), fn_oid, collation, 8, Some(query), state)?;
    Ok((out.matched, out.recheck))
}

/// Generic ternary `triConsistent` (`triConsistentFn`): nargs = 7 (`internal
/// check`, `int2 strategy`, `<query>`, `int4 nkeys`, `internal extra_data`,
/// `internal queryKeys`, `internal nullFlags`).
pub fn consistent_tri(
    fn_oid: Oid,
    collation: Oid,
    check: Vec<::gin::GinTernaryValue>,
    strategy: u16,
    nkeys: i32,
    extra_data: Vec<Option<Vec<u8>>>,
    query_categories: Vec<::gin::GinNullCategory>,
    query: &[u8],
) -> PgResult<::gin::GinTernaryValue> {
    let scratch = MemoryContext::new("gin_ext_triconsistent");
    let state = GinTriConsistentInOut {
        check,
        strategy,
        nkeys,
        extra_data,
        query_categories,
        result: ::gin::GIN_MAYBE,
    };
    let out = invoke_support(scratch.mcx(), fn_oid, collation, 7, Some(query), state)?;
    Ok(out.result)
}
