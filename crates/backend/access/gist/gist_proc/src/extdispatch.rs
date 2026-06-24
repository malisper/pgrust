//! Generic GiST opclass-support dispatch — the catalog-driven (`pg_amproc` +
//! fmgr) fallback that reaches ANY registered opclass support function,
//! built-in or extension-provided, when the typed by-OID match in
//! [`crate`]'s `dispatch_*` functions does not own the proc OID.
//!
//! This restores C's `index_getprocinfo` → `FunctionCall{1,2,3,5}Coll(
//! &giststate->…Fn, …)` for an arbitrary opclass: the GiST AM resolved the
//! support proc into an `FmgrInfo` (here we re-resolve `flinfo.fn_oid` through
//! [`::fmgr_core::fmgr_info`] to recover the real `PGFunction` resolution —
//! built-ins are memoised, and an extension C function rides its `CFuncHash`
//! cache exactly as C's `fmgr_info_C_lang`), build a real fmgr call frame, and
//! invoke it. The C by-pointer `internal`-typed arguments (`GISTENTRY *`,
//! `GistEntryVector *`, `GIST_SPLITVEC *`, `bool *recheck`, the
//! `*size`/`*penalty` out-params, the returned `GISTENTRY *`) cross through the
//! frame's `internal` side-channel as the [`::gist::extproc`] protocol structs;
//! the `consistent`/`distance` query argument additionally rides the by-ref
//! lane at slot 1 (C's `PG_GETARG_TEXT_P(1)`). This is the generic path that
//! makes the `gist_trgm_ops`, `btree_gist`, `hstore` GiST opclasses, etc.
//! reachable without per-opclass hardcoding.

use ::fmgr::boundary::RefPayload;
use ::fmgr::{FmgrInfo as CallFmgrInfo, FunctionCallInfoBaseData};
use ::mcx::{Mcx, MemoryContext, PgBox};
use ::types_core::primitive::Oid;
use ::types_error::{PgError, PgResult};
use ::types_tuple::heaptuple::Datum;

use ::datum::NullableDatum;
use ::gist::extproc::{
    GistConsistentInOut, GistDistanceInOut, GistEntryImage, GistEntryInOut, GistPenaltyInOut,
    GistPicksplitInOut, GistSameInOut, GistUnionInOut, GIST_EXTPROC_INTERNAL_SLOT,
};
use ::gist::{GistEntryVector, GISTENTRY, GIST_SPLITVEC};
use dispatch_seams::{GistConsistentResult, GistDistanceResult, StrategyNumber};

/// Read a key `Datum` as its HEADER-FUL varlena byte image, distinguishing a
/// genuinely NULL pointer key (`DatumGetPointer(key) == NULL`, carried as
/// `Datum::ByVal(0)`) from a real by-reference image.
fn key_image(key: &Datum<'_>) -> Option<Vec<u8>> {
    match key {
        // C `DatumGetPointer(entry->key) == NULL`.
        Datum::ByVal(0) => None,
        _ => Some(key.as_ref_bytes().to_vec()),
    }
}

/// Build a [`GistEntryImage`] from a typed [`GISTENTRY`] (key as a byte image).
fn entry_image(entry: &GISTENTRY<'_>) -> GistEntryImage {
    GistEntryImage::new(key_image(&entry.key), entry.leafkey)
}

/// Wrap a key byte image back into the by-reference `Datum` the GiST core
/// indexes (the opclass key is always a by-reference varlena).
fn image_to_datum<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<Datum<'mcx>> {
    Datum::from_byref_bytes_in(mcx, bytes)
}

/// Resolve `fn_oid`, build a frame carrying the boxed `internal` protocol
/// `state` at [`GIST_EXTPROC_INTERNAL_SLOT`] (and an optional by-ref `query` at
/// slot 1), invoke it, and move the (mutated-in-place) protocol struct back out.
/// `nargs` is the declared arg count of the support proc.
fn invoke_support<S: ::core::any::Any>(
    mcx: Mcx<'_>,
    fn_oid: Oid,
    collation: Oid,
    nargs: i16,
    query: Option<&[u8]>,
    state: S,
) -> PgResult<S> {
    // C: fmgr_info(fn_oid, &flinfo) — recover the resolved PGFunction. Built-ins
    // hit the registry; an extension C function rides load_external_function +
    // the CFuncHash cache (the dynamic-loader ported-library registry).
    let resolved = ::fmgr_core::fmgr_info(mcx, fn_oid)?;

    let mut finfo = CallFmgrInfo::empty();
    finfo.fn_oid = fn_oid;
    finfo.fn_nargs = nargs;
    finfo.fn_strict = resolved.finfo.fn_strict;
    finfo.fn_retset = resolved.finfo.fn_retset;

    let mut fcinfo =
        FunctionCallInfoBaseData::new(Some(Box::new(finfo)), nargs, collation, None, None);
    fcinfo.args = (0..nargs.max(0)).map(|_| NullableDatum::null()).collect();

    // The `internal` GISTENTRY/entryvec/splitvec protocol struct rides slot 0.
    fcinfo.set_internal_arg(GIST_EXTPROC_INTERNAL_SLOT, Box::new(state));

    // The by-ref query argument (consistent/distance) rides slot 1.
    if let Some(bytes) = query {
        if let Some(slot) = fcinfo.args.get_mut(1) {
            slot.isnull = false;
        }
        fcinfo.set_ref_arg(1, RefPayload::Varlena(bytes.to_vec()));
    }

    // C: FunctionCallNColl(&flinfo, collation, …).
    let _ = ::fmgr_core::function_call_invoke(mcx, &resolved.resolution, &mut fcinfo)?;

    match fcinfo.take_internal_arg(GIST_EXTPROC_INTERNAL_SLOT) {
        Some(boxed) => match boxed.downcast::<S>() {
            Ok(s) => Ok(*s),
            Err(_) => Err(PgError::error(format!(
                "GiST opclass support function (OID {fn_oid}) did not preserve its \
                 internal protocol state (extension wiring bug)"
            ))),
        },
        None => Err(PgError::error(format!(
            "GiST opclass support function (OID {fn_oid}) consumed its internal \
             protocol state without returning it (extension wiring bug)"
        ))),
    }
}

/// Generic `consistent` (`GIST_CONSISTENT_PROC`, nargs = 5).
pub fn consistent(
    mcx: Mcx<'_>,
    fn_oid: Oid,
    collation: Oid,
    entry: &GISTENTRY<'_>,
    is_leaf: bool,
    query: &Datum<'_>,
    strategy: StrategyNumber,
    subtype: Oid,
) -> PgResult<GistConsistentResult> {
    // `is_leaf` is C's `GIST_LEAF(entry)`, which the body computes as
    // `entry->leafkey`; the owned GISTENTRY's `leafkey` flag may not carry the
    // page leaf-ness, so override it with the AM-supplied `is_leaf`.
    let mut img = entry_image(entry);
    img.leafkey = is_leaf;
    let state = GistConsistentInOut::new(img, strategy, subtype);
    let out = invoke_support(mcx, fn_oid, collation, 5, Some(query.as_ref_bytes()), state)?;
    Ok(GistConsistentResult {
        matched: out.matched,
        recheck: out.recheck,
    })
}

/// Generic `distance` (`GIST_DISTANCE_PROC`, nargs = 5).
pub fn distance(
    mcx: Mcx<'_>,
    fn_oid: Oid,
    collation: Oid,
    entry: &GISTENTRY<'_>,
    is_leaf: bool,
    query: &Datum<'_>,
    strategy: StrategyNumber,
    subtype: Oid,
) -> PgResult<GistDistanceResult> {
    let mut img = entry_image(entry);
    img.leafkey = is_leaf;
    let state = GistDistanceInOut::new(img, strategy, subtype);
    let out = invoke_support(mcx, fn_oid, collation, 5, Some(query.as_ref_bytes()), state)?;
    Ok(GistDistanceResult {
        distance: out.distance,
        recheck: out.recheck,
    })
}

/// Generic `compress` (`GIST_COMPRESS_PROC`, nargs = 1). The leaf key may be a
/// toasted column value, so the dispatch supplies its detoasted image to the
/// body; inner keys are plain.
pub fn compress<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    // C `DatumGetTextPP(entry->key)` detoasts the leaf value; an inner key is a
    // plain stored image. Detoast the leaf so the body reads `VARDATA_ANY`.
    let img = if entry.leafkey {
        match key_image(&entry.key) {
            Some(_) => {
                let detoasted = detoast_seams::detoast_attr::call(mcx, entry.key.as_ref_bytes())?;
                GistEntryImage::new(Some(detoasted.to_vec()), true)
            }
            None => GistEntryImage::new(None, true),
        }
    } else {
        entry_image(entry)
    };
    let state = GistEntryInOut::new(img);
    let out = invoke_support(mcx, fn_oid, collation, 1, None, state)?;
    finish_entry(mcx, entry, out)
}

/// Generic `decompress` (`GIST_DECOMPRESS_PROC`, nargs = 1).
pub fn decompress<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    let state = GistEntryInOut::new(entry_image(entry));
    let out = invoke_support(mcx, fn_oid, collation, 1, None, state)?;
    finish_entry(mcx, entry, out)
}

/// Generic `fetch` (`GIST_FETCH_PROC`, nargs = 1).
pub fn fetch<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    let state = GistEntryInOut::new(entry_image(entry));
    let out = invoke_support(mcx, fn_oid, collation, 1, None, state)?;
    finish_entry(mcx, entry, out)
}

/// Materialize the result of a compress/decompress/fetch body into a new
/// [`GISTENTRY`] (or the original entry when the body passed through).
fn finish_entry<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &GISTENTRY<'mcx>,
    out: GistEntryInOut,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    if out.passthrough {
        return ::mcx::alloc_in(mcx, entry.clone());
    }
    let key = image_to_datum(mcx, &out.retval_key)?;
    let retval = GISTENTRY {
        key,
        rel: entry.rel,
        page: entry.page,
        offset: entry.offset,
        leafkey: out.retval_leafkey,
    };
    ::mcx::alloc_in(mcx, retval)
}

/// Generic `union` (`GIST_UNION_PROC`, nargs = 2).
pub fn union<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    entryvec: &GistEntryVector<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let n = entryvec.n.max(0) as usize;
    let entries: Vec<GistEntryImage> = entryvec
        .vector
        .iter()
        .take(n)
        .map(entry_image)
        .collect();
    let state = GistUnionInOut {
        entries,
        result: Vec::new(),
    };
    let out = invoke_support(mcx, fn_oid, collation, 2, None, state)?;
    image_to_datum(mcx, &out.result)
}

/// Generic `same`/`equal` (`GIST_EQUAL_PROC`, nargs = 3).
pub fn same(
    fn_oid: Oid,
    collation: Oid,
    a: &Datum<'_>,
    b: &Datum<'_>,
) -> PgResult<bool> {
    let scratch = MemoryContext::new("gist_ext_same");
    let state = GistSameInOut {
        a: key_image(a).unwrap_or_default(),
        b: key_image(b).unwrap_or_default(),
        equal: false,
    };
    let out = invoke_support(scratch.mcx(), fn_oid, collation, 3, None, state)?;
    Ok(out.equal)
}

/// Generic `penalty` (`GIST_PENALTY_PROC`, nargs = 3).
pub fn penalty(
    fn_oid: Oid,
    collation: Oid,
    origentry: &GISTENTRY<'_>,
    newentry: &GISTENTRY<'_>,
) -> PgResult<f32> {
    let scratch = MemoryContext::new("gist_ext_penalty");
    let state = GistPenaltyInOut {
        orig_key: key_image(&origentry.key).unwrap_or_default(),
        new_key: key_image(&newentry.key).unwrap_or_default(),
        penalty: 0.0,
    };
    let out = invoke_support(scratch.mcx(), fn_oid, collation, 3, None, state)?;
    Ok(out.penalty)
}

/// Generic `picksplit` (`GIST_PICKSPLIT_PROC`, nargs = 2).
pub fn picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    entryvec: &GistEntryVector<'mcx>,
    splitvec: &mut GIST_SPLITVEC<'mcx>,
) -> PgResult<()> {
    let n = entryvec.n.max(0) as usize;
    let entries: Vec<GistEntryImage> = entryvec
        .vector
        .iter()
        .take(n)
        .map(entry_image)
        .collect();
    let state = GistPicksplitInOut {
        entries,
        ..Default::default()
    };
    let out = invoke_support(mcx, fn_oid, collation, 2, None, state)?;

    splitvec.spl_left = out.spl_left;
    splitvec.spl_right = out.spl_right;
    splitvec.spl_ldatum = Some(image_to_datum(mcx, &out.spl_ldatum)?);
    splitvec.spl_ldatum_exists = false;
    splitvec.spl_rdatum = Some(image_to_datum(mcx, &out.spl_rdatum)?);
    splitvec.spl_rdatum_exists = false;
    Ok(())
}

/// Generic `options` (`GIST_OPTIONS_PROC`, nargs = 1). The relopts buffer is
/// the GiST core's local-relopts accumulator; an extension options proc fills
/// it through the by-ref lane. (Not wired through a protocol struct here yet —
/// the trgm opclass relies on the documented `siglen` default divergence, so
/// its options proc is a no-op for the default index.)
pub fn options(_fn_oid: Oid, _relopts: &mut Vec<u8>) -> PgResult<()> {
    // The default GiST index never specifies opclass options; the only opclass
    // option pg_trgm exposes (`siglen`) is deliberately not threaded (the
    // build/read paths use SIGLEN_DEFAULT — see the tsvector_ops divergence
    // note in lib.rs). A no-op leaves the default siglen in force.
    Ok(())
}
