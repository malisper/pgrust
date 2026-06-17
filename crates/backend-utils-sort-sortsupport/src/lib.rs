//! Port of PostgreSQL `src/backend/utils/sort/sortsupport.c` — support routines
//! for accelerated sorting.
//!
//! The file wires a [`SortSupportData`] to a comparator: it resolves the
//! type's btree sortsupport / comparison machinery (via the catalog and fmgr)
//! and installs the comparator. In the owned model a `SortSupportData` carries
//! the resolved comparator as a `Copy` [`SortComparatorId`] token rather than a
//! C function pointer; this crate owns the registry mapping a token to the
//! `SortShimExtra` analog (the resolved [`ResolvedFmgrInfo`] + collation) and
//! interprets the token when the comparator is invoked.
//!
//! The crate installs the four `backend-utils-sort-sortsupport-seams` slots its
//! cross-cycle callers (merge join, merge append) use.

#![allow(non_snake_case)]

use std::cell::RefCell;

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_core::Oid;
use types_error::{PgError, PgResult};
// Canonical value type (`types_tuple::Datum<'mcx>`, the ByVal/ByRef enum) —
// this crate's comparator API (`apply_sort_comparator` / `comparison_shim`)
// threads canonical `Datum<'mcx>` end-to-end. It drops to the bare-word
// `types_datum::Datum` ONLY at the still-bare-word fmgr-ABI scalar edge
// (`function_call2_coll` / `oid_function_call1_coll` argument/return slots).
use types_tuple::Datum;
use types_rel::Relation;
use types_sortsupport::{
    AbbrevAbortId, AbbrevConverterId, SortComparatorId, SortSupportData, BTORDER_PROC,
    BTSORTSUPPORT_PROC, COMPARE_GT, GIST_AM_OID, GIST_SORTSUPPORT_PROC,
};

use backend_utils_fmgr_core::{fmgr_info_cxt, function_call2_coll, oid_function_call1_coll};
use types_fmgr::ResolvedFmgrInfo;

use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_relcache_seams as relcache;
// The nbtcompare `*sortsupport` seams: `run_sortsupport` (INWARD to nbtcompare,
// the by-OID dispatch we call) and the `install_sortsupport_*` seams (OUTWARD,
// owned + installed by this sort substrate, called by nbtcompare to mint the
// fast-comparator token and write it into `ssup.comparator`).
use backend_access_nbt_compare_seams as nbtcompare_seams;
use nbtcompare_seams as nbtcompare; // `FastComparator` type alias lives here.
// The gist-proc `gist_point_sortsupport` install seams (OUTWARD, owned +
// installed by this sort substrate, called by gist-proc to mint the GiST box
// comparator / abbrev tokens and write them into the `ssup` fields). The
// `GistComparator` / `GistAbbrevConverter` kernel type aliases live here.
use backend_access_gist_proc_seams as gist_proc;

/// `OidIsValid(oid)` — `InvalidOid` is 0.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

/// `elog(ERROR, ...)` analog (default internal SQLSTATE).
fn elog(message: String) -> PgError {
    PgError::error(message)
}

// ===========================================================================
// Comparator-token registry — the owned-model `SortShimExtra` storage.
//
// The C `SortShimExtra` (the per-comparator `FmgrInfo` + reusable
// `FunctionCallInfoBaseData`) is allocated in `ssup->ssup_cxt` and reached
// through `ssup->ssup_extra`. The owned `SortSupportData` carries only a
// `SortComparatorId` token, so the resolved fmgr lookup lives here, keyed by
// the token, in per-backend (thread_local) state.
// ===========================================================================

/// The resolved comparison machinery a [`SortComparatorId`] denotes.
///
/// C's `comparator` is a single `int (*)(Datum, Datum, SortSupport)` pointer.
/// Two flavors install it:
///
/// * [`Comparator::Shim`] — `PrepareSortSupportComparisonShim` set
///   `ssup->comparator = comparison_shim`, where `comparison_shim` reaches the
///   old-style btree `cmp` function through the C `SortShimExtra` (`flinfo` +
///   reusable `fcinfo`). Held here as the [`ResolvedFmgrInfo`] + collation.
/// * [`Comparator::Native`] — a type's `*sortsupport` routine set
///   `ssup->comparator = <fastcmp>` directly (`ssup_datum_int32_cmp`,
///   `btoidfastcmp`, ...). Held here as the kernel function pointer the
///   type-specific unit handed us through its install seam.
enum Comparator {
    /// `comparison_shim` over a `SortShimExtra` (old-style btree `cmp`).
    Shim {
        /// The `fmgr_info_cxt(cmpFunc, &extra->flinfo, ...)` lookup.
        resolved: ResolvedFmgrInfo,
        /// `ssup->ssup_collation` — the `InitFunctionCallInfoData` collation.
        collation: Oid,
    },
    /// A type's fast comparator installed directly by its `*sortsupport`.
    Native(nbtcompare::FastComparator),
    /// A GiST box comparator (`gist_bbox_zorder_cmp`) installed by
    /// `gist_point_sortsupport`. Held as the kernel function pointer; unlike
    /// [`Comparator::Native`] its operands are pass-by-reference `BOX` images, so
    /// the kernel reads the canonical `Datum`'s `ByRef` bytes.
    GistBox(gist_proc::GistComparator),
    /// `ssup_datum_unsigned_cmp` (sortsupport.c): the abbreviated-key comparator
    /// `gist_point_sortsupport` installs in the abbreviated arm. A substrate
    /// primitive (no per-token state) comparing the two Datum words as unsigned.
    UnsignedCmp,
}

thread_local! {
    /// Token -> comparator, indexed by `SortComparatorId.0`. A fresh push gives
    /// out the next index; the registry lives for the backend (the C state
    /// lived in `ssup_cxt`, freed when that context resets — here a reset is the
    /// process lifetime, matching how merge join keeps its `ssup` for the scan).
    static SHIMS: RefCell<Vec<Comparator>> = const { RefCell::new(Vec::new()) };
}

/// Register a resolved comparator and hand back its token.
fn register_shim(state: Comparator) -> SortComparatorId {
    SHIMS.with(|s| {
        let mut v = s.borrow_mut();
        let id = v.len() as u32;
        v.push(state);
        SortComparatorId(id)
    })
}

// ===========================================================================
// comparison_shim — the body of the installed comparator.
// ===========================================================================

/// Shim for calling an old-style comparator (C `comparison_shim`): an inlined
/// `FunctionCall2Coll` over the `SortShimExtra` reachable from the token.
///
/// C sets `extra->fcinfo.args[0/1].value`, resets `isnull`, runs
/// `FunctionCallInvoke`, and `elog(ERROR, "function %u returned NULL")` if the
/// result came back null. [`function_call2_coll`] performs the invoke and that
/// exact NULL check.
fn comparison_shim(mcx: Mcx<'_>, id: SortComparatorId, x: Datum<'_>, y: Datum<'_>) -> PgResult<i32> {
    // Snapshot the resolved lookup and release the registry borrow before the
    // fmgr call, so a (re-entrant) comparator that itself prepares a shim can
    // not trip a RefCell double-borrow.
    enum Resolved {
        Shim(types_fmgr::FmgrResolution, types_fmgr::FmgrInfo, Oid),
        Native(nbtcompare::FastComparator),
        GistBox(gist_proc::GistComparator),
        UnsignedCmp,
    }
    let resolved = SHIMS.with(|s| {
        let shims = s.borrow();
        match &shims[id.0 as usize] {
            Comparator::Shim { resolved, collation } => Resolved::Shim(
                resolved.resolution.clone(),
                resolved.finfo.clone(),
                *collation,
            ),
            Comparator::Native(cmp) => Resolved::Native(*cmp),
            Comparator::GistBox(cmp) => Resolved::GistBox(*cmp),
            Comparator::UnsignedCmp => Resolved::UnsignedCmp,
        }
    });

    match resolved {
        Resolved::Shim(resolution, finfo, collation) => {
            // Bridge the canonical by-value words across the fmgr/kernel layer,
            // which still speaks the transitional bare-word `types_datum::Datum`
            // (fmgr-core is not in this migration batch — established sibling
            // pattern is `types_datum::Datum::from_usize(canonical.as_usize())`).
            // The comparator args are scalar Datum words exactly as C passes them.
            let x = types_datum::Datum::from_usize(x.as_usize());
            let y = types_datum::Datum::from_usize(y.as_usize());
            let result = function_call2_coll(mcx, &resolution, finfo, collation, x, y)?;
            // C: `comparison_shim` returns the `Datum` result as an `int`
            // (`DatumGetInt32`).
            Ok(result.as_i32())
        }
        // A native fast comparator (e.g. `ssup_datum_int32_cmp`): a pure
        // function of the two packed `Datum`s, infallible (no fmgr ereport).
        Resolved::Native(cmp) => {
            let x = types_datum::Datum::from_usize(x.as_usize());
            let y = types_datum::Datum::from_usize(y.as_usize());
            Ok(cmp(x, y))
        }
        // A GiST box comparator (`gist_bbox_zorder_cmp`): the operands are
        // pass-by-reference `BOX` images, so the canonical `Datum<'_>` is
        // threaded WHOLE (its `ByRef` payload, NOT a collapsed word) into the
        // kernel, which decodes the `BOX` from the bytes. Pure, infallible.
        Resolved::GistBox(cmp) => Ok(cmp(x, y)),
        // `ssup_datum_unsigned_cmp` (sortsupport.c): the abbreviated-key
        // comparator, comparing the two pass-by-value Datum words as unsigned
        // integers. On a 64-bit (`SIZEOF_DATUM == 8`) build the abbreviated key
        // occupies the whole word.
        Resolved::UnsignedCmp => Ok(ssup_datum_unsigned_cmp(x, y)),
    }
}

/// `ssup_datum_unsigned_cmp(x, y, ssup)` (utils/sort/sortsupport.c): compare two
/// pass-by-value `Datum`s as unsigned integers. Used by types whose abbreviated
/// keys (or unsigned native values) sort by raw word ordering. `ssup` is
/// unused. The operands are `ByVal` words.
fn ssup_datum_unsigned_cmp(x: Datum<'_>, y: Datum<'_>) -> i32 {
    let x = x.as_usize();
    let y = y.as_usize();
    // C: `(x < y) ? -1 : ((x > y) ? 1 : 0)`.
    match x.cmp(&y) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Equal => 0,
    }
}

// ===========================================================================
// PrepareSortSupportComparisonShim
// ===========================================================================

/// Set up a shim function to allow use of an old-style btree comparison
/// function as if it were a sort support comparator (C
/// `PrepareSortSupportComparisonShim`).
///
/// C `MemoryContextAlloc`s the `SortShimExtra` in `ssup->ssup_cxt`,
/// `fmgr_info_cxt`s `cmpFunc` into it, `InitFunctionCallInfoData`s the reusable
/// 2-arg callinfo with `ssup->ssup_collation`, and sets
/// `ssup->comparator = comparison_shim`. Here the resolved lookup is registered
/// under a token written into `ssup.comparator`.
pub fn PrepareSortSupportComparisonShim(
    cmpFunc: Oid,
    ssup: &mut SortSupportData<'_>,
) -> PgResult<()> {
    let resolved = fmgr_info_cxt(ssup.ssup_cxt, cmpFunc)?;
    let id = register_shim(Comparator::Shim {
        resolved,
        collation: ssup.ssup_collation,
    });
    ssup.comparator = Some(id);
    Ok(())
}

// ===========================================================================
// FinishSortSupportFunction
// ===========================================================================

/// Look up and call sortsupport function to setup SortSupport comparator; or if
/// no such function exists or it declines to set up the appropriate state,
/// prepare a suitable shim (C `FinishSortSupportFunction`).
fn FinishSortSupportFunction(
    opfamily: Oid,
    opcintype: Oid,
    ssup: &mut SortSupportData<'_>,
) -> PgResult<()> {
    // Look for a sort support function.
    let sortSupportFunction =
        lsyscache::get_opfamily_proc::call(opfamily, opcintype, opcintype, BTSORTSUPPORT_PROC)?;
    if OidIsValid(sortSupportFunction) {
        // The sort support function can provide a comparator, but it can also
        // choose not to do so (e.g. based on the selected collation).
        oid_function_call1_sortsupport(sortSupportFunction, ssup)?;
    }

    if ssup.comparator.is_none() {
        let sortFunction =
            lsyscache::get_opfamily_proc::call(opfamily, opcintype, opcintype, BTORDER_PROC)?;

        if !OidIsValid(sortFunction) {
            return Err(elog(format!(
                "missing support function {BTORDER_PROC}({opcintype},{opcintype}) in opfamily {opfamily}"
            )));
        }

        // We'll use a shim to call the old-style btree comparator.
        PrepareSortSupportComparisonShim(sortFunction, ssup)?;
    }

    Ok(())
}

// ===========================================================================
// OidFunctionCall1(sortfunc, PointerGetDatum(ssup)) — the BTSORTSUPPORT /
// GIST_SORTSUPPORT entry point.
//
// The type-specific sortsupport function receives the `SortSupport` and fills
// `ssup->comparator` (and the abbreviation hooks). An owned `Datum` carries no
// raw pointer, so the live `SortSupportData` cannot cross the fmgr boundary as
// `PointerGetDatum(ssup)`; instead the dispatch is routed by OID to the typed
// `*sortsupport` routine through `nbtcompare`'s `run_sortsupport` seam, which
// receives `&mut SortSupportData` directly and sets `ssup.comparator`.
//
// For a sortsupport function `nbtcompare` does not implement (e.g.
// float4/float8, or a future type's BTSORTSUPPORT not yet ported), the seam
// returns `false`; we then fall back to the fmgr `OidFunctionCall1` path, which
// loud-fails for the as-yet-unported builtin — the faithful "not yet ported"
// surface (C would reach the registered function once it exists).
// ===========================================================================
fn oid_function_call1_sortsupport(sortfunc: Oid, ssup: &mut SortSupportData<'_>) -> PgResult<()> {
    // C: OidFunctionCall1(sortfunc, PointerGetDatum(ssup)) — invoke the type's
    // sortsupport routine on the live SortSupport. Routed by OID to the typed
    // routine (the owned `Datum` cannot carry the SortSupport pointer).
    if nbtcompare_seams::run_sortsupport::call(sortfunc, ssup) {
        return Ok(());
    }

    // Not one of nbtcompare's in-core sortsupport routines: fall back to the
    // fmgr path. The SortSupport pointer argument cannot be represented in the
    // pointer-less owned `Datum`; an unported sortsupport builtin loud-fails
    // here, exactly the "function not yet ported" surface.
    oid_function_call1_coll(ssup.ssup_cxt, sortfunc, 0, types_datum::Datum::null())?;
    Ok(())
}

// ===========================================================================
// PrepareSortSupportFromOrderingOp
// ===========================================================================

/// Fill in SortSupport given an ordering operator (btree "<" or ">" operator)
/// (C `PrepareSortSupportFromOrderingOp`).
///
/// Caller must previously have zeroed the SortSupportData structure and then
/// filled in `ssup_cxt`, `ssup_collation`, and `ssup_nulls_first`. This fills
/// in `ssup_reverse` as well as the comparator.
pub fn PrepareSortSupportFromOrderingOp(
    orderingOp: Oid,
    ssup: &mut SortSupportData<'_>,
) -> PgResult<()> {
    debug_assert!(ssup.comparator.is_none());

    // Find the operator in pg_amop.
    let (opfamily, opcintype, cmptype) =
        match lsyscache::get_ordering_op_properties::call(orderingOp)? {
            Some(props) => props,
            None => {
                return Err(elog(format!(
                    "operator {orderingOp} is not a valid ordering operator"
                )));
            }
        };
    ssup.ssup_reverse = cmptype == COMPARE_GT;

    FinishSortSupportFunction(opfamily, opcintype, ssup)
}

// ===========================================================================
// PrepareSortSupportFromIndexRel
// ===========================================================================

/// Fill in SortSupport given an index relation and attribute (C
/// `PrepareSortSupportFromIndexRel`).
///
/// Caller must previously have zeroed the SortSupportData structure and then
/// filled in `ssup_cxt`, `ssup_attno`, `ssup_collation`, and
/// `ssup_nulls_first`. This fills in `ssup_reverse` (from `reverse`) and the
/// comparator.
pub fn PrepareSortSupportFromIndexRel(
    indexRel: &Relation<'_>,
    reverse: bool,
    ssup: &mut SortSupportData<'_>,
) -> PgResult<()> {
    let attno: AttrNumber = ssup.ssup_attno;
    let opfamily = relcache::rd_opfamily::call(indexRel, attno)?;
    let opcintype = relcache::rd_opcintype::call(indexRel, attno)?;

    debug_assert!(ssup.comparator.is_none());

    if !relcache::rd_indam_amcanorder::call(indexRel)? {
        let relam = relcache::rd_rel_relam::call(indexRel)?;
        return Err(elog(format!("unexpected non-amcanorder AM: {relam}")));
    }
    ssup.ssup_reverse = reverse;

    FinishSortSupportFunction(opfamily, opcintype, ssup)
}

// ===========================================================================
// PrepareSortSupportFromGistIndexRel
// ===========================================================================

/// Fill in SortSupport given a GiST index relation (C
/// `PrepareSortSupportFromGistIndexRel`).
///
/// Caller must previously have zeroed the SortSupportData structure and then
/// filled in `ssup_cxt`, `ssup_attno`, `ssup_collation`, and
/// `ssup_nulls_first`. This fills in `ssup_reverse` (always false for GiST
/// index build) and the comparator.
pub fn PrepareSortSupportFromGistIndexRel(
    indexRel: &Relation<'_>,
    ssup: &mut SortSupportData<'_>,
) -> PgResult<()> {
    let attno: AttrNumber = ssup.ssup_attno;
    let opfamily = relcache::rd_opfamily::call(indexRel, attno)?;
    let opcintype = relcache::rd_opcintype::call(indexRel, attno)?;

    debug_assert!(ssup.comparator.is_none());

    let relam = relcache::rd_rel_relam::call(indexRel)?;
    if relam != GIST_AM_OID {
        return Err(elog(format!("unexpected non-gist AM: {relam}")));
    }
    ssup.ssup_reverse = false;

    // Look up the sort support function. This is simpler than for B-tree
    // indexes because we don't support the old-style btree comparators.
    let sortSupportFunction =
        lsyscache::get_opfamily_proc::call(opfamily, opcintype, opcintype, GIST_SORTSUPPORT_PROC)?;
    if !OidIsValid(sortSupportFunction) {
        return Err(elog(format!(
            "missing support function {GIST_SORTSUPPORT_PROC}({opcintype},{opcintype}) in opfamily {opfamily}"
        )));
    }
    oid_function_call1_sortsupport(sortSupportFunction, ssup)?;

    Ok(())
}

// ===========================================================================
// apply_sort_comparator — invoke the installed comparator (sortsupport.h
// `ApplySortComparator` non-null dispatch).
// ===========================================================================

/// Invoke the comparator carried by `ssup.comparator` on two non-null datums.
/// The caller has already verified `ssup.comparator.is_some()` and handled the
/// null / reverse arithmetic (`ApplySortComparator` in sortsupport.h).
fn apply_sort_comparator(
    datum1: Datum<'_>,
    datum2: Datum<'_>,
    ssup: &SortSupportData<'_>,
) -> PgResult<i32> {
    let id = ssup
        .comparator
        .expect("apply_sort_comparator: ssup.comparator must be set");
    comparison_shim(ssup.ssup_cxt, id, datum1, datum2)
}

// ===========================================================================
// Abbreviated-key hook registries + apply seams.
//
// The three abbreviation hooks (`abbrev_converter`, `abbrev_abort`,
// `abbrev_full_comparator`) follow the same token model as `comparator`: the
// abbreviation-providing unit (varlena/numeric) registers the resolved kernel
// through its install seam and writes the token into the matching `ssup` field;
// the sort engine invokes it through the `apply_sort_abbrev_*` seams here, which
// interpret the token.
//
// `abbrev_full_comparator` shares the comparator-token space ([`SortComparatorId`]
// into `SHIMS`), exactly as C reuses the same `int (*)(Datum, Datum, SortSupport)`
// function-pointer type. Only the converter / abort have their own registries.
// ===========================================================================

/// A resolved `abbrev_converter` kernel: `Datum (*)(Datum original, SortSupport)`.
/// Owned `Datum` is the canonical carrier; the abbreviated key is a
/// pass-by-value word, but the kernel signature carries the canonical `Datum`
/// uniformly.
type AbbrevConverterKernel = fn(Datum<'_>, &SortSupportData<'_>) -> PgResult<Datum<'static>>;

/// A resolved `abbrev_abort` kernel: `bool (*)(int memtupcount, SortSupport)`.
type AbbrevAbortKernel = fn(i32, &mut SortSupportData<'_>) -> PgResult<bool>;

/// The resolved abbreviation converter an [`AbbrevConverterId`] denotes. C's
/// `abbrev_converter` is a single `Datum (*)(Datum, SortSupport)` pointer; the
/// owned model holds either a full-signature kernel ([`AbbrevConverter::Full`],
/// used by varlena/numeric once they install through this registry) or a
/// GiST-style converter ([`AbbrevConverter::Gist`]) that ignores `ssup` and is
/// infallible (`gist_bbox_zorder_abbrev_convert`).
enum AbbrevConverter {
    /// A `Datum (*)(Datum, SortSupport)` kernel that may `ereport`. The forward
    /// path for varlena/numeric, which do not yet install through this registry
    /// (their `*_abbrev_convert` install seams land with those units), so it is
    /// unconstructed for now — kept to model the full C `abbrev_converter`
    /// signature the `apply_sort_abbrev_converter` seam already honors.
    #[allow(dead_code)]
    Full(AbbrevConverterKernel),
    /// `gist_bbox_zorder_abbrev_convert`: pure, `ssup`-free, infallible.
    Gist(gist_proc::GistAbbrevConverter),
}

thread_local! {
    /// Token -> abbrev converter, indexed by `AbbrevConverterId.0`.
    static ABBREV_CONVERTERS: RefCell<Vec<AbbrevConverter>> =
        const { RefCell::new(Vec::new()) };
    /// Token -> abbrev abort, indexed by `AbbrevAbortId.0`.
    static ABBREV_ABORTS: RefCell<Vec<AbbrevAbortKernel>> = const { RefCell::new(Vec::new()) };
}

/// `ssup->abbrev_converter(original, ssup)` (sortsupport.h): invoke the
/// installed abbreviation converter on the original (non-null, pass-by-reference)
/// datum. The caller has verified `ssup.abbrev_converter.is_some()`.
fn apply_sort_abbrev_converter(
    original: Datum<'_>,
    ssup: &SortSupportData<'_>,
) -> PgResult<Datum<'static>> {
    let id: AbbrevConverterId = ssup
        .abbrev_converter
        .expect("apply_sort_abbrev_converter: ssup.abbrev_converter must be set");
    let converter = ABBREV_CONVERTERS.with(|s| match &s.borrow()[id.0 as usize] {
        AbbrevConverter::Full(k) => AbbrevConverter::Full(*k),
        AbbrevConverter::Gist(k) => AbbrevConverter::Gist(*k),
    });
    match converter {
        AbbrevConverter::Full(kernel) => kernel(original, ssup),
        AbbrevConverter::Gist(kernel) => Ok(kernel(original)),
    }
}

/// `ssup->abbrev_abort(memtupcount, ssup)` (sortsupport.h): poll the installed
/// abort-abbreviation cost-model callback. The caller has verified
/// `ssup.abbrev_abort.is_some()`.
fn apply_sort_abbrev_abort(
    memtupcount: i32,
    ssup: &mut SortSupportData<'_>,
) -> PgResult<bool> {
    let id: AbbrevAbortId = ssup
        .abbrev_abort
        .expect("apply_sort_abbrev_abort: ssup.abbrev_abort must be set");
    let kernel = ABBREV_ABORTS.with(|s| s.borrow()[id.0 as usize]);
    kernel(memtupcount, ssup)
}

/// `ssup->abbrev_full_comparator(x, y, ssup)` (sortsupport.h,
/// `ApplySortAbbrevFullComparator`): invoke the full authoritative comparator
/// the sortsupport routine moved aside when it installed the abbreviated
/// comparator. The token shares the comparator space (`SHIMS`), interpreted by
/// [`comparison_shim`]. The caller has verified
/// `ssup.abbrev_full_comparator.is_some()`.
fn apply_sort_abbrev_full_comparator(
    datum1: Datum<'_>,
    datum2: Datum<'_>,
    ssup: &SortSupportData<'_>,
) -> PgResult<i32> {
    let id = ssup
        .abbrev_full_comparator
        .expect("apply_sort_abbrev_full_comparator: ssup.abbrev_full_comparator must be set");
    comparison_shim(ssup.ssup_cxt, id, datum1, datum2)
}

// ===========================================================================
// install_sortsupport_* — the substrate side of `ssup->comparator = <fastcmp>`.
//
// These seams are OWNED by this sort substrate (declared in nbt-compare-seams,
// documented as substrate-owned) and installed here. A type's `*sortsupport`
// routine in nbtcompare calls them with its native fast comparator; we mint a
// `SortComparatorId` token denoting that kernel and write it into
// `ssup.comparator`, exactly as C's `ssup->comparator = ssup_datum_int32_cmp`.
// ===========================================================================

/// Common body for all four `install_sortsupport_*` seams: register the native
/// fast comparator and store its token in `ssup.comparator`.
fn install_native_comparator(ssup: &mut SortSupportData<'_>, cmp: nbtcompare::FastComparator) {
    let id = register_shim(Comparator::Native(cmp));
    ssup.comparator = Some(id);
}

// ===========================================================================
// install_gist_sortsupport_* — the substrate side of `gist_point_sortsupport`.
//
// Owned + installed here (declared in gist-proc-seams). `gist_point_sortsupport`
// calls one of these with its native z-order kernels; we mint the matching
// tokens and write them into the `ssup` fields, exactly as C's
// `ssup->comparator = gist_bbox_zorder_cmp;` (and the abbreviated arm).
// ===========================================================================

/// Register an abbrev converter and hand back its token.
fn register_abbrev_converter(state: AbbrevConverter) -> AbbrevConverterId {
    ABBREV_CONVERTERS.with(|s| {
        let mut v = s.borrow_mut();
        let id = v.len() as u32;
        v.push(state);
        AbbrevConverterId(id)
    })
}

/// Register an abbrev abort kernel and hand back its token.
fn register_abbrev_abort(kernel: AbbrevAbortKernel) -> AbbrevAbortId {
    ABBREV_ABORTS.with(|s| {
        let mut v = s.borrow_mut();
        let id = v.len() as u32;
        v.push(kernel);
        AbbrevAbortId(id)
    })
}

/// `gist_bbox_zorder_abbrev_abort` (gistproc.c): always `false`. A constant the
/// substrate supplies for the GiST abbreviated arm (the kernel reads neither
/// argument).
fn gist_bbox_zorder_abbrev_abort(_memtupcount: i32, _ssup: &mut SortSupportData<'_>) -> PgResult<bool> {
    Ok(false)
}

/// `ssup->comparator = gist_bbox_zorder_cmp;` (the non-abbreviated arm of
/// `gist_point_sortsupport`). Register the GiST box comparator and store its
/// token in `ssup.comparator`.
fn install_gist_comparator(ssup: &mut SortSupportData<'_>, cmp: gist_proc::GistComparator) {
    let id = register_shim(Comparator::GistBox(cmp));
    ssup.comparator = Some(id);
}

/// The abbreviated arm of `gist_point_sortsupport`:
/// `ssup->comparator = ssup_datum_unsigned_cmp;`
/// `ssup->abbrev_converter = gist_bbox_zorder_abbrev_convert;`
/// `ssup->abbrev_abort = gist_bbox_zorder_abbrev_abort;`
/// `ssup->abbrev_full_comparator = gist_bbox_zorder_cmp;`
fn install_gist_abbrev(
    ssup: &mut SortSupportData<'_>,
    full_cmp: gist_proc::GistComparator,
    converter: gist_proc::GistAbbrevConverter,
) {
    // ssup->comparator = ssup_datum_unsigned_cmp;
    ssup.comparator = Some(register_shim(Comparator::UnsignedCmp));
    // ssup->abbrev_converter = gist_bbox_zorder_abbrev_convert;
    ssup.abbrev_converter = Some(register_abbrev_converter(AbbrevConverter::Gist(converter)));
    // ssup->abbrev_abort = gist_bbox_zorder_abbrev_abort;
    ssup.abbrev_abort = Some(register_abbrev_abort(gist_bbox_zorder_abbrev_abort));
    // ssup->abbrev_full_comparator = gist_bbox_zorder_cmp;
    ssup.abbrev_full_comparator = Some(register_shim(Comparator::GistBox(full_cmp)));
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install every `backend-utils-sort-sortsupport-seams` slot, plus the
/// substrate-owned `install_sortsupport_*` slots declared in nbt-compare-seams.
pub fn init_seams() {
    use backend_utils_sort_sortsupport_seams as sx;

    sx::oid_function_call_1_sortsupport::set(oid_function_call1_sortsupport);
    sx::prepare_sort_support_comparison_shim::set(PrepareSortSupportComparisonShim);
    sx::apply_sort_comparator::set(apply_sort_comparator);
    sx::prepare_sort_support_from_ordering_op::set(PrepareSortSupportFromOrderingOp);
    sx::prepare_sort_support_from_index_rel::set(PrepareSortSupportFromIndexRel);
    sx::prepare_sort_support_from_gist_index_rel::set(PrepareSortSupportFromGistIndexRel);
    sx::apply_sort_abbrev_converter::set(apply_sort_abbrev_converter);
    sx::apply_sort_abbrev_abort::set(apply_sort_abbrev_abort);
    sx::apply_sort_abbrev_full_comparator::set(apply_sort_abbrev_full_comparator);

    // The fast-comparator install seams (owned by this substrate).
    nbtcompare_seams::install_sortsupport_int2::set(install_native_comparator);
    nbtcompare_seams::install_sortsupport_int4::set(install_native_comparator);
    nbtcompare_seams::install_sortsupport_int8::set(install_native_comparator);
    nbtcompare_seams::install_sortsupport_oid::set(install_native_comparator);

    // The GiST `gist_point_sortsupport` install seams (owned by this substrate).
    gist_proc::install_gist_sortsupport_comparator::set(install_gist_comparator);
    gist_proc::install_gist_sortsupport_abbrev::set(install_gist_abbrev);
}

#[cfg(test)]
mod tests;
