//! Seam declarations for the GiST core's typed opclass support-procedure
//! dispatch (`access/gist/`).
//!
//! The GiST AM dispatches its opclass support procedures
//! (`consistent`/`union`/`compress`/`decompress`/`penalty`/`picksplit`/`same`/
//! `distance`/`fetch`/`options`/`sortsupport`) by OID, exactly as the BRIN AM
//! dispatches `OpcInfo`/`AddValue`/`Consistent`/`Union` through
//! `backend-access-brin-entry-seams` and the SP-GiST core dispatches its five
//! procedures through `backend-access-spg-core-seams`. In C this is
//!
//! ```c
//! FunctionCall5Coll(&giststate->consistentFn[col], collation,
//!                   PointerGetDatum(entry), query, strategy, subtype,
//!                   PointerGetDatum(&recheck));
//! ```
//!
//! over the GiST-typed by-pointer structs ([`types_gist::GISTENTRY`],
//! [`types_gist::GistEntryVector`], [`types_gist::GIST_SPLITVEC`]) — *not* a
//! generic fmgr-by-pointer-Datum path. We model it as one typed seam per
//! support-procedure kind, each taking the proc OID (resolved by the GiST core
//! via `index_getprocinfo(rel, col + 1, GIST_*_PROC).fn_oid`) plus the typed
//! input and (where the C method writes through a by-pointer output) a `&mut`
//! output borrowing the owned [`types_gist`] vocabulary.
//!
//! The opclass crates INSTALL their typed bodies into these seams from their
//! `init_seams()`, keyed on their `pg_proc.dat` support-proc OIDs. The
//! `backend-access-gist-proc` unit owns the box/point opclass (`gistproc.c`) and
//! is the single installer of these dispatchers; it dispatches the box/point
//! support-proc OIDs and `ereport(ERROR)`s ("unrecognized GiST support function
//! OID") for any other OID. The range/inet/tsvector opclasses fold their OIDs
//! into the same dispatcher when their owners land. Until then a dispatch to an
//! unrecognized OID errors, and an uninstalled seam panics loudly
//! (mirror-PG-and-panic).

#![allow(non_snake_case)]

use mcx::{Mcx, PgBox};
use types_core::primitive::{uint16, Oid};
use types_error::PgResult;
use types_gist::{GistEntryVector, GISTENTRY, GIST_SPLITVEC};
use types_sortsupport::SortSupportData;
use types_tuple::backend_access_common_heaptuple::Datum;

/// `StrategyNumber` (access/stratnum.h) — the comparison strategy the scan key
/// requests of a consistent/distance support procedure.
pub type StrategyNumber = uint16;

/// The result of a GiST `consistent` support procedure: whether the entry's
/// predicate could match (`PG_RETURN_BOOL`) plus the `*recheck` flag the method
/// writes back.
#[derive(Clone, Copy, Debug)]
pub struct GistConsistentResult {
    /// `PG_RETURN_BOOL(...)` — could a data item below this entry match?
    pub matched: bool,
    /// `*recheck` — whether the operator result must be rechecked on the heap.
    pub recheck: bool,
}

/// The result of a GiST `distance` support procedure: the (possibly lossy)
/// distance plus the `*recheck` flag (`recheckDistances`) the method writes.
#[derive(Clone, Copy, Debug)]
pub struct GistDistanceResult {
    /// `PG_RETURN_FLOAT8(distance)`.
    pub distance: f64,
    /// `*recheck` — whether the distance is lossy and must be rechecked.
    pub recheck: bool,
}

seam_core::seam!(
    /// `FunctionCall5Coll(consistentFn, collation, entry, query, strategy,
    /// subtype, &recheck)` (gistget.c `gistindex_keytest`): the opclass
    /// `GIST_CONSISTENT_PROC`. `is_leaf` is the C `GIST_LEAF(entry)` the AM
    /// computes from the page being scanned (the owned `GISTENTRY` carries the
    /// block number, not page bytes, so the leaf-ness is supplied explicitly).
    /// Returns whether the entry may match plus the `*recheck` flag. `Err`
    /// carries the opclass' `ereport(ERROR)` surface.
    pub fn gist_consistent<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entry: &GISTENTRY<'mcx>,
        is_leaf: bool,
        query: &Datum<'mcx>,
        strategy: StrategyNumber,
        subtype: Oid,
    ) -> PgResult<GistConsistentResult>
);

seam_core::seam!(
    /// `FunctionCall2Coll(unionFn, collation, entryvec, &sizep)` (gistutil.c
    /// `gistMakeUnionItVec` / `gistunionsubkey`): the opclass `GIST_UNION_PROC`
    /// — the minimal bounding key enclosing every entry in `entryvec`. Returns
    /// the new key [`Datum`] (the C `palloc`'d result, sized by the method's
    /// `*sizep`). `Err` carries the opclass' `ereport(ERROR)` surface and OOM.
    pub fn gist_union<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entryvec: &GistEntryVector<'mcx>,
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `FunctionCall1Coll(compressFn, collation, entry)` (gistutil.c
    /// `gistcentryinit`): the opclass `GIST_COMPRESS_PROC` — convert a leaf
    /// value into its index representation. Returns the (possibly new)
    /// [`GISTENTRY`]. `Err` carries the opclass' `ereport(ERROR)` and OOM.
    pub fn gist_compress<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entry: &GISTENTRY<'mcx>,
    ) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>>
);

seam_core::seam!(
    /// `FunctionCall1Coll(decompressFn, collation, entry)` (gistutil.c
    /// `gistDeCompressAtt`): the opclass `GIST_DECOMPRESS_PROC` — the inverse of
    /// compress. Returns the (possibly new) [`GISTENTRY`]. `Err` carries the
    /// opclass' `ereport(ERROR)` and OOM.
    pub fn gist_decompress<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entry: &GISTENTRY<'mcx>,
    ) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>>
);

seam_core::seam!(
    /// `FunctionCall3Coll(penaltyFn, collation, origentry, newentry, &penalty)`
    /// (gistutil.c `gistpenalty`): the opclass `GIST_PENALTY_PROC` — the cost of
    /// inserting `newentry`'s key under `origentry`'s key. Returns the C
    /// `*result` (`float`). `Err` carries the opclass' `ereport(ERROR)`.
    pub fn gist_penalty<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        origentry: &GISTENTRY<'mcx>,
        newentry: &GISTENTRY<'mcx>,
    ) -> PgResult<f32>
);

seam_core::seam!(
    /// `FunctionCall2Coll(picksplitFn, collation, entryvec, &splitvec)`
    /// (gistsplit.c `gistUserPicksplit`): the opclass `GIST_PICKSPLIT_PROC` —
    /// distribute the entries in `entryvec` between two groups, writing the
    /// assignment + per-group union keys into `splitvec`. `Err` carries the
    /// opclass' `ereport(ERROR)` and OOM.
    pub fn gist_picksplit<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entryvec: &GistEntryVector<'mcx>,
        splitvec: &mut GIST_SPLITVEC<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FunctionCall3Coll(equalFn, collation, a, b, &result)` (gistutil.c
    /// `gistKeyIsEQ`): the opclass `GIST_EQUAL_PROC` — whether two index keys
    /// are exactly equal. Returns the C `*result` (`bool`). `Err` carries the
    /// opclass' `ereport(ERROR)`.
    pub fn gist_same<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        a: &Datum<'mcx>,
        b: &Datum<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `FunctionCall5Coll(distanceFn, collation, entry, query, strategy,
    /// subtype, &recheck)` (gistget.c `gistindex_keytest`): the opclass
    /// `GIST_DISTANCE_PROC` — the ordering distance from `entry` to `query`.
    /// `is_leaf` is the C `GIST_LEAF(entry)` supplied by the AM. Returns the
    /// distance plus the `*recheck` (recheckDistances) flag. `Err` carries the
    /// opclass' `ereport(ERROR)`.
    pub fn gist_distance<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entry: &GISTENTRY<'mcx>,
        is_leaf: bool,
        query: &Datum<'mcx>,
        strategy: StrategyNumber,
        subtype: Oid,
    ) -> PgResult<GistDistanceResult>
);

seam_core::seam!(
    /// `FunctionCall1Coll(fetchFn, collation, entry)` (gistget.c
    /// `gistFetchTuple`): the opclass `GIST_FETCH_PROC` — reconstruct the
    /// original indexed value from the index entry (index-only scans). Returns
    /// the (possibly new) [`GISTENTRY`]. `Err` carries the opclass'
    /// `ereport(ERROR)` and OOM.
    pub fn gist_fetch<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        collation: Oid,
        entry: &GISTENTRY<'mcx>,
    ) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>>
);

seam_core::seam!(
    /// `FunctionCall1(optionsFn, PointerGetDatum(&relopts))` (gistutil.c
    /// `gistAllocateOptions`): the opclass `GIST_OPTIONS_PROC` — fill the
    /// opclass-specific reloptions into `relopts` (the local-relopts buffer).
    /// Owned by the opclass; box/point have no options procedure, so this seam
    /// stays uninstalled / errors for them. `Err` carries `ereport(ERROR)`.
    pub fn gist_options<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
        relopts: &mut Vec<u8>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FunctionCall1(sortsupportFn, PointerGetDatum(ssup))` (gistbuild.c
    /// `gistSortedBuildCallback` setup): the opclass `GIST_SORTSUPPORT_PROC` —
    /// install the build-by-sort comparator (and abbreviation hooks) into
    /// `ssup`. Reached only from the sorted index-build path, which is itself
    /// gated on `table_index_build_scan`. `Err` carries `ereport(ERROR)`.
    pub fn gist_sortsupport<'mcx>(
        proc_oid: Oid,
        ssup: &mut SortSupportData<'mcx>,
    ) -> PgResult<()>
);
