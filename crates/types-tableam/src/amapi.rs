//! `access/amapi.h` — the index-access-method dispatch vocabulary: the
//! `IndexAmRoutine` vtable and `IndexUniqueCheck`, trimmed to the callbacks the
//! general index-AM dispatch layer (`access/index/indexam.c`) invokes through
//! `relation->rd_indam`.
//!
//! The model mirrors [`crate::tableam::TableAmRoutine`]: `rd_indam` is a vtable
//! of function pointers the per-AM implementation (nbtree / hash / gist / gin /
//! spgist / brin) installs; the dispatch layer reads the boolean property flags
//! and invokes the callbacks. Callbacks the C marks `/* can be NULL */` are
//! `Option<fn>`; the rest are plain `fn`. Only the callbacks and properties the
//! dispatch layer (indexam.c) actually consumes are present; further ones are
//! added as their callers are ported.

use std::boxed::Box;
use std::vec::Vec;

use types_datum::Datum;
use types_error::PgResult;
use types_rel::Relation;
use types_scan::sdir::ScanDirection;
use types_tuple::heaptuple::ItemPointerData;

use types_scan::scankey::ScanKeyData;

use crate::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use crate::relscan::{IndexScanDesc, IndexScanDescData};

/// `IndexUniqueCheck` (`access/genam.h`, used through amapi.h) — the
/// uniqueness-check mode passed to `aminsert`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexUniqueCheck {
    /// Don't do any uniqueness checking.
    UNIQUE_CHECK_NO = 0,
    /// Enforce uniqueness at insertion time.
    UNIQUE_CHECK_YES,
    /// Test uniqueness, but no error.
    UNIQUE_CHECK_PARTIAL,
    /// Check if existing tuple is unique.
    UNIQUE_CHECK_EXISTING,
}

/// `struct IndexInfo` (`nodes/execnodes.h`) — opaque to the index-AM dispatch
/// layer: indexam.c only forwards it to `aminsert`/`aminsertcleanup`, never
/// reading it. Owned by the executor/catalog code that builds it; carried here
/// as a type-erased payload so the AM callback can downcast it.
pub struct IndexInfo {
    pub payload: Option<Box<dyn core::any::Any>>,
}

/// `TIDBitmap` (`nodes/tidbitmap.h`) — opaque to the index-AM dispatch layer:
/// indexam.c only forwards it to `amgetbitmap`. Owned by the tidbitmap
/// subsystem; carried here as a type-erased payload.
pub struct TIDBitmap {
    pub payload: Option<Box<dyn core::any::Any>>,
}

/// `IndexAmRoutine` (`access/amapi.h`) — the index-access-method vtable,
/// trimmed to the property flags and callbacks the dispatch layer
/// (`indexam.c`) reads/invokes through `relation->rd_indam`. The owning AM
/// installs it; the relcache hands it back per relation.
pub struct IndexAmRoutine {
    /* property flags read by indexam.c */
    /// `uint16 amsupport` — total number of support functions this AM uses.
    pub amsupport: u16,
    /// `uint16 amoptsprocnum` — opclass options support function number, or 0.
    pub amoptsprocnum: u16,
    /// `bool ampredlocks` — does the AM handle predicate locks itself?
    pub ampredlocks: bool,

    /* required interface functions invoked by indexam.c */
    /// `aminsert(indexRelation, values, isnull, heap_tid, heapRelation,
    /// checkUnique, indexUnchanged, indexInfo)`.
    #[allow(clippy::type_complexity)]
    pub aminsert: fn(
        index_relation: &Relation<'_>,
        values: &[Datum],
        isnull: &[bool],
        heap_tid: &ItemPointerData,
        heap_relation: &Relation<'_>,
        check_unique: IndexUniqueCheck,
        index_unchanged: bool,
        index_info: &mut IndexInfo,
    ) -> PgResult<bool>,

    /// `ambulkdelete(info, stats, callback, callback_state)`. The deletion
    /// callback + its state live in the vacuum substrate, so the AM owns the
    /// whole call; `stats` is `None` for the C `NULL`.
    pub ambulkdelete: fn(
        info: &IndexVacuumInfo<'_>,
        stats: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>,

    /// `amvacuumcleanup(info, stats)`.
    pub amvacuumcleanup: fn(
        info: &IndexVacuumInfo<'_>,
        stats: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>,

    /// `ambeginscan(indexRelation, nkeys, norderbys)` — prepare for an index
    /// scan; the AM allocates and returns the scan descriptor.
    pub ambeginscan: for<'mcx> fn(
        index_relation: &Relation<'mcx>,
        nkeys: i32,
        norderbys: i32,
    ) -> PgResult<IndexScanDesc<'mcx>>,

    /// `amrescan(scan, keys, nkeys, orderbys, norderbys)`.
    pub amrescan: fn(
        scan: &mut IndexScanDescData<'_>,
        keys: &[ScanKeyData],
        orderbys: &[ScanKeyData],
    ) -> PgResult<()>,

    /// `amendscan(scan)`.
    pub amendscan: fn(scan: &mut IndexScanDescData<'_>) -> PgResult<()>,

    /* optional interface functions ("can be NULL" in C) */
    /// `aminsertcleanup(indexRelation, indexInfo)`.
    pub aminsertcleanup:
        Option<fn(index_relation: &Relation<'_>, index_info: &mut IndexInfo) -> PgResult<()>>,

    /// `amcanreturn(indexRelation, attno)` — does the AM support index-only
    /// scans for the given column?
    pub amcanreturn: Option<fn(index_relation: &Relation<'_>, attno: i32) -> PgResult<bool>>,

    /// `amgettuple(scan, direction)` — next valid tuple (TID into
    /// `scan->xs_heaptid`).
    pub amgettuple:
        Option<fn(scan: &mut IndexScanDescData<'_>, direction: ScanDirection) -> PgResult<bool>>,

    /// `amgetbitmap(scan, tbm)` — fetch all valid tuples into the bitmap.
    pub amgetbitmap:
        Option<fn(scan: &mut IndexScanDescData<'_>, tbm: &mut TIDBitmap) -> PgResult<i64>>,

    /// `ammarkpos(scan)`.
    pub ammarkpos: Option<fn(scan: &mut IndexScanDescData<'_>) -> PgResult<()>>,

    /// `amrestrpos(scan)`.
    pub amrestrpos: Option<fn(scan: &mut IndexScanDescData<'_>) -> PgResult<()>>,

    /* parallel index scan support ("can be NULL" in C) */
    /// `amestimateparallelscan(indexRelation, nkeys, norderbys)` — DSM space
    /// the AM needs for its shared parallel-scan state.
    pub amestimateparallelscan:
        Option<fn(index_relation: &Relation<'_>, nkeys: i32, norderbys: i32) -> PgResult<usize>>,

    /// `aminitparallelscan(target)` — initialize the AM-specific shared state.
    /// `target` is the AM-specific tail of the parallel descriptor.
    pub aminitparallelscan: Option<fn(target: &mut Vec<u8>) -> PgResult<()>>,

    /// `amparallelrescan(scan)`.
    pub amparallelrescan: Option<fn(scan: &mut IndexScanDescData<'_>) -> PgResult<()>>,
}
