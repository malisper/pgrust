//! Seam declarations for the cross-module nbtree **build helpers** that
//! `nbtsort.c`'s grounded leaf-load path (`_bt_buildadd` / `_bt_load`) calls
//! into the still-`todo` `backend-access-nbtree-core` unit (`nbtutils.c`).
//!
//! These belong to the nbtree-core owner. They are declared here (rather than
//! in `backend-access-nbtree-core-seams`) only because that crate is owned /
//! being edited by another unit during this port; when nbtree-core lands it
//! must install them from its `init_seams()`. Until then each call panics
//! loudly — sanctioned mirror-and-panic, never a silent stub.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::types_nbtree::BTScanInsert;
use ::rel::Relation;
use ::types_tableam::amapi::IndexBuildResult;
use ::types_tableam::index_info_carrier::IndexInfoCarrier;

seam_core::seam!(
    /// `btbuild(heap, index, indexInfo)` (nbtsort.c): the btree AM's `ambuild`
    /// entry — drive the serial CREATE INDEX build (create the spool(s), scan
    /// the heap once feeding the per-tuple callback, then sort + leaf-load the
    /// btree pages) and return the heap/index tuple counts.
    ///
    /// `btbuild`'s real body lives in `backend-access-nbtree-nbtsort`, which
    /// sits ABOVE the AM-vtable crate (`backend-access-nbtree-nbtree`) in the
    /// dep graph, so the vtable's `ambuild` adapter cannot call it directly.
    /// This seam bridges that edge (owner = nbtsort, installed from its
    /// `init_seams`): the adapter passes the `IndexInfoCarrier` (#342) through,
    /// and nbtsort downcasts it back to the real
    /// `nodes::execnodes::IndexInfo<'mcx>`. `Err` carries the build's
    /// `ereport(ERROR)` surface.
    pub fn btbuild<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        heap: &Relation<'mcx>,
        index: &Relation<'mcx>,
        index_info: &mut IndexInfoCarrier<'a, 'mcx>,
    ) -> PgResult<IndexBuildResult>
);

seam_core::seam!(
    /// `_bt_truncate(rel, lastleft, firstright, itup_key)` (nbtutils.c):
    /// produce a suffix-truncated pivot tuple suitable as a high key / downlink
    /// separator between the `lastleft` and `firstright` leaf tuples, given the
    /// build's insertion scankey `itup_key`. Returns the new pivot tuple as
    /// owned on-disk bytes in `mcx`. `Err` carries comparison-support /
    /// oversize ereports.
    pub fn bt_truncate<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        lastleft: &[u8],
        firstright: &[u8],
        itup_key: &BTScanInsert<'mcx>,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `_bt_check_third_page(rel, heap, needheaptidspace, page, newtup)`
    /// (nbtutils.c): a tuple too large for a normal page reached the build;
    /// either it fits the "third page" exception or this `ereport(ERROR)`s with
    /// the "index row size exceeds maximum" diagnostic. Returns `Ok(())` when
    /// the tuple is allowed.
    pub fn bt_check_third_page<'mcx>(
        rel: &Relation<'mcx>,
        heap: &Relation<'mcx>,
        needheaptidspace: bool,
        page: &[u8],
        newtup: &[u8],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `_bt_load` unique-index merge inner loop (`index_getattr` +
    /// `ApplySortComparator` across all key attributes, then `ItemPointerCompare`
    /// on heap TID): compare two build-sorted index tuples `itup1` / `itup2` in
    /// the index's sort order, returning `<0`/`0`/`>0`.
    ///
    /// C inlines this over the per-key `SortSupport` the build sets up from the
    /// scankey; the build's `SortSupport` substrate is not yet ported, so the
    /// whole comparison is one owned-by-nbtree-core seam (panic-until-owner)
    /// rather than a fabricated comparator. `Err` carries comparison-support
    /// `ereport(ERROR)`s.
    pub fn bt_load_compare_index_tuples<'mcx>(
        rel: &Relation<'mcx>,
        itup_key: &BTScanInsert<'mcx>,
        itup1: &[u8],
        itup2: &[u8],
    ) -> PgResult<i32>
);
