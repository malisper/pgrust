//! SP-GiST access method (spgutils.c + spgdoinsert.c + spginsert.c +
//! spgscan.c + spgvacuum.c; spgxlog.c redo lives in spgist_xlog, spgbuild in
//! spgist_build). LOUD lanes: spgbuildempty (unlogged), polymorphic leaf
//! types.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub mod doinsert;
pub mod scan;
pub mod utils;
pub mod vacuum;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_spgist::state::SpGistState;
use ::types_tuple::itemptr::ItemPointerData;

pub use ::types_spgist::{spgFormDeadTuple, spgPageIndexMultiDelete, spgUpdateNodeLink};
pub use doinsert::{spgdoinsert, RM_SPGIST_ID};
pub use scan::{spgbeginscan, spgcanreturn, spgendscan, spggetbitmap, spggettuple, spgrescan};
pub use vacuum::{spgbulkdelete, spgbulkdelete_collect, spgvacuumcleanup};
pub use utils::{
    buf_page_mut as spg_buf_page_mut, initSpGistState,
    relation_needs_wal as spg_relation_needs_wal, spgGetCache,
    unlock_release as spg_unlock_release, SpGistGetBuffer, SpGistInitBuffer,
    SpGistInitMetapage, SpGistNewBuffer, SpGistSetLastUsedPage, SpGistUpdateMetaPage,
};

#[cold]
#[inline(never)]
pub(crate) fn non_spgist_opaque() -> ! {
    panic!("spgist entry point reached with a non-spgist scan opaque")
}

pub(crate) fn check_for_interrupts() -> ::types_error::PgResult<()> {
    // CHECK_FOR_INTERRUPTS() — route a pending interrupt through the ported
    // ProcessInterrupts seam (the gin/hash/heapam pattern). The former
    // unported-panic stub surfaced as a spurious ERROR line in regress
    // vacuum's `VACUUM (PARALLEL 2)` whenever a signal landed mid-bulkdelete
    // (train-6 validate, 2/9 lane-ON runs).
    if init_small::globals::InterruptPending() {
        return ::postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// The per-statement insert cache (C indexInfo->ii_AmCache mirror; C rebuilds
// SpGistState per spginsert call, but the state is pure derived data).
pub struct SpgInsertAmCache<'mcx> {
    pub state: SpGistState<'mcx>,
    pub temp: MemoryContext,
}

/// spginsert.
pub fn spginsert<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    amcache: &mut Option<SpgInsertAmCache<'mcx>>,
) -> PgResult<bool> {
    if amcache.is_none() {
        *amcache = Some(SpgInsertAmCache {
            state: initSpGistState(mcx, r)?,
            temp: MemoryContext::new_bump("SP-GiST insert temporary context"),
        });
    }
    let cache = amcache.as_mut().expect("just initialized");
    // C re-runs initSpGistState per call; only redirectXid can change.
    cache.state.redirectXid = xact::GetTopTransactionIdIfAny();

    loop {
        let done = {
            let mcx = cache.temp.mcx();
            spgdoinsert(mcx, r, &mut cache.state, ht_ctid, values, isnull)?
        };
        cache.temp.reset();
        if done {
            break;
        }
    }

    SpGistUpdateMetaPage(r)?;
    Ok(false)
}
