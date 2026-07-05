//! SP-GiST access method (spgutils.c + spgdoinsert.c + spginsert.c +
//! spgscan.c; spgxlog.c redo lives in spgist_xlog, spgbuild in spgist_build).
//! LOUD lanes: vacuum (spgbulkdelete/spgvacuumcleanup), ordered/KNN scans,
//! spgbuildempty (unlogged), polymorphic/compress-opclass leaf types.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub mod doinsert;
pub mod scan;
pub mod utils;

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_spgist::state::SpGistState;
use ::types_tuple::itemptr::ItemPointerData;

pub use ::types_spgist::{spgFormDeadTuple, spgPageIndexMultiDelete, spgUpdateNodeLink};
pub use doinsert::{spgdoinsert, RM_SPGIST_ID};
pub use scan::{spgbeginscan, spgcanreturn, spgendscan, spggetbitmap, spggettuple, spgrescan};
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

pub(crate) fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        panic!("unported: ProcessInterrupts (tcop/postgres.c) reached from spgist");
    }
}

// The per-statement insert cache (C indexInfo->ii_AmCache mirror; C rebuilds
// SpGistState per spginsert call, but the state is pure derived data).
pub struct SpgInsertAmCache<'mcx> {
    pub state: SpGistState<'mcx>,
    pub temp: MemoryContext,
}

/// spginsert.
pub fn spginsert<'mcx>(
    r: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    amcache: &mut Option<SpgInsertAmCache<'mcx>>,
) -> PgResult<bool> {
    if amcache.is_none() {
        *amcache = Some(SpgInsertAmCache {
            state: initSpGistState(r)?,
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

/// spgbulkdelete: named LOUD lane (spgvacuum.c).
pub fn spgbulkdelete(rel: &Relation<'_>) -> PgResult<()> {
    panic!(
        "unported: spgbulkdelete for index \"{}\" (spgvacuum.c lane)",
        rel.name()
    );
}

/// spgvacuumcleanup: named LOUD lane except the analyze-only no-op.
pub fn spgvacuumcleanup(rel: &Relation<'_>, analyze_only: bool) -> PgResult<()> {
    if analyze_only {
        return Ok(());
    }
    panic!(
        "unported: spgvacuumcleanup for index \"{}\" (spgvacuum.c lane)",
        rel.name()
    );
}
