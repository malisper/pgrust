//! GiST access method (gist.c + gistutil.c + gistscan.c + gistget.c +
//! gistsplit.c + gistbuild.c plain path + gistxlog.c producers). LOUD lanes:
//! vacuum (gistbulkdelete/gistvacuumcleanup), sorted build
//! (point_ops sortsupport), KNN/ordered scans, unlogged/temp relations.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub mod get;
pub mod insert;
pub mod scan;
pub mod split;
pub mod state;
pub mod util;
pub mod vacuum;
pub mod wal;

use ::mcx::MemoryContext;
use ::types_core::{InvalidSubTransactionId, Buffer};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::PageMut;
use ::types_tuple::itemptr::ItemPointerData;
use ::datum::Datum;

pub use get::{gistgetbitmap, gistgettuple};
pub use insert::gistdoinsert;
pub use scan::{gistbeginscan, gistcanreturn, gistendscan, gistrescan};
pub use state::{initGISTstate, GistState};
pub use vacuum::{gistbulkdelete, gistbulkdelete_collect, gistvacuumcleanup};

#[cold]
#[inline(never)]
pub(crate) fn non_gist_opaque() -> ! {
    panic!("gist entry point reached with a non-gist scan opaque")
}

pub(crate) fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        panic!("unported: ProcessInterrupts (tcop/postgres.c) reached from gist");
    }
}

#[inline]
pub fn buf_page_mut_pub(buffer: Buffer) -> PageMut<'static> {
    buf_page_mut(buffer)
}

pub fn relation_needs_wal_pub(rel: &Relation<'_>) -> bool {
    relation_needs_wal(rel)
}

#[inline]
pub(crate) fn buf_page_mut(buffer: Buffer) -> PageMut<'static> {
    // SAFETY: caller holds the exclusive content lock (C's write contract).
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

// RelationNeedsWAL (rel.h); XLogIsNeeded ≡ the xlog_standby_info_active seam.
pub(crate) fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == InvalidSubTransactionId))
}

// wal_level=logical unported; const-false (heapam precedent).
pub(crate) fn relation_is_accessible_in_logical_decoding(_rel: &Relation<'_>) -> bool {
    false
}

// The per-statement GISTSTATE insert cache (C indexInfo->ii_AmCache).
pub struct GistInsertAmCache<'mcx> {
    pub giststate: GistState<'mcx>,
    pub temp: MemoryContext,
}

/// gistinsert. `amcache` is the C ii_AmCache slot: the caller owns one
/// Option per index per statement and passes it back on every call; `mcx`
/// must outlive it (C ii_Context; holds INCLUDE indexes' truncated tupdesc).
pub fn gistinsert<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    r: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    heapRel: &Relation<'_>,
    amcache: &mut Option<GistInsertAmCache<'mcx>>,
) -> PgResult<bool> {
    if amcache.is_none() {
        *amcache = Some(GistInsertAmCache {
            giststate: initGISTstate(mcx, r)?,
            temp: MemoryContext::new_bump("GiST temporary context"),
        });
    }
    let cache = amcache.as_mut().expect("just initialized");

    {
        let mcx = cache.temp.mcx();
        let mut itup =
            util::gistFormTuple(mcx, &mut cache.giststate, r, values, isnull, true)?;
        // SAFETY: owned image; t_tid is the leading 6 bytes.
        unsafe {
            itup.as_mut_ptr()
                .cast::<ItemPointerData>()
                .write_unaligned(*ht_ctid);
        }
        gistdoinsert(mcx, r, itup.as_ptr(), 0, &mut cache.giststate, heapRel, false)?;
    }
    cache.temp.reset();
    Ok(false)
}

/// gist_translate_cmptype_common's RT-strategy table (gistutil.c), used by
/// amapi's IndexAmTranslate* arms.
pub fn gist_translate_cmptype_common(cmptype: i32) -> u16 {
    match cmptype {
        3 /* COMPARE_EQ */ => 18,
        1 /* COMPARE_LT */ => 20,
        2 /* COMPARE_LE */ => 21,
        5 /* COMPARE_GT */ => 22,
        4 /* COMPARE_GE */ => 23,
        7 /* COMPARE_OVERLAP */ => 3,
        8 /* COMPARE_CONTAINED_BY */ => 8,
        _ => 0,
    }
}
