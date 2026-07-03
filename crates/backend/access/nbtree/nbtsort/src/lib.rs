// nbtsort.c, empty-build lane: zero tuples reach the spool (else loud).
#![allow(non_snake_case)]

use types_core::{BlockNumber, ForkNumber, InvalidOid, BLCKSZ};
use types_error::PgResult;
use types_rel::Relation;

const P_NONE: BlockNumber = 0;
const BTREE_METAPAGE: BlockNumber = 0;
const BTEQUALIMAGE_PROC: i16 = 4;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: nbtsort {what}")
}

pub fn btbuild<'mcx>(
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    if bufmgr::RelationGetNumberOfBlocksInFork(heap, ForkNumber::MAIN_FORKNUM)? != 0 {
        unported("_bt_spools_heapscan (nbtsort.c: non-empty heap at build time)");
    }
    let reltuples = 0.0;
    let indtuples = 0.0;

    // _bt_uppershutdown with no page state writes only the metapage.
    let allequalimage = bt_allequalimage(index)?;
    let mut bulkstate = bulkwrite::smgr_bulk_start_rel(index, ForkNumber::MAIN_FORKNUM)?;
    let mut metabuf = bulkwrite::smgr_bulk_get_buf(&bulkstate);
    // SAFETY: freshly allocated aligned page, exclusively owned.
    let mut page = unsafe {
        types_storage::bufpage::PageMut::from_raw(core::ptr::NonNull::new_unchecked(
            metabuf.page_mut().as_mut_ptr(),
        ))
    };
    nbtree::bt_initmetapage(&mut page, P_NONE, 0, allequalimage);
    bulkwrite::smgr_bulk_write(&mut bulkstate, BTREE_METAPAGE, metabuf, true)?;
    bulkwrite::smgr_bulk_finish(bulkstate)?;

    Ok(IndexBuildResult { heap_tuples: reltuples, index_tuples: indtuples })
}

// _bt_allequalimage (nbtutils.c), sans the DEBUG1 message.
fn bt_allequalimage(rel: &Relation<'_>) -> PgResult<bool> {
    for i in 0..rel.indnkeyatts() as usize {
        let opfamily = rel.rd_opfamily[i];
        let opcintype = rel.rd_opcintype[i];
        let collation = rel.rd_indcollation[i];
        let equalimageproc =
            lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, BTEQUALIMAGE_PROC)?;
        if equalimageproc == InvalidOid {
            return Ok(false);
        }
        let mut finfo = fmgr_seams::fmgr_info::call(equalimageproc)?;
        let mut fcinfo = types_fmgr::LocalFcinfo::<1>::fresh(collation);
        fcinfo.set_arg(0, datum::Datum::from_oid(opcintype));
        if !finfo.invoke(&mut fcinfo)?.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

const _: () = assert!(BLCKSZ == 8192);
