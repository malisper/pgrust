//! GISTSTATE carrier and per-column opclass support-proc call frames.
//! Support procs are resolved once per state (C fmgr_info_copy from
//! rd_support); per-tuple calls rewrite args in place on owned frames.
use std::rc::Rc;

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_tuple::TupleDescData;

use crate::{GistEntryVector, GistSplitVec, GISTENTRY};

const InvalidOid: Oid = 0;

pub struct GistState<'mcx> {
    pub leafTupdesc: Rc<TupleDescData<'mcx>>,
    pub nonLeafTupdesc: Rc<TupleDescData<'mcx>>,
    pub fetchTupdesc: Option<Rc<TupleDescData<'mcx>>>,

    pub consistentFn: Vec<FmgrInfo>,
    pub unionFn: Vec<FmgrInfo>,
    pub compressFn: Vec<FmgrInfo>,
    pub decompressFn: Vec<FmgrInfo>,
    pub penaltyFn: Vec<FmgrInfo>,
    pub picksplitFn: Vec<FmgrInfo>,
    pub equalFn: Vec<FmgrInfo>,
    pub distanceFn: Vec<FmgrInfo>,
    pub fetchFn: Vec<FmgrInfo>,

    pub supportCollation: Vec<Oid>,

    pub frame1: LocalFcinfo<1>,
    pub frame2: LocalFcinfo<2>,
    pub frame3: LocalFcinfo<3>,
    pub frame5: LocalFcinfo<5>,
}

impl<'mcx> GistState<'mcx> {
    pub fn has_compress(&self, attno: usize) -> bool {
        self.compressFn[attno].fn_oid != InvalidOid
    }
    pub fn has_decompress(&self, attno: usize) -> bool {
        self.decompressFn[attno].fn_oid != InvalidOid
    }
    pub fn has_fetch(&self, attno: usize) -> bool {
        self.fetchFn[attno].fn_oid != InvalidOid
    }
    pub fn has_distance(&self, attno: usize) -> bool {
        self.distanceFn[attno].fn_oid != InvalidOid
    }

    // FunctionCall1Coll(&compressFn[attno], …, PointerGetDatum(&entry)); the
    // result GISTENTRY* may be the input or a temp-allocated replacement.
    pub fn call_compress(&mut self, mcx: Mcx<'_>, attno: usize, entry: &GISTENTRY) -> PgResult<GISTENTRY> {
        self.frame1.rearm(self.supportCollation[attno]);
        // SAFETY: caller's temp context outlives the call (its reset point
        // is after consuming the results).
        unsafe { self.frame1.set_result_mcx(mcx) };
        self.frame1
            .set_arg(0, Datum::from_usize(entry as *const GISTENTRY as usize));
        let r = self.compressFn[attno].invoke(&mut self.frame1)?;
        // SAFETY: opclass contract — returns a GISTENTRY* (input or palloc'd
        // in the armed temp context, live until reset_temp).
        Ok(unsafe { *(r.as_usize() as *const GISTENTRY) })
    }

    pub fn call_decompress(&mut self, mcx: Mcx<'_>, attno: usize, entry: &GISTENTRY) -> PgResult<GISTENTRY> {
        self.frame1.rearm(self.supportCollation[attno]);
        // SAFETY: as call_compress.
        unsafe { self.frame1.set_result_mcx(mcx) };
        self.frame1
            .set_arg(0, Datum::from_usize(entry as *const GISTENTRY as usize));
        let r = self.decompressFn[attno].invoke(&mut self.frame1)?;
        // SAFETY: as call_compress.
        Ok(unsafe { *(r.as_usize() as *const GISTENTRY) })
    }

    pub fn call_fetch(&mut self, mcx: Mcx<'_>, attno: usize, entry: &GISTENTRY) -> PgResult<GISTENTRY> {
        self.frame1.rearm(self.supportCollation[attno]);
        // SAFETY: as call_compress.
        unsafe { self.frame1.set_result_mcx(mcx) };
        self.frame1
            .set_arg(0, Datum::from_usize(entry as *const GISTENTRY as usize));
        let r = self.fetchFn[attno].invoke(&mut self.frame1)?;
        // SAFETY: as call_compress.
        Ok(unsafe { *(r.as_usize() as *const GISTENTRY) })
    }

    // FunctionCall2Coll(&unionFn[attno], …, evec, &size) -> new key Datum.
    pub fn call_union(&mut self, mcx: Mcx<'_>, attno: usize, evec: &GistEntryVector) -> PgResult<Datum> {
        let mut size: i32 = 0;
        self.frame2.rearm(self.supportCollation[attno]);
        // SAFETY: as call_compress.
        unsafe { self.frame2.set_result_mcx(mcx) };
        self.frame2
            .set_arg(0, Datum::from_usize(evec as *const GistEntryVector as usize));
        self.frame2
            .set_arg(1, Datum::from_usize(&mut size as *mut i32 as usize));
        self.unionFn[attno].invoke(&mut self.frame2)
    }

    // FunctionCall3Coll(&penaltyFn[attno], …, orig, add, &penalty).
    pub fn call_penalty(
        &mut self,
        mcx: Mcx<'_>,
        attno: usize,
        orig: &GISTENTRY,
        add: &GISTENTRY,
    ) -> PgResult<f32> {
        let mut penalty: f32 = 0.0;
        self.frame3.rearm(self.supportCollation[attno]);
        // SAFETY: as call_compress.
        unsafe { self.frame3.set_result_mcx(mcx) };
        self.frame3
            .set_arg(0, Datum::from_usize(orig as *const GISTENTRY as usize));
        self.frame3
            .set_arg(1, Datum::from_usize(add as *const GISTENTRY as usize));
        self.frame3
            .set_arg(2, Datum::from_usize(&mut penalty as *mut f32 as usize));
        self.penaltyFn[attno].invoke(&mut self.frame3)?;
        Ok(penalty)
    }

    pub fn call_picksplit(
        &mut self,
        mcx: Mcx<'_>,
        attno: usize,
        evec: &GistEntryVector,
        sv: &mut GistSplitVec,
    ) -> PgResult<()> {
        self.frame2.rearm(self.supportCollation[attno]);
        // SAFETY: as call_compress.
        unsafe { self.frame2.set_result_mcx(mcx) };
        self.frame2
            .set_arg(0, Datum::from_usize(evec as *const GistEntryVector as usize));
        self.frame2
            .set_arg(1, Datum::from_usize(sv as *mut GistSplitVec as usize));
        self.picksplitFn[attno].invoke(&mut self.frame2)?;
        Ok(())
    }

    // FunctionCall3Coll(&equalFn[attno], …, a, b, &result).
    pub fn call_same(&mut self, mcx: Mcx<'_>, attno: usize, a: Datum, b: Datum) -> PgResult<bool> {
        let mut result = false;
        self.frame3.rearm(self.supportCollation[attno]);
        // SAFETY: as call_compress.
        unsafe { self.frame3.set_result_mcx(mcx) };
        self.frame3.set_arg(0, a);
        self.frame3.set_arg(1, b);
        self.frame3
            .set_arg(2, Datum::from_usize(&mut result as *mut bool as usize));
        self.equalFn[attno].invoke(&mut self.frame3)?;
        Ok(result)
    }

    // FunctionCall5Coll(consistent-proc via scankey, …). Frame owned here so
    // gistindex_keytest never builds a fresh fcinfo per tuple.
    #[allow(clippy::too_many_arguments)]
    pub fn call_consistent(
        &mut self,
        mcx: Mcx<'_>,
        finfo: &mut FmgrInfo,
        collation: Oid,
        de: &GISTENTRY,
        query: Datum,
        strategy: u16,
        subtype: Oid,
        recheck: &mut bool,
    ) -> PgResult<bool> {
        self.frame5.rearm(collation);
        // SAFETY: as call_compress.
        unsafe { self.frame5.set_result_mcx(mcx) };
        self.frame5
            .set_arg(0, Datum::from_usize(de as *const GISTENTRY as usize));
        self.frame5.set_arg(1, query);
        self.frame5.set_arg(2, Datum::from_i16(strategy as i16));
        self.frame5.set_arg(3, Datum::from_oid(subtype));
        self.frame5
            .set_arg(4, Datum::from_usize(recheck as *mut bool as usize));
        let r = finfo.invoke(&mut self.frame5)?;
        Ok(r.as_bool())
    }
}

// ---------------------------------------------------------------------------
// Scan opaque (gist_private.h GISTScanOpaqueData), KNN-free rendering: ordered
// scans are a LOUD lane, so queue items carry no distances and heap items
// never enter the queue.
// ---------------------------------------------------------------------------

use ::mcx::MemoryContext;
use ::types_core::{BlockNumber, OffsetNumber, XLogRecPtr};
use ::types_tuple::itemptr::ItemPointerData;

#[derive(Clone, Copy, Debug)]
pub struct GISTSearchItem {
    pub blkno: BlockNumber,
    pub parentlsn: XLogRecPtr,
}

// zero-orderby pairingheap_GISTSearchItem_cmp: no distances, no heap items —
// all entries compare equal; visit order is the heap mechanics (C-exact).
pub fn gist_search_item_cmp(_a: &GISTSearchItem, _b: &GISTSearchItem) -> i32 {
    0
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GISTSearchHeapItem {
    pub heapPtr: ItemPointerData,
    pub recheck: bool,
    pub offnum: OffsetNumber,
    // IOS: (offset, len) of the reconstructed tuple in fetch_buf.
    pub recontup: Option<(u32, u32)>,
}

pub struct GISTScanOpaqueData<'mcx> {
    pub giststate: GistState<'mcx>,
    // C giststate->tempCxt: reset after each keytest batch.
    pub temp: MemoryContext,
    pub queue: crate::pairingheap::PairingHeap<GISTSearchItem, fn(&GISTSearchItem, &GISTSearchItem) -> i32>,
    pub qual_ok: bool,
    pub firstCall: bool,

    pub killedItems: Option<Vec<OffsetNumber>>,
    pub numKilled: i32,
    pub curBlkno: BlockNumber,
    pub curPageLSN: XLogRecPtr,

    pub pageData: Vec<GISTSearchHeapItem>,
    pub nPageData: usize,
    pub curPageData: usize,
    // IOS reconstructed index tuples for the current page (currTuples-style);
    // xs_itup points in here, valid until the next page / rescan / endscan.
    pub fetch_buf: Vec<u8>,
}
