//! SpGistState carrier, opclass support-proc call frames, and the scan
//! opaque. Procs are resolved once per state (rule 4); the opclass arg
//! structs cross the fmgr boundary as pointer datums, C-shaped.
use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext};
use ::types_core::{Oid, TransactionId};
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_scan::scankey::ScanKeyData;
use ::types_storage::bufpage::MaxIndexTuplesPerPage;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::TupleDescData;

use crate::{spgConfigOut, SpGistTypeDesc};

pub struct SpGistState<'mcx> {
    pub config: spgConfigOut,
    pub attType: SpGistTypeDesc,
    pub attLeafType: SpGistTypeDesc,
    pub attPrefixType: SpGistTypeDesc,
    pub attLabelType: SpGistTypeDesc,
    pub leafTupDesc: Rc<TupleDescData<'mcx>>,
    pub redirectXid: TransactionId,
    pub isBuild: bool,

    pub indexCollation: Oid,
    pub chooseFn: FmgrInfo,
    pub picksplitFn: FmgrInfo,
    pub compressFn: FmgrInfo,

    pub frame1: LocalFcinfo<1>,
    pub frame2: LocalFcinfo<2>,
}

impl SpGistState<'_> {
    pub fn has_compress(&self) -> bool {
        self.compressFn.fn_oid != 0
    }

    pub fn call_compress(&mut self, mcx: Mcx<'_>, datum: Datum) -> PgResult<Datum> {
        self.frame1.rearm(self.indexCollation);
        // SAFETY: the caller's temp context outlives consumption of the result.
        unsafe { self.frame1.set_result_mcx(mcx) };
        self.frame1.set_arg(0, datum);
        self.compressFn.invoke(&mut self.frame1)
    }

    pub fn call_choose(
        &mut self,
        mcx: Mcx<'_>,
        input: &spgChooseIn,
        out: &mut spgChooseOut,
    ) -> PgResult<()> {
        self.frame2.rearm(self.indexCollation);
        // SAFETY: as call_compress.
        unsafe { self.frame2.set_result_mcx(mcx) };
        self.frame2
            .set_arg(0, Datum::from_usize(input as *const spgChooseIn as usize));
        self.frame2
            .set_arg(1, Datum::from_usize(out as *mut spgChooseOut as usize));
        self.chooseFn.invoke(&mut self.frame2)?;
        Ok(())
    }

    pub fn call_picksplit(
        &mut self,
        mcx: Mcx<'_>,
        input: &spgPickSplitIn,
        out: &mut spgPickSplitOut,
    ) -> PgResult<()> {
        self.frame2.rearm(self.indexCollation);
        // SAFETY: as call_compress.
        unsafe { self.frame2.set_result_mcx(mcx) };
        self.frame2
            .set_arg(0, Datum::from_usize(input as *const spgPickSplitIn as usize));
        self.frame2
            .set_arg(1, Datum::from_usize(out as *mut spgPickSplitOut as usize));
        self.picksplitFn.invoke(&mut self.frame2)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Opclass argument structs (spgist.h). Arrays are raw pointer + count pairs
// allocated in the armed result mcx, matching C's palloc-in-temp-context
// protocol; both sides of the fmgr boundary are in-tree.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub struct spgChooseIn {
    pub datum: Datum,
    pub leafDatum: Datum,
    pub level: i32,
    pub allTheSame: bool,
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: i32,
    pub nodeLabels: *const Datum,
}

#[derive(Clone, Copy)]
pub enum spgChooseOut {
    None,
    MatchNode {
        nodeN: i32,
        levelAdd: i32,
        restDatum: Datum,
    },
    AddNode {
        nodeLabel: Datum,
        nodeN: i32,
    },
    SplitTuple {
        prefixHasPrefix: bool,
        prefixPrefixDatum: Datum,
        prefixNNodes: i32,
        prefixNodeLabels: *const Datum,
        childNodeN: i32,
        postfixHasPrefix: bool,
        postfixPrefixDatum: Datum,
    },
}

#[derive(Clone, Copy)]
pub struct spgPickSplitIn {
    pub nTuples: i32,
    pub datums: *const Datum,
    pub level: i32,
}

#[derive(Clone, Copy)]
pub struct spgPickSplitOut {
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: i32,
    pub nodeLabels: *const Datum,
    pub mapTuplesToNodes: *mut i32,
    pub leafTupleDatums: *const Datum,
}

impl Default for spgPickSplitOut {
    fn default() -> Self {
        spgPickSplitOut {
            hasPrefix: false,
            prefixDatum: Datum::null(),
            nNodes: 0,
            nodeLabels: core::ptr::null(),
            mapTuplesToNodes: core::ptr::null_mut(),
            leafTupleDatums: core::ptr::null(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct spgInnerConsistentIn<'a> {
    pub scankeys: *const ScanKeyData,
    pub orderbys: *const ScanKeyData,
    pub nkeys: i32,
    pub norderbys: i32,
    pub reconstructedValue: Datum,
    pub traversalValue: usize,
    pub traversalMemoryContext: Mcx<'a>,
    pub level: i32,
    pub returnData: bool,
    pub allTheSame: bool,
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: i32,
    pub nodeLabels: *const Datum,
}

#[derive(Clone, Copy)]
pub struct spgInnerConsistentOut {
    pub nNodes: i32,
    pub nodeNumbers: *const i32,
    pub levelAdds: *const i32,
    pub reconstructedValues: *const Datum,
    pub traversalValues: *const usize,
    // per-output-node rows of norderbys distances (C double **)
    pub distances: *const *const f64,
}

impl Default for spgInnerConsistentOut {
    fn default() -> Self {
        spgInnerConsistentOut {
            nNodes: 0,
            nodeNumbers: core::ptr::null(),
            levelAdds: core::ptr::null(),
            reconstructedValues: core::ptr::null(),
            traversalValues: core::ptr::null(),
            distances: core::ptr::null(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct spgLeafConsistentIn {
    pub scankeys: *const ScanKeyData,
    pub orderbys: *const ScanKeyData,
    pub nkeys: i32,
    pub norderbys: i32,
    pub reconstructedValue: Datum,
    pub traversalValue: usize,
    pub level: i32,
    pub returnData: bool,
    pub leafDatum: Datum,
}

#[derive(Clone, Copy)]
pub struct spgLeafConsistentOut {
    pub leafValue: Datum,
    pub recheck: bool,
    pub distances: *const f64,
    pub recheckDistances: bool,
}

impl Default for spgLeafConsistentOut {
    fn default() -> Self {
        spgLeafConsistentOut {
            leafValue: Datum::null(),
            recheck: false,
            distances: core::ptr::null(),
            recheckDistances: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Scan opaque (spgist_private.h SpGistScanOpaqueData).
// ---------------------------------------------------------------------------

pub struct SpGistSearchItem {
    pub value: Datum,
    // owned copy of the whole leaf tuple image (IOS INCLUDE reconstruction)
    pub leafTuple: Option<Vec<u8>>,
    pub traversalValue: usize,
    pub level: i32,
    pub heapPtr: ItemPointerData,
    pub isNull: bool,
    pub isLeaf: bool,
    pub recheck: bool,
    pub recheckDistances: bool,
    // numberOfNonNullOrderBys entries (empty for null items and unordered
    // scans; C allocates the array inline in the item)
    pub distances: Vec<f64>,
}

/// pairingheap_SpGistSearchItem_cmp. KNN searches only support NULLS LAST;
/// both non-null items carry numberOfNonNullOrderBys distances.
pub fn spg_search_item_cmp(a: &SpGistSearchItem, b: &SpGistSearchItem) -> i32 {
    if a.isNull {
        if !b.isNull {
            return -1;
        }
    } else if b.isNull {
        return 1;
    } else {
        let n = a.distances.len().min(b.distances.len());
        for i in 0..n {
            let (da, db) = (a.distances[i], b.distances[i]);
            if da.is_nan() && db.is_nan() {
                continue; // NaN == NaN
            }
            if da.is_nan() {
                return -1; // NaN > number
            }
            if db.is_nan() {
                return 1; // number < NaN
            }
            if da != db {
                return if da < db { 1 } else { -1 };
            }
        }
    }
    // Leaf items go before inner pages, for depth-first search.
    if a.isLeaf && !b.isLeaf {
        return 1;
    }
    if !a.isLeaf && b.isLeaf {
        return -1;
    }
    0
}

pub type SpGistScanQueue =
    ::types_gist::pairingheap::PairingHeap<SpGistSearchItem, fn(&SpGistSearchItem, &SpGistSearchItem) -> i32>;

pub struct SpGistScanOpaqueData<'mcx> {
    pub state: SpGistState<'mcx>,
    pub scanQueue: SpGistScanQueue,
    pub tempCxt: MemoryContext,
    pub traversalCxt: MemoryContext,

    pub searchNulls: bool,
    pub searchNonNulls: bool,

    pub numberOfKeys: i32,
    pub keyData: Vec<ScanKeyData>,
    pub indexCollation: Oid,

    // ordered (KNN) scans: NULL-argument orderbys are compacted out of
    // orderByData; nonNullOrderByOffsets maps original positions to the
    // compacted ones (-1 for removed).
    pub numberOfOrderBys: i32,
    pub numberOfNonNullOrderBys: i32,
    pub orderByData: Vec<ScanKeyData>,
    pub nonNullOrderByOffsets: Vec<i32>,
    pub orderByTypes: Vec<Oid>,
    pub zeroDistances: Vec<f64>,
    pub infDistances: Vec<f64>,

    pub innerConsistentFn: FmgrInfo,
    pub leafConsistentFn: FmgrInfo,
    pub frame2: LocalFcinfo<2>,

    pub want_itup: bool,
    pub reconTupDesc: Option<Rc<TupleDescData<'mcx>>>,
    pub nPtrs: usize,
    pub iPtr: usize,
    pub heapPtrs: [ItemPointerData; MaxIndexTuplesPerPage],
    pub recheck: [bool; MaxIndexTuplesPerPage],
    pub recheckDistances: [bool; MaxIndexTuplesPerPage],
    // numberOfOrderBys IndexOrderByDistance entries per reported item (None
    // for null items); parallel to heapPtrs
    pub distances: Vec<Option<Vec<::types_gist::state::IndexOrderByDistance>>>,
    // reconstructed index-tuple images for IOS, packed 8-aligned; per-item
    // byte offsets in recon_offs (scan-lifetime scratch, reset per page)
    pub recon_buf: Vec<u8>,
    pub recon_offs: [u32; MaxIndexTuplesPerPage],
}
