//! hnsw.h vocabulary (pgvector): scan state lives here so relscan's
//! IndexScanOpaque can hold it without a cycle through the AM crates.

use mcx::{Mcx, PgVec};
use types_core::{BlockNumber, Oid};
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use types_tuple::itemptr::ItemPointerData;

pub const HNSW_MAX_DIM: usize = 2000;

pub const HNSW_DISTANCE_PROC: u16 = 1;
pub const HNSW_NORM_PROC: u16 = 2;
pub const HNSW_TYPE_INFO_PROC: u16 = 3;

pub const HNSW_VERSION: u32 = 1;
pub const HNSW_MAGIC_NUMBER: u32 = 0xA953A953;
pub const HNSW_PAGE_ID: u16 = 0xFF90;

pub const HNSW_METAPAGE_BLKNO: BlockNumber = 0;
pub const HNSW_HEAD_BLKNO: BlockNumber = 1;

pub const HNSW_UPDATE_LOCK: BlockNumber = 0;
pub const HNSW_SCAN_LOCK: BlockNumber = 1;

pub const HNSW_DEFAULT_M: i32 = 16;
pub const HNSW_MAX_M: i32 = 100;
pub const HNSW_DEFAULT_EF_CONSTRUCTION: i32 = 64;

pub const HNSW_ELEMENT_TUPLE_TYPE: u8 = 1;
pub const HNSW_NEIGHBOR_TUPLE_TYPE: u8 = 2;

pub const HNSW_HEAPTIDS: usize = 10;

pub const HNSW_UPDATE_ENTRY_GREATER: i32 = 1;
pub const HNSW_UPDATE_ENTRY_ALWAYS: i32 = 2;

pub const HNSW_ITERATIVE_SCAN_OFF: i32 = 0;
pub const HNSW_ITERATIVE_SCAN_RELAXED: i32 = 1;
pub const HNSW_ITERATIVE_SCAN_STRICT: i32 = 2;

#[inline]
pub fn hnsw_get_layer_m(m: i32, layer: i32) -> i32 {
    if layer == 0 {
        m * 2
    } else {
        m
    }
}

#[inline]
pub fn hnsw_get_ml(m: i32) -> f64 {
    1.0 / (m as f64).ln()
}

/// C: HnswTypeInfo.normalize — takes the full detoasted varlena image.
pub type HnswNormalizeFn = for<'m> fn(Mcx<'m>, &[u8]) -> PgResult<PgVec<'m, u8>>;
/// C: HnswTypeInfo.checkValue.
pub type HnswCheckValueFn = fn(&[u8]) -> PgResult<()>;

/// C: hnsw.h HnswTypeInfo. Returned (by pointer, as a Datum) from opclass
/// support proc 3 (`hnsw_*_support`) and consulted by HnswGetTypeInfo.
/// Callbacks receive the full detoasted varlena image (4-byte header
/// included), which is the representation the HNSW code already carries.
pub struct HnswTypeInfo {
    /// C: maxDimensions.
    pub max_dimensions: i32,
    /// C: normalize (l2_normalize for vector, halfvec_l2_normalize, ...).
    pub normalize: Option<HnswNormalizeFn>,
    /// C: checkValue (SparsevecCheckValue); None for vector/halfvec/bit.
    pub check_value: Option<HnswCheckValueFn>,
}

// Resolved support procs (C HnswSupport): distance (proc 1), optional norm
// (proc 2), index collation.
#[derive(Clone)]
pub struct HnswSupport {
    pub procinfo: FmgrInfo,
    pub normprocinfo: Option<FmgrInfo>,
    pub collation: Oid,
    /// C: HnswBuildState/ScanOpaque typeInfo, resolved once via HnswGetTypeInfo.
    pub type_info: &'static HnswTypeInfo,
}

#[derive(Clone, Copy)]
pub struct HnswScanElement {
    pub blkno: BlockNumber,
    pub offno: u16,
    pub level: u8,
    pub version: u8,
    pub heaptids: [ItemPointerData; HNSW_HEAPTIDS],
    pub heaptids_len: u8,
    pub neighbor_page: BlockNumber,
    pub neighbor_offno: u16,
    pub distance: f64,
}

// Owned (global-allocator) storage: this heap persists across hnswgettuple
// calls inside one scan, and C keeps it in so->tmpCtx which hnswrescan
// resets — dropping/reassigning it must actually free.
pub struct DistanceMinHeap {
    pub items: Vec<HnswScanElement>,
}

impl Default for DistanceMinHeap {
    fn default() -> Self {
        Self::new()
    }
}

impl DistanceMinHeap {
    pub fn new() -> Self {
        DistanceMinHeap { items: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push(&mut self, e: HnswScanElement) {
        self.items.push(e);
        let mut i = self.items.len() - 1;
        while i > 0 {
            let parent = (i - 1) / 2;
            if self.items[i].distance < self.items[parent].distance {
                self.items.swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }

    pub fn pop(&mut self) -> Option<HnswScanElement> {
        if self.items.is_empty() {
            return None;
        }
        let last = self.items.len() - 1;
        self.items.swap(0, last);
        let out = self.items.pop();
        let n = self.items.len();
        let mut i = 0;
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut m = i;
            if l < n && self.items[l].distance < self.items[m].distance {
                m = l;
            }
            if r < n && self.items[r].distance < self.items[m].distance {
                m = r;
            }
            if m == i {
                break;
            }
            self.items.swap(i, m);
            i = m;
        }
        out
    }
}

// C HnswScanOpaqueData; `w` is furthest-first, last() = nearest.
pub struct HnswScanOpaqueData<'mcx> {
    pub mcx: mcx::Mcx<'mcx>,
    pub first: bool,
    pub m: i32,
    pub tuples: i64,
    pub previous_distance: f64,
    pub max_memory: usize,
    // Approximates C MemoryContextMemAllocated(tmpCtx) for the iterative cap.
    pub mem_used: usize,
    // value/w/visited/discarded live in C's so->tmpCtx, which hnswrescan
    // resets; here they are globally-allocated owned values so reassignment
    // in hnswrescan/hnswendscan frees them (bounded memory across rescans).
    pub value: Option<Vec<u8>>,
    pub support: HnswSupport,
    pub max_dimensions: i32,
    pub norm_is_l2: bool,
    pub w: Vec<HnswScanElement>,
    pub visited: std::collections::HashSet<(BlockNumber, u16), rustc_hash::FxBuildHasher>,
    pub discarded: Option<DistanceMinHeap>,
}

#[cfg(test)]
mod type_info_tests {
    use super::*;
    use types_error::PgError;

    fn upper<'m>(_mcx: Mcx<'m>, img: &[u8]) -> PgResult<PgVec<'m, u8>> {
        unreachable!("not called in this test: {}", img.len())
    }
    fn reject_all(_img: &[u8]) -> PgResult<()> {
        Err(PgError::error("rejected").into())
    }

    static TI: HnswTypeInfo =
        HnswTypeInfo { max_dimensions: 7, normalize: Some(upper), check_value: Some(reject_all) };

    #[test]
    fn type_info_is_a_plain_static_with_fn_pointers() {
        assert_eq!(TI.max_dimensions, 7);
        assert!(TI.normalize.is_some());
        let err = (TI.check_value.unwrap())(&[0u8; 4]).unwrap_err();
        assert_eq!(err.message(), "rejected");
    }
}
