use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::backend::access::transam::xact::Snapshot;
use crate::backend::executor::RelationDesc;
use crate::backend::storage::buffer::OwnedBufferPin;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTuple;
use crate::include::access::scankey::ScanKeyData;
use crate::{BufferPool, ClientId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GistOrderByDistance {
    pub value: f64,
    pub is_null: bool,
}

impl Eq for GistOrderByDistance {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GistSearchItemKind {
    Page {
        block: u32,
        parent_lsn: u64,
    },
    Heap {
        tid: ItemPointerData,
        tuple: IndexTuple,
        recheck: bool,
        recheck_order_by: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GistSearchItem {
    pub kind: GistSearchItemKind,
    pub distances: Vec<GistOrderByDistance>,
    pub ordinal: u64,
}

impl GistSearchItem {
    fn is_heap(&self) -> bool {
        matches!(self.kind, GistSearchItemKind::Heap { .. })
    }
}

impl Ord for GistSearchItem {
    fn cmp(&self, other: &Self) -> Ordering {
        for (left, right) in self.distances.iter().zip(other.distances.iter()) {
            let cmp = match (left.is_null, right.is_null) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => right.value.total_cmp(&left.value),
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        match (self.is_heap(), other.is_heap()) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => self.ordinal.cmp(&other.ordinal),
        }
    }
}

impl PartialOrd for GistSearchItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct BtIndexScanOpaque {
    pub current_block: Option<u32>,
    pub current_pin: Option<OwnedBufferPin<SmgrStorageBackend>>,
    pub page_prev: Option<u32>,
    pub page_next: Option<u32>,
    pub next_offset: usize,
    pub current_items: Vec<IndexTuple>,
}

#[derive(Debug, Clone, Default)]
pub struct GistIndexScanOpaque {
    pub search_queue: BinaryHeap<GistSearchItem>,
    pub next_ordinal: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BrinIndexScanOpaque {
    pub pages_per_range: u32,
    pub current_range_start: Option<u32>,
    pub next_revmap_page: u32,
    pub next_revmap_index: usize,
    pub scan_started: bool,
}

#[derive(Debug, Clone)]
pub enum IndexScanOpaque {
    None,
    Btree(BtIndexScanOpaque),
    Gist(GistIndexScanOpaque),
    Brin(BrinIndexScanOpaque),
}

#[derive(Clone)]
pub struct IndexScanDescData {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub snapshot: Snapshot,
    pub heap_relation: Option<RelFileLocator>,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub indoption: Vec<i16>,
    pub number_of_keys: usize,
    pub key_data: Vec<ScanKeyData>,
    pub number_of_order_bys: usize,
    pub order_by_data: Vec<ScanKeyData>,
    pub direction: ScanDirection,
    pub xs_want_itup: bool,
    pub xs_itup: Option<IndexTuple>,
    pub xs_heaptid: Option<ItemPointerData>,
    pub xs_recheck: bool,
    pub xs_recheck_order_by: bool,
    pub xs_orderby_values: Vec<Option<f64>>,
    pub opaque: IndexScanOpaque,
}

pub type IndexScanDesc = IndexScanDescData;
