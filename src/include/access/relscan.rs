use std::sync::Arc;

use crate::backend::access::transam::xact::Snapshot;
use crate::backend::storage::buffer::OwnedBufferPin;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::executor::RelationDesc;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTuple;
use crate::include::access::scankey::ScanKeyData;
use crate::{BufferPool, ClientId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
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

#[derive(Debug, Clone)]
pub enum IndexScanOpaque {
    None,
    Btree(BtIndexScanOpaque),
}

#[derive(Clone)]
pub struct IndexScanDescData {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub snapshot: Snapshot,
    pub heap_relation: Option<RelFileLocator>,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub indoption: Vec<i16>,
    pub number_of_keys: usize,
    pub key_data: Vec<ScanKeyData>,
    pub direction: ScanDirection,
    pub xs_want_itup: bool,
    pub xs_itup: Option<IndexTuple>,
    pub xs_heaptid: Option<ItemPointerData>,
    pub opaque: IndexScanOpaque,
}

pub type IndexScanDesc = IndexScanDescData;
