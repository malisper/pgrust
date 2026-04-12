use crate::include::access::itup::IndexTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::scankey::ScanKeyData;
use crate::backend::storage::smgr::RelFileLocator;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexScanDescData {
    pub heap_relation: Option<RelFileLocator>,
    pub index_relation: RelFileLocator,
    pub number_of_keys: usize,
    pub key_data: Vec<ScanKeyData>,
    pub xs_want_itup: bool,
    pub xs_itup: Option<IndexTuple>,
    pub xs_heaptid: Option<ItemPointerData>,
}

pub type IndexScanDesc = IndexScanDescData;
