use crate::backend::catalog::CatalogError;
use crate::include::access::relscan::IndexScanDesc;

pub fn index_beginscan_stub(index_relation: crate::backend::storage::smgr::RelFileLocator) -> Result<IndexScanDesc, CatalogError> {
    Ok(IndexScanDesc {
        heap_relation: None,
        index_relation,
        number_of_keys: 0,
        key_data: Vec::new(),
        xs_want_itup: false,
        xs_itup: None,
        xs_heaptid: None,
    })
}
