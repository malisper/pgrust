use crate::backend::catalog::CatalogError;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::access::amapi::{IndexAmRoutine, IndexBuildResult};
use crate::include::access::relscan::IndexScanDesc;

fn btbuild_stub(
    _heap_relation: RelFileLocator,
    _index_relation: RelFileLocator,
) -> Result<IndexBuildResult, CatalogError> {
    Ok(IndexBuildResult::default())
}

fn btbuildempty_stub(_index_relation: RelFileLocator) -> Result<(), CatalogError> {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    crate::backend::access::nbtree::nbtpage::bt_init_meta_page(&mut page, 0, 0, false)
        .map_err(|err| CatalogError::Io(format!("btree metapage init failed: {err:?}")))?;
    Ok(())
}

fn btinsert_stub(_index_relation: RelFileLocator) -> Result<bool, CatalogError> {
    Ok(false)
}

fn btbeginscan_stub(
    index_relation: RelFileLocator,
    _nkeys: usize,
) -> Result<IndexScanDesc, CatalogError> {
    crate::backend::access::index::genam::index_beginscan_stub(index_relation)
}

pub fn btree_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 5,
        amsupport: 5,
        amcanorder: true,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentordering: true,
        amcanbackward: true,
        amcanunique: true,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: true,
        amsearchnulls: true,
        amstorage: false,
        amclusterable: true,
        ampredlocks: true,
        ambuild: Some(btbuild_stub),
        ambuildempty: Some(btbuildempty_stub),
        aminsert: Some(btinsert_stub),
        ambeginscan: Some(btbeginscan_stub),
    }
}
