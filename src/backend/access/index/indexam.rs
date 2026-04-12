use crate::backend::catalog::CatalogError;
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::include::access::amapi::IndexBuildResult;

pub fn index_build_stub(
    heap_relation: RelFileLocator,
    index_relation: RelFileLocator,
    am_oid: u32,
    smgr: &mut impl StorageManager,
) -> Result<IndexBuildResult, CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    let _ = smgr.open(index_relation);
    let _ = smgr.create(index_relation, ForkNumber::Main, false);
    if let Some(ambuildempty) = routine.ambuildempty {
        ambuildempty(index_relation)?;
    }
    if let Some(ambuild) = routine.ambuild {
        ambuild(heap_relation, index_relation)
    } else {
        Ok(IndexBuildResult::default())
    }
}
