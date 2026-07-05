use std::rc::Rc;

use types_core::Oid;
use types_error::PgResult;
use types_rel::ForeignKeyCacheInfo;

use crate::with_state;

// RelationGetFKeyList (relcache.c): rd_fkeylist equivalent, keyed off the
// relcache state so invalidation drops it with the entry. Shared read-only
// (C callers get the cache's own list and must not modify it).
#[allow(non_snake_case)]
pub fn RelationGetFKeyList(relid: Oid) -> PgResult<Rc<[ForeignKeyCacheInfo]>> {
    if let Some(hit) = with_state(|st| st.fkey_cache.get(&relid).cloned()) {
        return Ok(hit);
    }
    let cx = mcx::MemoryContext::new("RelationGetFKeyList");
    let infos = relcache_build_seams::scan_pg_constraint_fkeys::call(cx.mcx(), relid)?;
    let built: Rc<[ForeignKeyCacheInfo]> = Rc::from(&infos[..]);
    with_state(|st| st.fkey_cache.insert(relid, Rc::clone(&built)));
    Ok(built)
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| {
        st.fkey_cache.remove(&relid);
    });
}
