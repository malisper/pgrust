use std::rc::Rc;

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

use crate::with_state;

// RelationGetStatExtList (relcache.c): rd_statlist equivalent, keyed off the
// relcache state so invalidation drops it with the entry.
#[allow(non_snake_case)]
pub fn RelationGetStatExtList<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    if let Some(hit) = with_state(|st| st.statext_cache.get(&relid).cloned()) {
        let mut out: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, hit.len())?;
        out.extend_from_slice(&hit);
        return Ok(out);
    }
    let oids = relcache_build_seams::scan_pg_statistic_ext_oids::call(mcx, relid)?;
    let built: Rc<[Oid]> = Rc::from(&oids[..]);
    with_state(|st| st.statext_cache.insert(relid, built));
    Ok(oids)
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| {
        st.statext_cache.remove(&relid);
    });
}
