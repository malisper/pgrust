// Deform-JIT kernel side-cache (rule-5, the indexattr/fkeylist pattern):
// keyed (reloid, ncols), dropped on entry invalidation so an ALTERed table
// can never run a stale kernel. The kernel pins its emission-time tupdesc;
// hits require identity with the CURRENT entry's rd_att, so a rebuilt entry
// (even one whose forget() raced) re-emits rather than aliasing.
use std::rc::Rc;

use jit_deform::DeformKernel;
use types_core::Oid;

use crate::with_state;

pub fn RelationGetDeformKernel(relid: Oid, ncols: u16) -> Option<Rc<DeformKernel>> {
    let rel = with_state(|st| st.id_cache.get(&relid).map(|e| Rc::clone(&e.rel)))?;
    if let Some(k) = with_state(|st| st.deform_jit_cache.get(&(relid, ncols)).cloned()) {
        if k.matches(&rel.rd_att) {
            return Some(k);
        }
    }
    let k = jit_deform::install(&rel.rd_att, ncols as usize)?;
    with_state(|st| st.deform_jit_cache.insert((relid, ncols), Rc::clone(&k)));
    Some(k)
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| st.deform_jit_cache.retain(|k, _| k.0 != relid));
}
