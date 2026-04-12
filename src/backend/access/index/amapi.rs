use crate::include::access::amapi::IndexAmRoutine;
use crate::include::catalog::BTREE_AM_OID;

pub fn index_am_handler(am_oid: u32) -> Option<IndexAmRoutine> {
    match am_oid {
        BTREE_AM_OID => Some(crate::backend::access::nbtree::btree_am_handler()),
        _ => None,
    }
}
