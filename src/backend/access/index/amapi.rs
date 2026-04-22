use crate::include::access::amapi::IndexAmRoutine;
use crate::include::catalog::{BTREE_AM_OID, GIST_AM_OID, SPGIST_AM_OID};

pub fn index_am_handler(am_oid: u32) -> Option<IndexAmRoutine> {
    match am_oid {
        BTREE_AM_OID => Some(crate::backend::access::nbtree::btree_am_handler()),
        GIST_AM_OID => Some(crate::backend::access::gist::gist_am_handler()),
        // :HACK: pgrust does not have a native SP-GiST implementation yet.
        // Keep the SQL-visible access method/catalog surface moving by routing
        // the currently supported box opclass through the existing GiST runtime.
        SPGIST_AM_OID => Some(crate::backend::access::gist::gist_am_handler()),
        _ => None,
    }
}
