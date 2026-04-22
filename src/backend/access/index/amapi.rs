use crate::include::access::amapi::IndexAmRoutine;
use crate::include::catalog::{BRIN_AM_OID, BTREE_AM_OID, GIST_AM_OID, SPGIST_AM_OID};

pub fn index_am_handler(am_oid: u32) -> Option<IndexAmRoutine> {
    match am_oid {
        BTREE_AM_OID => Some(crate::backend::access::nbtree::btree_am_handler()),
        GIST_AM_OID => Some(crate::backend::access::gist::gist_am_handler()),
        SPGIST_AM_OID => Some(crate::backend::access::spgist::spgist_am_handler()),
        BRIN_AM_OID => Some(crate::backend::access::brin::brin_am_handler()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::include::catalog::SPGIST_AM_OID;

    use super::index_am_handler;

    #[test]
    fn spgist_oid_routes_to_native_spgist_handler() {
        let am = index_am_handler(SPGIST_AM_OID).expect("spgist handler");

        assert_eq!(am.amsupport, 7);
        assert!(am.amcanorderbyop);
        assert!(!am.amcanmulticol);
        assert!(!am.amsearchnulls);
    }
}
