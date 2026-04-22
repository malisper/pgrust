use std::collections::{BTreeMap, BTreeSet};

use crate::include::access::gist::{
    GIST_CONSISTENT_PROC, GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_PENALTY_PROC,
    GIST_PICKSPLIT_PROC, GIST_TRANSLATE_CMPTYPE_PROC, GIST_UNION_PROC,
};
use crate::include::access::spgist::{
    SPGIST_CHOOSE_PROC, SPGIST_CONFIG_PROC, SPGIST_INNER_CONSISTENT_PROC,
    SPGIST_LEAF_CONSISTENT_PROC, SPGIST_PICKSPLIT_PROC,
};
use crate::include::catalog::{
    GIST_AM_OID, SPGIST_AM_OID, bootstrap_pg_amop_rows, bootstrap_pg_amproc_rows,
    bootstrap_pg_opclass_rows,
};

pub fn validate_index_am(am_oid: u32) -> bool {
    if crate::backend::access::index::amapi::index_am_handler(am_oid).is_none() {
        return false;
    }
    if am_oid != GIST_AM_OID && am_oid != SPGIST_AM_OID {
        return true;
    }

    let proc_rows = bootstrap_pg_amproc_rows();
    let amop_rows = bootstrap_pg_amop_rows();
    let opclasses = bootstrap_pg_opclass_rows()
        .into_iter()
        .filter(|row| row.opcmethod == am_oid)
        .collect::<Vec<_>>();

    let mut family_procnums = BTreeMap::<u32, BTreeSet<i16>>::new();
    for row in proc_rows {
        family_procnums
            .entry(row.amprocfamily)
            .or_default()
            .insert(row.amprocnum);
    }

    for opclass in opclasses {
        let present = family_procnums.get(&opclass.opcfamily);
        let required_procnums: &[i16] = if am_oid == GIST_AM_OID {
            &[
                GIST_CONSISTENT_PROC,
                GIST_UNION_PROC,
                GIST_PENALTY_PROC,
                GIST_PICKSPLIT_PROC,
                GIST_EQUAL_PROC,
            ]
        } else {
            &[
                SPGIST_CONFIG_PROC,
                SPGIST_CHOOSE_PROC,
                SPGIST_PICKSPLIT_PROC,
                SPGIST_INNER_CONSISTENT_PROC,
                SPGIST_LEAF_CONSISTENT_PROC,
            ]
        };
        for required in required_procnums {
            if !present.is_some_and(|procnums| procnums.contains(&required)) {
                return false;
            }
        }
    }

    for row in &amop_rows {
        if row.amopmethod != am_oid || row.amoppurpose != 'o' {
            continue;
        }
        let Some(procnums) = family_procnums.get(&row.amopfamily) else {
            return false;
        };
        if am_oid == GIST_AM_OID {
            if !procnums.contains(&GIST_DISTANCE_PROC) {
                return false;
            }
            if row.amopsortfamily != 0 && !procnums.contains(&GIST_TRANSLATE_CMPTYPE_PROC) {
                return false;
            }
        } else if row.amopsortfamily == 0 {
            return false;
        }
    }

    true
}
