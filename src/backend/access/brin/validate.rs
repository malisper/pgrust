use std::collections::{BTreeMap, BTreeSet};

use crate::include::access::brin_internal::{
    BRIN_MANDATORY_NPROCS, BRIN_PROCNUM_STRATEGY_GE, BRIN_PROCNUM_STRATEGY_GT,
    BRIN_PROCNUM_STRATEGY_LE, BRIN_PROCNUM_STRATEGY_LT,
};
use crate::include::catalog::{
    BRIN_AM_OID, PgAmopRow, PgAmprocRow, PgOpclassRow, bootstrap_pg_amop_rows,
    bootstrap_pg_amproc_rows, bootstrap_pg_opclass_rows,
};

fn validate_brin_catalog(
    opclasses: &[PgOpclassRow],
    amproc_rows: &[PgAmprocRow],
    amop_rows: &[PgAmopRow],
) -> bool {
    let mut family_procnums = BTreeMap::<(u32, u32), BTreeSet<i16>>::new();
    for row in amproc_rows {
        family_procnums
            .entry((row.amprocfamily, row.amproclefttype))
            .or_default()
            .insert(row.amprocnum);
    }

    let mut family_strategies = BTreeMap::<(u32, u32), BTreeSet<i16>>::new();
    for row in amop_rows {
        if row.amoppurpose != 's' {
            continue;
        }
        family_strategies
            .entry((row.amopfamily, row.amoplefttype))
            .or_default()
            .insert(row.amopstrategy);
    }

    for opclass in opclasses.iter().filter(|row| row.opcmethod == BRIN_AM_OID) {
        let procnums = family_procnums.get(&(opclass.opcfamily, opclass.opcintype));
        for required in [
            1_i16,
            2,
            3,
            BRIN_MANDATORY_NPROCS,
            BRIN_PROCNUM_STRATEGY_LT,
            BRIN_PROCNUM_STRATEGY_LE,
            BRIN_PROCNUM_STRATEGY_GE,
            BRIN_PROCNUM_STRATEGY_GT,
        ] {
            if !procnums.is_some_and(|present| present.contains(&required)) {
                return false;
            }
        }

        let strategies = family_strategies.get(&(opclass.opcfamily, opclass.opcintype));
        for required in [1_i16, 2, 3, 4, 5] {
            if !strategies.is_some_and(|present| present.contains(&required)) {
                return false;
            }
        }
    }

    true
}

pub(crate) fn validate_brin_am() -> bool {
    validate_brin_catalog(
        &bootstrap_pg_opclass_rows(),
        &bootstrap_pg_amproc_rows(),
        &bootstrap_pg_amop_rows(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_brin_bootstrap_rows() {
        assert!(validate_brin_am());
    }

    #[test]
    fn validate_brin_rejects_missing_required_proc() {
        let opclasses = bootstrap_pg_opclass_rows();
        let mut amprocs = bootstrap_pg_amproc_rows();
        let amops = bootstrap_pg_amop_rows();

        amprocs.retain(|row| {
            !(row.amprocnum == BRIN_PROCNUM_STRATEGY_LT
                && row.amprocfamily == crate::include::catalog::BRIN_INTEGER_MINMAX_FAMILY_OID
                && row.amproclefttype == crate::include::catalog::INT4_TYPE_OID)
        });

        assert!(!validate_brin_catalog(&opclasses, &amprocs, &amops));
    }
}
