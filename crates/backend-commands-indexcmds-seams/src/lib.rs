//! Seam declarations for the `backend-commands-indexcmds` unit
//! (`commands/indexcmds.c`), limited to the helpers `pg_constraint.c` calls.
//!
//! The owning unit (`backend-commands-indexcmds`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use mcx::Mcx;
use types_amapi::CompareType;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::ddlnodes::IndexStmt;

seam_core::seam!(
    /// `makeObjectName(name1, name2, label)` (indexcmds.c): build an object name
    /// of the form `name1_name2_label`, truncating the components as needed to
    /// fit `NAMEDATALEN`. Returns a freshly-allocated name string. Used by
    /// `ChooseConstraintName`. `Err` carries OOM.
    pub fn make_object_name(name1: &str, name2: &str, label: &str) -> PgResult<String>
);

seam_core::seam!(
    /// `GetOperatorFromCompareType(opclass, rhstype, cmptype, &op, &strat)`
    /// (indexcmds.c): resolve the operator OID + opfamily strategy number for
    /// the given comparison type against `opclass` (and optional `rhstype`).
    /// Returns `(operator_oid, strategy_number)`. Used by `FindFKPeriodOpers`.
    /// `Err` carries the cache-lookup `ereport(ERROR)`s.
    pub fn get_operator_from_compare_type(
        opclass: Oid,
        rhstype: Oid,
        cmptype: CompareType,
    ) -> PgResult<(Oid, u16)>
);
