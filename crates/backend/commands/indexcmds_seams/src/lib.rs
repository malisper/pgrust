//! Seam declarations for the `backend-commands-indexcmds` unit
//! (`commands/indexcmds.c`), limited to the helpers `pg_constraint.c` calls.
//!
//! The owning unit (`backend-commands-indexcmds`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use mcx::Mcx;
use types_amapi::CompareType;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::ddlnodes::IndexStmt;

/// `DefineIndex(...)` (indexcmds.c) parameters, bundled to keep the seam call
/// readable. Mirrors the C positional arguments one-for-one.
pub struct DefineIndexArgs<'mcx> {
    /// `Oid tableId` — the relation to index.
    pub table_id: Oid,
    /// `IndexStmt *stmt` — the (already-built) index statement.
    pub stmt: IndexStmt<'mcx>,
    /// `Oid indexRelationId` — preassigned index OID (`InvalidOid` to choose).
    pub index_relation_id: Oid,
    /// `Oid parentIndexId` — parent partitioned-index OID, or `InvalidOid`.
    pub parent_index_id: Oid,
    /// `Oid parentConstraintId` — parent constraint OID, or `InvalidOid`.
    pub parent_constraint_id: Oid,
    /// `int total_parts` — number of leaf partitions, or `-1` if not known.
    pub total_parts: i32,
    /// `bool is_alter_table`.
    pub is_alter_table: bool,
    /// `bool check_rights`.
    pub check_rights: bool,
    /// `bool check_not_in_use`.
    pub check_not_in_use: bool,
    /// `bool skip_build` — make catalog entries but don't build the files.
    pub skip_build: bool,
    /// `bool quiet`.
    pub quiet: bool,
}

seam_core::seam!(
    /// `DefineIndex(tableId, stmt, ...)` (indexcmds.c): create an index per the
    /// given `IndexStmt`, returning the new index's object address. Used by the
    /// bootstrap (BKI) grammar's `DECLARE [UNIQUE] INDEX` actions (with
    /// `skip_build = true`). The owning unit `backend-commands-indexcmds` is not
    /// yet ported, so this panics until it lands. `Err` carries the
    /// `ereport(ERROR)` surface.
    pub fn define_index(mcx: Mcx<'static>, args: DefineIndexArgs<'static>) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `DefineIndex(tableId, stmt, ...)` (indexcmds.c) — the lifetime-generic
    /// form used by ALTER TABLE's `ATExecAddIndex` (which runs on the caller's
    /// `'mcx`, not the backend-`'static` arena the BKI grammar uses). Same
    /// behaviour as [`define_index`]; the owning unit installs both.
    pub fn define_index_full<'mcx>(
        mcx: Mcx<'mcx>,
        args: DefineIndexArgs<'mcx>,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `CheckIndexCompatible(oldId, stmt->accessMethod, stmt->indexParams,
    /// stmt->excludeOpNames, stmt->iswithoutoverlaps)` (indexcmds.c): determine
    /// whether the existing index `old_id` is compatible enough with the new
    /// `IndexStmt` definition that its physical storage can be reused. Used by
    /// `ATPostAlterTypeParse`'s `TryReuseIndex` during an
    /// `ALTER TABLE ... ALTER COLUMN TYPE` rebuild. `Err` carries the
    /// cache-lookup / opclass-resolution `ereport(ERROR)` surface.
    pub fn check_index_compatible<'mcx>(
        mcx: Mcx<'mcx>,
        old_id: Oid,
        stmt: &IndexStmt<'mcx>,
    ) -> PgResult<bool>
);

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
