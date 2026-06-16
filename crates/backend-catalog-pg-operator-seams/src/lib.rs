//! Seam declarations for `catalog/pg_operator.c`.
//!
//! `operatorcmds.c` (CREATE / ALTER / DROP OPERATOR) hands the
//! fully-deconstructed clauses to the pg_operator.c catalog-munging routines
//! (`OperatorCreate`, `OperatorUpd`, `OperatorValidateParams`,
//! `makeOperatorDependencies`) plus the raw operator-tuple I/O. pg_operator.c
//! is not ported yet, so each catalog seam panics until its owner lands —
//! mirror-PG-and-panic.
//!
//! `RemoveOperatorById` is the lone exception: its C lives in operatorcmds.c
//! (it is only *declared* here because dependency.c calls it across a cycle),
//! so `backend-commands-operatorcmds` installs it from its own `init_seams()`.

#![allow(non_snake_case)]

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_operator::FormPgOperator;
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `RemoveOperatorById(operOid)` (operatorcmds.c): the per-class
    /// `OCLASS_OPERATOR` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_operator` object. Removes the operator's catalog row (resetting any
    /// commutator/negator back-links first). Can `ereport(ERROR)`, carried on
    /// `Err`. Owned + installed by `backend-commands-operatorcmds`.
    pub fn RemoveOperatorById(operOid: Oid) -> PgResult<()>
);

/// Argument bundle for `OperatorCreate(...)` (pg_operator.c). Field order
/// mirrors the C parameter list. The qualified commutator/negator name lists
/// are carried as the bare component strings produced by `defGetQualifiedName`.
#[derive(Clone, Debug)]
pub struct OperatorCreateArgs {
    /// operator name
    pub operator_name: String,
    /// namespace
    pub operator_namespace: Oid,
    /// left type id (`InvalidOid` if unary)
    pub left_type: Oid,
    /// right type id
    pub right_type: Oid,
    /// function (`pg_proc` OID) implementing the operator
    pub proc: Oid,
    /// optional commutator operator name (qualified)
    pub commutator_name: Vec<String>,
    /// optional negator operator name (qualified)
    pub negator_name: Vec<String>,
    /// optional restriction-selectivity function OID
    pub restriction_oid: Oid,
    /// optional join-selectivity function OID
    pub join_oid: Oid,
    /// operator merges
    pub can_merge: bool,
    /// operator hashes
    pub can_hash: bool,
}

seam_core::seam!(
    /// `OperatorCreate(...)` (pg_operator.c): inserts the `pg_operator` row
    /// (creating commutator/negator shells as needed) and records its
    /// dependencies, returning its `ObjectAddress`. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn operator_create(args: OperatorCreateArgs) -> PgResult<ObjectAddress>
);

/// Argument bundle for `OperatorValidateParams(...)` (pg_operator.c), the
/// shared sanity checks `OperatorCreate` and `AlterOperator` both run.
#[derive(Clone, Debug)]
pub struct OperatorValidateParamsArgs {
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprresult: Oid,
    pub has_commutator: bool,
    pub has_negator: bool,
    pub has_restriction_selectivity: bool,
    pub has_join_selectivity: bool,
    pub can_merge: bool,
    pub can_hash: bool,
}

seam_core::seam!(
    /// `OperatorValidateParams(...)` (pg_operator.c): the additional checks
    /// `AlterOperator` runs (matching what `OperatorCreate` does). Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn operator_validate_params(args: OperatorValidateParamsArgs) -> PgResult<()>
);

seam_core::seam!(
    /// `OperatorUpd(baseId, commId, negId, isDelete)` (pg_operator.c): fix up
    /// commutator/negator cross-links on the referenced operators. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn operator_upd(
        base_id: Oid,
        comm_id: Oid,
        neg_id: Oid,
        is_delete: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, operOid)` + `GETSTRUCT`, projected to the
    /// fields `operatorcmds.c` reads. Returns `None` when the syscache lookup
    /// misses. Both `RemoveOperatorById` (a fresh cached tuple) and
    /// `AlterOperator` (`SearchSysCacheCopy1`) use this projection.
    pub fn fetch_operator_form(oper_oid: Oid) -> PgResult<Option<FormPgOperator>>
);

seam_core::seam!(
    /// The tuple-touching guts of `RemoveOperatorById`: under
    /// `RowExclusiveLock`, optionally `OperatorUpd(operOid, oprcom, oprnegate,
    /// true)` (re-fetching on a self-commutator/self-negator) and then
    /// `CatalogTupleDelete(relation, &tup->t_self)`. `do_operator_upd` is
    /// `OidIsValid(oprcom) || OidIsValid(oprnegate)`. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn remove_operator_tuple(
        oper_oid: Oid,
        oprcom: Oid,
        oprnegate: Oid,
        do_operator_upd: bool,
    ) -> PgResult<()>
);

/// One attribute change `AlterOperator` packs into the `values`/`replaces`
/// arrays before `heap_modify_tuple` + `CatalogTupleUpdate`.
#[derive(Clone, Copy, Debug)]
pub enum OperatorAttrUpdate {
    Restriction(Oid),
    Join(Oid),
    Commutator(Oid),
    Negator(Oid),
    Merges(bool),
    Hashes(bool),
}

seam_core::seam!(
    /// The held-tuple update path of `AlterOperator`: `heap_modify_tuple(tup,
    /// RelationGetDescr(catalog), values, nulls, replaces)`,
    /// `CatalogTupleUpdate(catalog, &tup->t_self, tup)`, then
    /// `makeOperatorDependencies(tup, false, true)` returning the operator's
    /// `ObjectAddress`. The catalog is opened/closed inside the owner. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_operator_apply(
        oper_oid: Oid,
        updates: Vec<OperatorAttrUpdate>,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `InvokeObjectPostAlterHook(OperatorRelationId, operOid, 0)`
    /// (objectaccess.h). A no-op unless an extension registered the hook; can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn invoke_object_post_alter_hook(oper_oid: Oid) -> PgResult<()>
);
