//! Seam declarations for the `pg_constraint` catalog access `ri_triggers.c`'s
//! `ri_LoadConstraintInfo` performs: the `SearchSysCache1(CONSTROID)` +
//! `DeconstructFkConstraintRow` projection, the `GetSysCacheHashValue1` and
//! cache-callback registration, the `conparentid`-to-root walk, and the
//! temporal-FK `FindFKPeriodOpers` opclass resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_ri_triggers::{FkConstraintRow, PeriodOpers};

seam_core::seam!(
    /// Register `InvalidateConstraintCacheCallBack` for `CONSTROID`
    /// (`CacheRegisterSyscacheCallback`). Called once at hashtable init. Can
    /// `ereport(ERROR)` if the callback array is full, carried on `Err`.
    pub fn register_constraint_inval_callback() -> PgResult<()>
);
seam_core::seam!(
    /// Fetch the FK `pg_constraint` row + identity/hash fields, running
    /// `SearchSysCache1(CONSTROID)`, the `contype == CONSTRAINT_FOREIGN`
    /// check, `DeconstructFkConstraintRow`, and `GetSysCacheHashValue1`. The
    /// row is copied into `mcx`. `Ok(None)` if no such tuple exists (the C
    /// caller `elog(ERROR)`s "cache lookup failed"). Can `ereport(ERROR)`.
    pub fn load_fk_constraint<'mcx>(
        mcx: Mcx<'mcx>,
        constraint_oid: Oid,
    ) -> PgResult<Option<FkConstraintRow<'mcx>>>
);
seam_core::seam!(
    /// `GetSysCacheHashValue1(CONSTROID, ObjectIdGetDatum(oid))`.
    pub fn constraint_hash_value(oid: Oid) -> PgResult<u32>
);
seam_core::seam!(
    /// `get_ri_constraint_root(constr_oid)` — walk `conparentid` to the root.
    /// Can `ereport(ERROR)` (cache lookup), carried on `Err`.
    pub fn get_ri_constraint_root(constr_oid: Oid) -> PgResult<Oid>
);
seam_core::seam!(
    /// `FindFKPeriodOpers(opclass, ...)` (pg_constraint.c:1666) — resolve the
    /// PERIOD contained-by / agged-contained-by / intersect operators from the
    /// PERIOD element's `opclass`. Callers pass the opclass directly (matching
    /// the C signature): `tablecmds.c` from `opclasses[numpks - 1]`, and
    /// `ri_triggers.c` from `get_index_column_opclass(conindid, nkeys)`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn find_fk_period_opers(opclass: Oid) -> PgResult<PeriodOpers>
);
seam_core::seam!(
    /// `get_catalog_object_by_oid(pg_constraint, Anum_pg_constraint_oid,
    /// constroid)` + `((Form_pg_constraint) GETSTRUCT(constrTup))` projected to
    /// `(conrelid, contypid, oid)` — the table- vs domain-constraint
    /// disambiguation `getConstraintTypeDescription` /
    /// `getConstraintIdentity` perform (objectaddress.c). `Ok(None)` when no
    /// such row exists (the C caller's `missing_ok` fallback / `elog(ERROR)`);
    /// the installer owns the `table_open`/`table_close(AccessShareLock)`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn constraint_type_oids(constroid: Oid) -> PgResult<Option<(Oid, Oid, Oid)>>
);

seam_core::seam!(
    /// `get_relation_constraint_oid(relid, conname, missing_ok)`
    /// (pg_constraint.c): the OID of the named constraint on relation `relid`,
    /// or `InvalidOid` with `missing_ok = true`. With `missing_ok = false` a
    /// miss raises `ERRCODE_UNDEFINED_OBJECT` (`Err`).
    pub fn get_relation_constraint_oid(
        mcx: Mcx<'_>,
        relid: Oid,
        conname: &str,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_domain_constraint_oid(typid, conname, missing_ok)`
    /// (pg_constraint.c): the OID of the named constraint on domain `typid`, or
    /// `InvalidOid` with `missing_ok = true`. With `missing_ok = false` a miss
    /// raises `ERRCODE_UNDEFINED_OBJECT` (`Err`).
    pub fn get_domain_constraint_oid(
        mcx: Mcx<'_>,
        typid: Oid,
        conname: &str,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RemoveConstraintById(conId)` (catalog/pg_constraint.c): the per-class
    /// `OCLASS_CONSTRAINT` drop handler dependency.c's `doDeletion` invokes for
    /// a `pg_constraint` object. Removes the constraint's catalog row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveConstraintById(conId: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `findDomainNotNullConstraint(typid)` (pg_constraint.c) reduced to the OID
    /// the caller actually reads (`((Form_pg_constraint) GETSTRUCT(conTup))->oid`):
    /// the OID of the domain's NOT NULL constraint, or `InvalidOid` if none.
    /// Consumed by `AlterDomainNotNull` (typecmds.c). Can `ereport(ERROR)`.
    pub fn find_domain_not_null_constraint_oid(mcx: Mcx<'_>, typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// The pg_constraint half of `AlterDomainDropConstraint` (typecmds.c:2860):
    /// the `systable_beginscan(ConstraintRelidTypidNameIndexId, conrelid=Invalid,
    /// contypid=domainoid, conname=constrName)` scan + `performDeletion` of the
    /// at-most-one matching row, returning `(found, was_notnull)` so the caller
    /// can clear `pg_type.typnotnull` for a dropped NOT NULL constraint and apply
    /// the `missing_ok` NOTICE/ERROR. `behavior` is the DROP behavior. Can
    /// `ereport(ERROR)`.
    pub fn drop_domain_constraint(
        mcx: Mcx<'_>,
        domainoid: Oid,
        constr_name: String,
        behavior: types_nodes::parsenodes::DropBehavior,
    ) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// The pg_constraint catalog half of `AlterDomainValidateConstraint`
    /// (typecmds.c:3031): locate the CHECK constraint of `domainoid` named
    /// `constr_name`, return its cooked `conbin` text (for the executor VALIDATE)
    /// and its OID; then `set_constraint_validated` flips `convalidated`. Errors
    /// if the constraint does not exist or is not a CHECK constraint. Can
    /// `ereport(ERROR)`.
    pub fn find_domain_check_constraint(
        mcx: Mcx<'_>,
        domainoid: Oid,
        constr_name: String,
    ) -> PgResult<(Oid, String)>
);

seam_core::seam!(
    /// `copy_con->convalidated = true; CatalogTupleUpdate` for the constraint OID
    /// (the catalog-write half of `AlterDomainValidateConstraint`,
    /// typecmds.c:3106). Can `ereport(ERROR)`.
    pub fn set_constraint_validated(mcx: Mcx<'_>, con_oid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterConstraintNamespaces(ownerId, oldNspId, newNspId, isType, objsMoved)`
    /// (pg_constraint.c): move every constraint of the object to the new schema,
    /// recording each in `objsMoved`. Consumed by `AlterTypeNamespaceInternal`
    /// (typecmds.c) for both the composite-rel and domain paths. Can
    /// `ereport(ERROR)`.
    pub fn alter_constraint_namespaces(
        mcx: Mcx<'_>,
        owner_id: Oid,
        old_nsp_id: Oid,
        new_nsp_id: Oid,
        is_type: bool,
        objs_moved: &mut types_catalog::catalog_dependency::ObjectAddresses,
    ) -> PgResult<()>
);

pub use types_catalog::pg_constraint::ConstraintCategory;

seam_core::seam!(
    /// `ConstraintNameIsUsed(conCat, objId, conname)` (pg_constraint.c): is the
    /// given constraint name already in use for the relation / domain `objId`?
    /// Scans `pg_constraint` by `conname` + namespace. `Err` carries the
    /// catalog-scan `ereport(ERROR)`s.
    pub fn constraint_name_is_used(
        mcx: Mcx<'_>,
        con_cat: ConstraintCategory,
        obj_id: Oid,
        conname: &str,
    ) -> PgResult<bool>
);

/// Arguments to [`create_constraint_entry`], trimmed to the fields
/// `index_constraint_create` (catalog/index.c) supplies for a PRIMARY KEY /
/// UNIQUE / EXCLUDE index constraint. The C `CreateConstraintEntry` takes the
/// full ~30-parameter list; the foreign-key legs (`foreignRelId`, the
/// `pfEqOp`/`ppEqOp`/`ffEqOp` arrays, the FK action chars, `fkDeleteSetCols`),
/// the CHECK-expression legs (`conExpr`/`conBin`), and `domainId` are all
/// NULL/`InvalidOid`/`' '`/`0` at this call site and are not carried; the owner
/// re-supplies those constants. `isEnforced` / `isValidated` are both `true`
/// here.
#[derive(Debug)]
pub struct CreateConstraintEntryArgs<'a> {
    /// `const char *constraintName`.
    pub constraint_name: &'a str,
    /// `Oid constraintNamespace`.
    pub constraint_namespace: Oid,
    /// `char constraintType`.
    pub constraint_type: i8,
    /// `bool isDeferrable`.
    pub is_deferrable: bool,
    /// `bool isDeferred`.
    pub is_deferred: bool,
    /// `Oid parentConstrId`.
    pub parent_constr_id: Oid,
    /// `Oid relId` — `RelationGetRelid(heapRelation)`.
    pub rel_id: Oid,
    /// `const int16 *constraintKey` — `indexInfo->ii_IndexAttrNumbers`, the FULL
    /// `constraintNTotalKeys` (= `ii_NumIndexAttrs`) elements (key + INCLUDE
    /// columns). The first `constraint_n_keys` form `conkey`; all
    /// `constraint_n_total_keys` are walked for the column dependencies.
    pub constraint_key: &'a [i16],
    /// `int constraintNKeys` — `indexInfo->ii_NumIndexKeyAttrs` (the key columns
    /// only; drives `conkey`/`conexclop`).
    pub constraint_n_keys: i32,
    /// `int constraintNTotalKeys` — `indexInfo->ii_NumIndexAttrs`.
    pub constraint_n_total_keys: i32,
    /// `Oid indexRelId`.
    pub index_rel_id: Oid,
    /// `const Oid *exclOp` — `indexInfo->ii_ExclusionOps`
    /// (`None` for non-exclusion constraints), `constraintNKeys` elements.
    pub excl_op: Option<&'a [Oid]>,
    /// `bool conIsLocal`.
    pub con_is_local: bool,
    /// `int16 conInhCount`.
    pub con_inh_count: i16,
    /// `bool conNoInherit`.
    pub con_no_inherit: bool,
    /// `bool conPeriod` — `is_without_overlaps`.
    pub con_period: bool,
    /// `bool is_internal`.
    pub is_internal: bool,
}

seam_core::seam!(
    /// `CreateConstraintEntry(...)` (pg_constraint.c): construct a
    /// `pg_constraint` entry and its dependencies. Returns the new constraint's
    /// OID. `Err` carries the catalog-mutation `ereport(ERROR)`s.
    pub fn create_constraint_entry(mcx: Mcx<'_>, args: CreateConstraintEntryArgs<'_>) -> PgResult<Oid>
);

seam_core::seam!(
    /// `index_concurrently_swap`'s "move constraints and triggers over to the
    /// new index" leg (catalog/index.c:1656-1724): for every constraint OID in
    /// `constraint_oids` whose `conindid == old_index_id`, set `conindid =
    /// new_index_id`; then scan `pg_trigger` by `tgconstraint` and move any
    /// `tgconstrindid == old_index_id` to `new_index_id`. Owned by the
    /// `pg_constraint` unit (it owns the constraint catalog and can reach
    /// `pg_trigger` through genam). `Err` carries the catalog-mutation
    /// `ereport(ERROR)`s.
    pub fn swap_index_constraints_and_triggers(
        constraint_oids: &[Oid],
        old_index_id: Oid,
        new_index_id: Oid,
    ) -> PgResult<()>
);
