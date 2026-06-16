//! Seam declarations for the `backend-commands-typecmds` unit
//! (`commands/typecmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_parsenodes::Node;

/// Owned projection of the `RangeVar *typevar` that `DefineCompositeType`
/// passes to `DefineRelation` (the composite relation's name + persistence).
/// Carried by value so the seam does not bind an arena lifetime; the owner
/// reconstructs the `CreateStmt->relation` from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeCmdsRangeVar {
    /// `catalogname` — None if unqualified.
    pub catalogname: Option<String>,
    /// `schemaname` — None if unqualified.
    pub schemaname: Option<String>,
    /// `relname`.
    pub relname: Option<String>,
    /// `bool inh`.
    pub inh: bool,
    /// `char relpersistence`.
    pub relpersistence: i8,
    /// `ParseLoc location`.
    pub location: i32,
}

seam_core::seam!(
    /// `AlterTypeOwner_oid(typeOid, newOwnerId, hasDependEntry)` (typecmds.c):
    /// change a type's owner during REASSIGN OWNED. `hasDependEntry` is the C
    /// flag telling the routine a pg_shdepend OWNER entry already exists. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_type_owner_oid(
        type_oid: Oid,
        new_owner_id: Oid,
        has_depend_entry: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveTypeById(typeOid)` (commands/typecmds.c): the per-class
    /// `OCLASS_TYPE` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_type` object. Removes the type's catalog row. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn RemoveTypeById(typeOid: Oid) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// OUTWARD seams — these are typecmds.c statics (`makeRangeConstructors`,
// `makeMultirangeConstructors`) and the tablecmds.c `DefineRelation` call that
// `DefineCompositeType` makes. Their bodies require `ProcedureCreate`
// (catalog/pg_proc.c) and `DefineRelation` (commands/tablecmds.c), neither of
// which is ported yet. They are declared here and called from `DefineRange` /
// `DefineCompositeType`, but are NOT installed by F2's `init_seams()`: until
// their unported dependencies (`ProcedureCreate`/`DefineRelation`) land, a call
// panics loudly. This is the sanctioned seam-and-panic pattern for an unported
// dependency reached through this unit's own code.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `makeRangeConstructors(name, namespace, rangeOid, subtype)`
    /// (typecmds.c:1770): create the 2- and 3-arg range constructor functions
    /// (`range_constructor2`/`range_constructor3`) via `ProcedureCreate` and
    /// record their `DEPENDENCY_INTERNAL` dependency on the range type.
    ///
    /// PANICS until `ProcedureCreate` (catalog/pg_proc.c) is ported.
    pub fn make_range_constructors(
        name: String,
        namespace: Oid,
        range_oid: Oid,
        subtype: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `makeMultirangeConstructors(name, namespace, multirangeOid, rangeOid,
    /// rangeArrayOid, &castFuncOid)` (typecmds.c:1845): create the 0/1/variadic
    /// multirange constructors via `ProcedureCreate`, recording their
    /// dependencies, and return the OID of the constructor usable as the
    /// range→multirange cast function (`*castFuncOid`).
    ///
    /// PANICS until `ProcedureCreate` (catalog/pg_proc.c) is ported.
    pub fn make_multirange_constructors(
        name: String,
        namespace: Oid,
        multirange_oid: Oid,
        range_oid: Oid,
        range_array_oid: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `DefineRelation(createStmt, RELKIND_COMPOSITE_TYPE, InvalidOid, &address,
    /// NULL)` (tablecmds.c) as called by `DefineCompositeType` (typecmds.c:2600).
    /// The composite `CreateStmt` is built from `typevar` + `coldeflist` inside
    /// the owner; it returns the created relation's `ObjectAddress`.
    ///
    /// PANICS until `DefineRelation` (commands/tablecmds.c) is ported.
    pub fn define_relation_composite(
        typevar: TypeCmdsRangeVar,
        coldeflist: Vec<Node>,
    ) -> PgResult<ObjectAddress>
);

// ---------------------------------------------------------------------------
// Generic ALTER dispatch targets driven by commands/alter.c.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `RenameType(RenameStmt *stmt)` (typecmds.c) — ALTER TYPE/DOMAIN RENAME TO.
    pub fn RenameType<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_parsenodes::RenameStmt,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `AlterTypeNamespace(List *names, const char *newschema, ObjectType
    /// objecttype, Oid *oldschema)` (typecmds.c) — ALTER TYPE/DOMAIN SET SCHEMA.
    /// `names` is the qualified type-name `List *` node. When `want_oldschema`
    /// is true the previous schema OID rides the tuple's second slot.
    pub fn AlterTypeNamespace<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        names: &Node,
        newschema: &str,
        objecttype: types_nodes::parsenodes::ObjectType,
        want_oldschema: bool,
    ) -> PgResult<(ObjectAddress, Oid)>
);

seam_core::seam!(
    /// `AlterTypeNamespace_oid(Oid typeOid, Oid nspOid, bool ignoreDependent,
    /// ObjectAddresses *objsMoved)` (typecmds.c) — move a type to `nspOid` by
    /// OID, used by ALTER EXTENSION SET SCHEMA. Returns the previous schema OID.
    pub fn AlterTypeNamespace_oid(
        type_oid: Oid,
        nsp_oid: Oid,
        ignore_dependent: bool,
        objs_moved: &mut types_catalog::catalog_dependency::ObjectAddresses,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `AlterTypeOwner(List *names, Oid newOwnerId, ObjectType objecttype)`
    /// (typecmds.c) — ALTER TYPE/DOMAIN OWNER TO. `names` is the qualified
    /// type-name `List *` node.
    pub fn AlterTypeOwner<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        names: &Node,
        new_owner_id: Oid,
        objecttype: types_nodes::parsenodes::ObjectType,
    ) -> PgResult<ObjectAddress>
);
