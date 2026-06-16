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
// F3 (DOMAIN) OUTWARD seams — bodies require unported owners reached through
// this unit's own code (the executor for the VALIDATE scans, and the
// parser/ruleutils expression-cook path for DEFAULT/CHECK). Declared here,
// called from the AlterDomain*/DefineDomain code, NOT installed by F3's
// init_seams(): a call panics loudly until the owner lands.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `validateDomainNotNullConstraint(domainoid)` (typecmds.c:3130): scan every
    /// relation column using the domain (`get_rels_with_domain`) and ereport if
    /// any value is NULL.
    ///
    /// OUTWARD seam: the body needs `get_rels_with_domain`'s pg_depend systable
    /// scan + the EXECUTOR table scan (`table_beginscan`/`table_scan_getnextslot`/
    /// `slot_attisnull`), neither reachable here (executor unported). PANICS
    /// until the executor + a get_rels_with_domain owner land.
    pub fn validate_domain_not_null_constraint(domainoid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `validateDomainCheckConstraint(domainoid, ccbin)` (typecmds.c:3196): build
    /// an `ExprState` from `ccbin` (`stringToNode` + `ExecPrepareExpr`), then scan
    /// every relation column using the domain and ereport on any row violating
    /// the CHECK.
    ///
    /// OUTWARD seam: needs `CreateExecutorState`/`ExecPrepareExpr`/`ExecEvalExpr`
    /// + `get_rels_with_domain`. PANICS until the executor lands.
    pub fn validate_domain_check_constraint(domainoid: Oid, ccbin: String) -> PgResult<()>
);

seam_core::seam!(
    /// `cookDefault(pstate, raw_default, atttypid, atttypmod, attname, 0)`
    /// (parser/parse_node.c): parse-analyze + coerce a raw DEFAULT/domain-default
    /// expression to the target type, returning the cooked expression node (or
    /// `None`).
    ///
    /// OUTWARD seam: `cookDefault`'s real owner is `parser/parse_node.c`
    /// (`backend-parser-small1`), but its body drives `transformExpr` +
    /// `coerce_to_target_type` + `assign_expr_collations` over a full
    /// `ParseState`, none reachable from this layer; this `cook_default` is the
    /// node-level surface that AlterDomainDefault/DefineDomain reach. PANICS
    /// until that parser path is wired through.
    pub fn cook_default<'mcx>(
        raw_default: types_nodes::nodes::Node<'mcx>,
        type_id: Oid,
        typmod: i32,
        attname: String,
    ) -> PgResult<Option<types_nodes::nodes::Node<'mcx>>>
);

seam_core::seam!(
    /// `deparse_expression(expr, NIL, false, false)` (utils/adt/ruleutils.c): the
    /// human-readable SQL text of a cooked default/check expression (for
    /// `typdefault` / pg_dump).
    ///
    /// OUTWARD seam: ruleutils.c is unported (NEEDS_DECOMP). PANICS until it lands.
    pub fn deparse_expression<'mcx>(expr: types_nodes::nodes::Node<'mcx>) -> PgResult<String>
);

seam_core::seam!(
    /// `nodeToString(expr)` (nodes/outfuncs.c): the serialized node text of a
    /// cooked default/check expression (for `typdefaultbin` / `conbin`).
    ///
    /// OUTWARD seam: the cooked node only exists on the parser-cook path that is
    /// itself seam-panicked (`cook_default`), so this serialization surface is
    /// reached only there. PANICS until that path is wired.
    pub fn node_to_string<'mcx>(node: types_nodes::nodes::Node<'mcx>) -> PgResult<String>
);

seam_core::seam!(
    /// `domainAddCheckConstraint(...)` (typecmds.c:3504): the shared CREATE/ALTER
    /// DOMAIN CHECK-constraint builder — assign/validate the constraint name,
    /// cook the `raw_expr` into a boolean expression (with a `VALUE`
    /// `CoerceToDomainValue` substitution), and `CreateConstraintEntry`. Returns
    /// the cooked `conbin` text (the C return) and, via `constr_addr`, the new
    /// constraint's OID when requested.
    ///
    /// OUTWARD seam: the cook half (`transformExpr` / `coerce_to_boolean` /
    /// `assign_expr_collations` / `contain_var_clause` / `nodeToString`) is
    /// parser-blocked; `CreateConstraintEntry` is reachable (pg_constraint
    /// ported) but the cook precedes it. Declared as one node-level surface so
    /// the domain CHECK path is a single seam-and-panic until the parser cook is
    /// wired. PANICS until then.
    pub fn domain_add_check_constraint<'mcx>(
        domain_oid: Oid,
        domain_namespace: Oid,
        base_type_oid: Oid,
        typ_mod: i32,
        constr: types_nodes::nodes::Node<'mcx>,
        domain_name: String,
        want_constr_addr: bool,
    ) -> PgResult<(String, Option<ObjectAddress>)>
);

seam_core::seam!(
    /// `domainAddNotNullConstraint(...)` (typecmds.c:3664): the shared CREATE/ALTER
    /// DOMAIN NOT NULL constraint builder — assign/validate the constraint name
    /// and `CreateConstraintEntry(CONSTRAINT_NOTNULL, ...)`. Returns the new
    /// constraint's OID via `constr_addr` when requested.
    ///
    /// `ConstraintNameIsUsed`/`ChooseConstraintName`/`CreateConstraintEntry` are
    /// all in the ported pg_constraint owner; this seam is declared as the unit's
    /// own surface (the C statics live in typecmds.c) and is installed by F3's
    /// `init_seams()` once the pg_constraint dep is wired.
    pub fn domain_add_not_null_constraint<'mcx>(
        domain_oid: Oid,
        domain_namespace: Oid,
        base_type_oid: Oid,
        typ_mod: i32,
        constr: types_nodes::nodes::Node<'mcx>,
        domain_name: String,
        want_constr_addr: bool,
    ) -> PgResult<Option<ObjectAddress>>
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
