// AlterTable three-phase machinery (ATController): ADD/DROP COLUMN,
// SET/DROP DEFAULT, SET/DROP NOT NULL, ADD CONSTRAINT CHECK, ALTER TYPE
// (no-USING; rewrite via the cluster lane). LOUD: other subtypes,
// inheritance children, partitions, USING, index rebuilds on rewrite.
// Ownership checks ride the aclchk lane (superuser fast path only).
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, DEFAULT_COLLATION_OID, RELATION_RELATION_ID, TYPE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_NOT_NULL_VIOLATION, ERRCODE_TOO_MANY_COLUMNS, ERRCODE_UNDEFINED_COLUMN, ERROR,
    NOTICE,
};
use types_nodes::parsenodes::{AlterTableCmd, AlterTableStmt, AlterTableType};
use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, TypeName};
use types_nodes::{Node, NodeList};
use types_rel::{AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, ShareRowExclusiveLock, LOCKMODE, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{MaxHeapAttributeNumber, TupleDescData};

const AT_NUM_PASSES: usize = 12;
const AT_PASS_DROP: usize = 0;
const AT_PASS_ALTER_TYPE: usize = 1;
const AT_PASS_ADD_COL: usize = 2;
const AT_PASS_SET_EXPRESSION: usize = 3;
const AT_PASS_ADD_CONSTR: usize = 6;
const AT_PASS_COL_ATTRS: usize = 7;
const AT_PASS_ADD_INDEXCONSTR: usize = 8;
const AT_PASS_ADD_INDEX: usize = 9;
const AT_PASS_ADD_OTHERCONSTR: usize = 10;
const AT_PASS_MISC: usize = 11;
const AT_REWRITE_DEFAULT_VAL: i32 = 1 << 1;
const AT_REWRITE_COLUMN_REWRITE: i32 = 1 << 2;
const AT_REWRITE_ACCESS_METHOD: i32 = 1 << 3;

pub(crate) const Anum_pg_attribute_attname: usize = 2;
const Anum_pg_attribute_atttypid: usize = 3;
const Anum_pg_attribute_attlen: usize = 4;
const Anum_pg_attribute_atttypmod: usize = 6;
const Anum_pg_attribute_attndims: usize = 7;
const Anum_pg_attribute_attbyval: usize = 8;
const Anum_pg_attribute_attalign: usize = 9;
const Anum_pg_attribute_attstorage: usize = 10;
const Anum_pg_attribute_attcompression: usize = 11;
pub(crate) const Anum_pg_attribute_attnotnull: usize = 12;
const Anum_pg_attribute_atthasmissing: usize = 14;
const Anum_pg_attribute_attgenerated: usize = 16;
const Anum_pg_attribute_attcollation: usize = 20;

const AttributeRelidNumIndexId: Oid = 2659;
const InheritsRelationId: Oid = 2611;
const InheritsParentIndexId: Oid = 2187;
const Anum_pg_inherits_inhparent: usize = 2;
const Anum_pg_class_relnatts: usize = 19;
const CollationRelationId: Oid = 3456;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: tablecmds ALTER {what}")
}

pub(crate) fn oid_scankey(attno: usize, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

pub fn AlterTableGetLockLevel(cmds: &NodeList<'_>) -> LOCKMODE {
    let mut lockmode = types_rel::ShareUpdateExclusiveLock;
    for cnode in cmds.iter() {
        let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
        let cmd_lockmode = match cmd.subtype {
            AlterTableType::AT_AddColumn
            | AlterTableType::AT_DropColumn
            | AlterTableType::AT_ColumnDefault
            | AlterTableType::AT_DropNotNull
            | AlterTableType::AT_SetNotNull
            | AlterTableType::AT_AlterColumnType
            | AlterTableType::AT_CookedColumnDefault
            | AlterTableType::AT_SetExpression
            | AlterTableType::AT_DropExpression
            | AlterTableType::AT_DropConstraint
            | AlterTableType::AT_AddIndex
            | AlterTableType::AT_AddIndexConstraint
            | AlterTableType::AT_SetStorage => AccessExclusiveLock,
            AlterTableType::AT_SetStatistics => types_rel::ShareUpdateExclusiveLock,
            AlterTableType::AT_AddConstraint => {
                let constr = cmd
                    .def
                    .expect("AT_AddConstraint Constraint")
                    .as_variant::<Constraint>()
                    .expect("Constraint");
                match constr.contype {
                    ConstrType::CONSTR_FOREIGN => ShareRowExclusiveLock,
                    _ => AccessExclusiveLock,
                }
            }
            AlterTableType::AT_EnableRowSecurity
            | AlterTableType::AT_DisableRowSecurity
            | AlterTableType::AT_ForceRowSecurity
            | AlterTableType::AT_NoForceRowSecurity => AccessExclusiveLock,
            AlterTableType::AT_EnableRule
            | AlterTableType::AT_EnableAlwaysRule
            | AlterTableType::AT_EnableReplicaRule
            | AlterTableType::AT_DisableRule => AccessExclusiveLock,
            AlterTableType::AT_ReplicaIdentity
            | AlterTableType::AT_AddOf
            | AlterTableType::AT_DropOf
            | AlterTableType::AT_SetTableSpace
            | AlterTableType::AT_SetAccessMethod
            | AlterTableType::AT_GenericOptions => AccessExclusiveLock,
            other => unported(&format!("AlterTableGetLockLevel {other:?}")),
        };
        if cmd_lockmode > lockmode {
            lockmode = cmd_lockmode;
        }
    }
    lockmode
}

pub fn AlterTableLookupRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTableStmt<'_>,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    AlterTableLookupRangeVar(
        mcx,
        stmt.relation.expect("AlterTableStmt.relation"),
        lockmode,
        stmt.missing_ok,
    )
}

pub(crate) fn AlterTableLookupRangeVar<'mcx>(
    mcx: Mcx<'mcx>,
    prv: &types_nodes::primnodes::RangeVar<'_>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Oid> {
    let rv = rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    };
    let mut callback = |rv: &rel_vocab::RangeVar<'_>, relOid: Oid, _old: Oid| {
        RangeVarCallbackForAlterRelation(mcx, rv, relOid)
    };
    let flags = if missing_ok { catalog_namespace::RVR_MISSING_OK } else { 0 };
    catalog_namespace::RangeVarGetRelidExtended(&rv, lockmode, flags, Some(&mut callback))
}

// RangeVarCallbackForAlterRelation slice: relkind gate + superuser ownership
// fast path (per-role object_ownercheck rides the aclchk lane).
fn RangeVarCallbackForAlterRelation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel_vocab::RangeVar<'_>,
    relOid: Oid,
) -> PgResult<()> {
    if relOid == InvalidOid {
        return Ok(());
    }
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        unported("RangeVarCallbackForAlterRelation: object_ownercheck for non-superusers");
    }
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_scankey(1, relOid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
        return Ok(());
    };
    let desc = pg_class.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_class columns under pg_class's descriptor.
    let relnamespace = unsafe { types_tuple::heap_getattr(tup, 3, desc, &mut isnull) }.as_oid();
    // SAFETY: as above.
    let relkind =
        unsafe { types_tuple::heap_getattr(tup, 18, desc, &mut isnull) }.as_i8() as u8;
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;

    if relkind != RELKIND_RELATION {
        unported("RangeVarCallbackForAlterRelation: non-plain-table relkind");
    }
    let is_system =
        catalog::IsCatalogRelationOid(relOid) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied: \"{}\" is a system catalog", rel.relname),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

// expr is over *old* table values, except when is_generated: then it is
// over the new tuple (tablecmds.c NewColumnValue).
struct NewColumnValue<'mcx> {
    attnum: AttrNumber,
    expr: Node<'mcx>,
    is_generated: bool,
}

struct NewConstraint<'mcx> {
    name: &'mcx str,
    qual: Node<'mcx>,
}

struct AlteredTableInfo<'mcx> {
    relid: Oid,
    old_desc: std::rc::Rc<TupleDescData<'mcx>>,
    subcmds: [NodeList<'mcx>; AT_NUM_PASSES],
    rewrite: i32,
    new_tablespace: Oid,
    chg_access_method: bool,
    new_access_method: Oid,
    has_newvals: bool,
    verify_new_notnull: bool,
    newvals: PgVec<'mcx, NewColumnValue<'mcx>>,
    constraints: PgVec<'mcx, NewConstraint<'mcx>>,
    fk_checks: PgVec<'mcx, crate::fk::FkValidateItem<'mcx>>,
}

pub fn AlterTable<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    lockmode: LOCKMODE,
    stmt: &AlterTableStmt<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let rel = table::table_open(mcx, relid, NoLock)?;
    // CheckAlterTableIsSafe: other-session temp tables are unreachable
    // (temp relations unported).
    catalog_heap::CheckTableNotInUse(&rel, "ALTER TABLE")?;
    let recurse = stmt.relation.expect("AlterTableStmt.relation").inh;
    ATController(mcx, rel, &stmt.cmds, recurse, lockmode, query_string)
}

fn ATController<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    cmds: &NodeList<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    let mut tab = AlteredTableInfo {
        relid: rel.rd_id,
        old_desc: rel.rd_att.clone(),
        subcmds: core::array::from_fn(|_| NodeList::nil()),
        rewrite: 0,
        new_tablespace: InvalidOid,
        chg_access_method: false,
        new_access_method: InvalidOid,
        has_newvals: false,
        verify_new_notnull: false,
        newvals: PgVec::new_in(mcx),
        constraints: PgVec::new_in(mcx),
        fk_checks: PgVec::new_in(mcx),
    };

    for cnode in cmds.iter() {
        ATPrepCmd(mcx, &mut tab, &rel, cnode, recurse, query_string)?;
    }
    rel.close(NoLock)?;

    ATRewriteCatalogs(mcx, &mut tab, lockmode, query_string)?;
    ATRewriteTables(mcx, &mut tab, lockmode)
}

// ATPrepCmd: the statement arena is single-use, so the subcommand is
// scribbled on in place instead of C's copyObject.
fn ATPrepCmd<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    recurse: bool,
    query_string: &str,
) -> PgResult<()> {
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    // ATSimplePermissions relkind gate; ownership was checked at lookup.
    if rel.rd_rel.relkind != RELKIND_RELATION {
        unported("ATSimplePermissions: non-plain-table relkind");
    }
    let set_recurse = || {
        if recurse {
            // SAFETY: parse tree is statement-owned; no derived refs live.
            unsafe {
                cnode.with_mut::<AlterTableCmd, _>(|c| c.recurse = true).expect("AlterTableCmd");
            }
        }
    };
    let pass = match cmd.subtype {
        AlterTableType::AT_AddColumn => {
            // ATPrepAddColumn: typed-table/composite arms unreachable.
            set_recurse();
            AT_PASS_ADD_COL
        }
        AlterTableType::AT_DropColumn => {
            set_recurse();
            AT_PASS_DROP
        }
        // ATSimpleRecursion: children are loud at exec (no inheritance).
        AlterTableType::AT_ColumnDefault => {
            if cmd.def.is_some() { AT_PASS_ADD_OTHERCONSTR } else { AT_PASS_DROP }
        }
        AlterTableType::AT_DropNotNull => AT_PASS_DROP,
        AlterTableType::AT_SetNotNull => {
            set_recurse();
            AT_PASS_COL_ATTRS
        }
        AlterTableType::AT_AddConstraint => {
            set_recurse();
            ATPrepAddPrimaryKey(mcx, tab, rel, cmd, recurse)?;
            AT_PASS_ADD_CONSTR
        }
        AlterTableType::AT_DropConstraint => {
            set_recurse();
            AT_PASS_DROP
        }
        AlterTableType::AT_AlterColumnType => {
            ATPrepAlterColumnType(mcx, tab, rel, cmd, query_string)?;
            AT_PASS_ALTER_TYPE
        }
        // ATSimpleRecursion: children are loud at exec (no inheritance).
        AlterTableType::AT_SetExpression => AT_PASS_SET_EXPRESSION,
        AlterTableType::AT_DropExpression => {
            ATPrepDropExpression(mcx, rel, cmd, recurse)?;
            AT_PASS_DROP
        }
        AlterTableType::AT_CookedColumnDefault => AT_PASS_ADD_OTHERCONSTR,
        AlterTableType::AT_EnableRule
        | AlterTableType::AT_EnableAlwaysRule
        | AlterTableType::AT_EnableReplicaRule
        | AlterTableType::AT_DisableRule => AT_PASS_MISC,
        AlterTableType::AT_EnableRowSecurity
        | AlterTableType::AT_DisableRowSecurity
        | AlterTableType::AT_ForceRowSecurity
        | AlterTableType::AT_NoForceRowSecurity => AT_PASS_MISC,
        AlterTableType::AT_SetStatistics => {
            set_recurse();
            AT_PASS_MISC
        }
        AlterTableType::AT_SetStorage => {
            set_recurse();
            AT_PASS_MISC
        }
        // These commands never recurse; no command-specific prep.
        AlterTableType::AT_ReplicaIdentity
        | AlterTableType::AT_AddOf
        | AlterTableType::AT_DropOf => AT_PASS_MISC,
        AlterTableType::AT_AddIndexConstraint => AT_PASS_ADD_INDEXCONSTR,
        AlterTableType::AT_SetTableSpace => {
            ATPrepSetTableSpace(mcx, tab, cmd.name.expect("SET TABLESPACE name"))?;
            AT_PASS_MISC
        }
        AlterTableType::AT_SetAccessMethod => {
            if tab.chg_access_method {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot have multiple SET ACCESS METHOD subcommands".to_string(),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            ATPrepSetAccessMethod(tab, rel, cmd.name)?;
            AT_PASS_MISC
        }
        AlterTableType::AT_GenericOptions => {
            // ATSimplePermissions(ATT_FOREIGN_TABLE): foreign tables cannot
            // exist yet, so only the relkind error is reachable.
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "ALTER action OPTIONS cannot be performed on relation \"{}\"",
                        rel.name()
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail("This operation is not supported for tables.".to_string()),
            ));
        }
        other => unported(&format!("ATPrepCmd {other:?}")),
    };
    tab.subcmds[pass].lappend(mcx, cnode)?;
    Ok(())
}

fn ATRewriteCatalogs<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    _lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    for pass in 0..AT_NUM_PASSES {
        if tab.subcmds[pass].is_nil() {
            continue;
        }
        let mut nodes: mcx::PgVec<'_, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        for c in tab.subcmds[pass].iter() {
            nodes.push(c);
        }
        for &cnode in nodes.iter() {
            let rel = table::table_open(mcx, tab.relid, NoLock)?;
            let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
            match cmd.subtype {
                AlterTableType::AT_AddColumn => {
                    ATExecAddColumn(mcx, tab, &rel, cnode, query_string)?;
                }
                AlterTableType::AT_DropColumn => {
                    ATExecDropColumn(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_ColumnDefault => {
                    ATExecColumnDefault(mcx, &rel, cmd, query_string)?;
                }
                AlterTableType::AT_DropNotNull => {
                    ATExecDropNotNull(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_SetNotNull => {
                    ATExecSetNotNull(mcx, tab, &rel, cmd)?;
                }
                AlterTableType::AT_CookedColumnDefault => {
                    let defnode = cmd.def.expect("AT_CookedColumnDefault expr");
                    pg_attrdef::StoreAttrDefault(mcx, &rel, cmd.num, defnode)?;
                }
                AlterTableType::AT_AddConstraint => {
                    // ATParseTransformCmd: PK/UNIQUE constraints become an
                    // AT_AddIndex IndexStmt scheduled for AT_PASS_ADD_INDEX.
                    let defnode = cmd.def.expect("AT_AddConstraint Constraint");
                    let constr = defnode.as_variant::<Constraint>().expect("Constraint");
                    match constr.contype {
                        ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE => {
                            let (istmt, nnconstraints) =
                                parse_utilcmd::transformIndexConstraintForAlter(
                                    mcx, &rel, defnode,
                                )?;
                            let is_existing = istmt
                                .as_variant::<types_nodes::rawnodes::IndexStmt>()
                                .expect("IndexStmt")
                                .indexOid
                                != InvalidOid;
                            if !is_existing {
                                parse_clause::transformIndexStmt(
                                    mcx,
                                    tab.relid,
                                    istmt,
                                    query_string,
                                )?;
                            }
                            // C's transformAlterTableStmt: PK USING INDEX
                            // not-null constraints run in COL_ATTRS, before
                            // the ADD_INDEXCONSTR pass checks them.
                            for nn in nnconstraints.iter() {
                                let mut nncmd = Node::build::<AlterTableCmd>(mcx)?;
                                nncmd.subtype = AlterTableType::AT_AddConstraint;
                                nncmd.recurse = true;
                                nncmd.def = Some(nn);
                                tab.subcmds[AT_PASS_COL_ATTRS]
                                    .lappend(mcx, nncmd.seal())?;
                            }
                            let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
                            newcmd.subtype = if is_existing {
                                AlterTableType::AT_AddIndexConstraint
                            } else {
                                AlterTableType::AT_AddIndex
                            };
                            newcmd.def = Some(istmt);
                            let target_pass = if is_existing {
                                AT_PASS_ADD_INDEXCONSTR
                            } else {
                                AT_PASS_ADD_INDEX
                            };
                            tab.subcmds[target_pass].lappend(mcx, newcmd.seal())?;
                        }
                        ConstrType::CONSTR_NOTNULL if pass == AT_PASS_ADD_CONSTR => {
                            tab.subcmds[AT_PASS_COL_ATTRS].lappend(mcx, cnode)?;
                        }
                        ConstrType::CONSTR_NOTNULL => {
                            ATExecAddNotNullConstraint(mcx, tab, &rel, constr)?;
                        }
                        _ => ATExecAddConstraint(mcx, tab, &rel, cmd, query_string)?,
                    }
                }
                AlterTableType::AT_DropConstraint => {
                    ATExecDropConstraint(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_AddIndex => {
                    ATExecAddIndex(mcx, tab, &rel, cmd)?;
                }
                AlterTableType::AT_AlterColumnType => {
                    ATExecAlterColumnType(mcx, tab, &rel, cmd)?;
                }
                AlterTableType::AT_SetExpression => {
                    ATExecSetExpression(mcx, tab, &rel, cmd, query_string)?;
                }
                AlterTableType::AT_DropExpression => {
                    ATExecDropExpression(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_EnableRule => {
                    rewrite_define::EnableDisableRule(
                        mcx,
                        &rel,
                        cmd.name.expect("ENABLE RULE has a name"),
                        b'O',
                    )?;
                }
                AlterTableType::AT_EnableAlwaysRule => {
                    rewrite_define::EnableDisableRule(
                        mcx,
                        &rel,
                        cmd.name.expect("ENABLE ALWAYS RULE has a name"),
                        b'A',
                    )?;
                }
                AlterTableType::AT_EnableReplicaRule => {
                    rewrite_define::EnableDisableRule(
                        mcx,
                        &rel,
                        cmd.name.expect("ENABLE REPLICA RULE has a name"),
                        b'R',
                    )?;
                }
                AlterTableType::AT_DisableRule => {
                    rewrite_define::EnableDisableRule(
                        mcx,
                        &rel,
                        cmd.name.expect("DISABLE RULE has a name"),
                        b'D',
                    )?;
                }
                AlterTableType::AT_EnableRowSecurity => {
                    ATExecSetRowSecurity(mcx, &rel, true)?;
                }
                AlterTableType::AT_DisableRowSecurity => {
                    ATExecSetRowSecurity(mcx, &rel, false)?;
                }
                AlterTableType::AT_ForceRowSecurity => {
                    ATExecForceNoForceRowSecurity(mcx, &rel, true)?;
                }
                AlterTableType::AT_NoForceRowSecurity => {
                    ATExecForceNoForceRowSecurity(mcx, &rel, false)?;
                }
                AlterTableType::AT_SetStatistics => {
                    ATExecSetStatistics(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_SetStorage => {
                    ATExecSetStorage(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_AddIndexConstraint => {
                    let stmt = cmd
                        .def
                        .expect("AT_AddIndexConstraint IndexStmt")
                        .as_variant::<types_nodes::rawnodes::IndexStmt>()
                        .expect("IndexStmt");
                    ATExecAddIndexConstraint(mcx, &rel, stmt)?;
                }
                AlterTableType::AT_ReplicaIdentity => {
                    let stmt = cmd
                        .def
                        .expect("AT_ReplicaIdentity ReplicaIdentityStmt")
                        .as_variant::<types_nodes::parsenodes::ReplicaIdentityStmt>()
                        .expect("ReplicaIdentityStmt");
                    ATExecReplicaIdentity(mcx, &rel, stmt)?;
                }
                AlterTableType::AT_AddOf => {
                    let tn = cmd
                        .def
                        .expect("AT_AddOf TypeName")
                        .as_variant::<TypeName>()
                        .expect("TypeName");
                    ATExecAddOf(mcx, &rel, tn)?;
                }
                AlterTableType::AT_DropOf => {
                    ATExecDropOf(mcx, &rel)?;
                }
                // Phase-2 arms only fire for partitioned relkinds (no
                // storage), which are unreachable here; phase 3 does the work.
                AlterTableType::AT_SetTableSpace | AlterTableType::AT_SetAccessMethod => {}
                other => unported(&format!("ATExecCmd {other:?}")),
            }
            rel.close(NoLock)?;
            xact::CommandCounterIncrement()?;
        }
        // ATPostAlterTypeCleanup: dependent constraints/indexes are loud in
        // ATExecAlterColumnType, so the re-add queue is always empty.
    }
    // AlterTableCreateToastTable: a no-op when a toast table already exists
    // or none is needed.
    catalog_toasting::NewRelationCreateToastTable(mcx, tab.relid)
}

fn ATRewriteTables<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // find_composite_type_dependencies: composite-type columns are unported,
    // so no dependent rowtype uses can exist.
    if tab.rewrite > 0 {
        if tab.rewrite & AT_REWRITE_ACCESS_METHOD != 0 {
            unported("ATRewriteTable rewrite (SET ACCESS METHOD; only heap exists)");
        }
        if tab.rewrite & !(AT_REWRITE_COLUMN_REWRITE | AT_REWRITE_DEFAULT_VAL) != 0 {
            unported("ATRewriteTable rewrite flags (persistence)");
        }
        if tab.new_tablespace != InvalidOid {
            unported("ATRewriteTable rewrite combined with SET TABLESPACE");
        }
        let old_heap = table::table_open(mcx, tab.relid, NoLock)?;
        let persistence = old_heap.rd_rel.relpersistence;
        old_heap.close(NoLock)?;
        let oid_new_heap =
            commands_cluster::make_new_heap(mcx, tab.relid, persistence, lockmode)?;
        ATRewriteTable(mcx, tab, oid_new_heap)?;
        commands_cluster::finish_heap_swap(
            mcx,
            tab.relid,
            oid_new_heap,
            false,
            false,
            true,
            true,
            procarray::RecentXmin(),
            multixact::ReadNextMultiXactId()?,
            persistence,
        )?;
    } else {
        if !tab.constraints.is_empty() || tab.verify_new_notnull {
            ATRewriteTable(mcx, tab, InvalidOid)?;
        }
        if tab.new_tablespace != InvalidOid {
            ATExecSetTableSpace(mcx, tab.relid, tab.new_tablespace, lockmode)?;
        }
    }
    let _ = tab.has_newvals;

    // C's final pass: FK constraints are checked after all rewrites.
    if !tab.fk_checks.is_empty() {
        let rel = table::table_open(mcx, tab.relid, NoLock)?;
        for item in tab.fk_checks.iter() {
            crate::fk::validate_foreign_key_constraint(mcx, &rel, item)?;
        }
        rel.close(NoLock)?;
    }
    Ok(())
}

// ATRewriteTable: scan (verify) or rewrite one table.
fn ATRewriteTable<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    oid_new_heap: Oid,
) -> PgResult<()> {
    let oldrel = table::table_open(mcx, tab.relid, NoLock)?;
    let new_tupdesc = &oldrel.rd_att;
    let newrel = if oid_new_heap != InvalidOid {
        Some(table::table_open(mcx, oid_new_heap, NoLock)?)
    } else {
        None
    };

    let mut needscan = false;
    let mut con_states: PgVec<'mcx, (usize, mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>)> =
        PgVec::new_in(mcx);
    for (i, con) in tab.constraints.iter().enumerate() {
        needscan = true;
        // ExecPrepareExpr: expression_planner + init.
        let planned = clauses::eval_const_expressions(mcx, con.qual)?;
        let state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("check constraint expr");
        con_states.push((i, state));
    }
    let mut newval_states: PgVec<
        'mcx,
        (AttrNumber, bool, mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>),
    > = PgVec::new_in(mcx);
    for nv in tab.newvals.iter() {
        let state = execexpr::exec_init_expr(mcx, Some(nv.expr), execexpr::ParamBind::NONE)?
            .expect("transform expr");
        newval_states.push((nv.attnum, nv.is_generated, state));
    }

    let mut notnull_attrs: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);
    if newrel.is_some() || tab.verify_new_notnull {
        for i in 0..new_tupdesc.natts as usize {
            let att = new_tupdesc.attr(i);
            if att.attnotnull && !att.attisdropped {
                if att.attgenerated == b'v' as i8 {
                    unported("ATRewriteTable notnull_virtual_attrs (ExecRelGenVirtualNotNull)");
                }
                notnull_attrs.push(att.attnum);
            }
        }
        if !notnull_attrs.is_empty() {
            needscan = true;
        }
    }

    if newrel.is_some() || needscan {
        let relname = oldrel.name().to_string();
        let mut oldslot = if tab.rewrite > 0 {
            exectuples::make_tuple_table_slot(
                mcx,
                tableam::table_slot_callbacks(&oldrel),
                Some(tab.old_desc.clone()),
            )
        } else {
            tableam::table_slot_create(mcx, &oldrel)?
        };
        let mut newslot = match &newrel {
            Some(nr) => Some(tableam::table_slot_create(mcx, nr)?),
            None => None,
        };
        let mut dropped_attrs: PgVec<'mcx, usize> = PgVec::new_in(mcx);
        for i in 0..new_tupdesc.natts as usize {
            if new_tupdesc.attr(i).attisdropped {
                dropped_attrs.push(i);
            }
        }
        let (mycid, ti_options) = if newrel.is_some() {
            (xact::GetCurrentCommandId(true)?, tableam_vocab::TABLE_INSERT_SKIP_FSM)
        } else {
            (0, 0)
        };
        let snapshot = snapmgr::GetLatestSnapshot()?;
        let snapshot = snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered snapshot");
        let mut scan =
            tableam::table_beginscan(mcx, &oldrel, Some(snapshot.clone()), 0, PgVec::new_in(mcx))?;
        while tableam::table_scan_getnextslot(
            mcx,
            &mut scan,
            types_scan::ScanDirection::ForwardScanDirection,
            &mut oldslot,
        )? {
            let insertslot: &mut types_slot::SlotData<'mcx>;
            if tab.rewrite > 0 {
                let ns = newslot.as_mut().expect("rewrite has newslot");
                exectuples::slot_getallattrs(&mut oldslot);
                exectuples::exec_clear_tuple(ns, mcx);
                {
                    let ob = oldslot.base_mut();
                    let nvalid = ob.tts_values.len();
                    let natts = new_tupdesc.natts as usize;
                    let nsb = ns.base_mut();
                    nsb.tts_values.clear();
                    nsb.tts_isnull.clear();
                    for i in 0..natts {
                        if i < nvalid {
                            nsb.tts_values.push(ob.tts_values[i]);
                            nsb.tts_isnull.push(ob.tts_isnull[i]);
                        } else {
                            nsb.tts_values.push(Datum::null());
                            nsb.tts_isnull.push(true);
                        }
                    }
                    for &i in dropped_attrs.iter() {
                        nsb.tts_isnull[i] = true;
                    }
                }
                for (attnum, is_generated, state) in newval_states.iter_mut() {
                    if *is_generated {
                        continue;
                    }
                    let mut slots = execexpr::EvalSlots {
                        scan: Some(&mut oldslot),
                        inner: None,
                        outer: None,
                    };
                    let r = execexpr::exec_eval_expr(state, &mut slots)?;
                    let nsb = ns.base_mut();
                    nsb.tts_values[*attnum as usize - 1] = r.value;
                    nsb.tts_isnull[*attnum as usize - 1] = r.isnull;
                }
                exectuples::exec_store_virtual_tuple(ns);
                ns.base_mut().tts_tableOid = oldrel.rd_id;
                // Generated expressions read the NEW tuple (assumed not to
                // reference each other, as in C).
                for (attnum, is_generated, state) in newval_states.iter_mut() {
                    if !*is_generated {
                        continue;
                    }
                    let r = {
                        let mut slots = execexpr::EvalSlots {
                            scan: Some(&mut *ns),
                            inner: None,
                            outer: None,
                        };
                        execexpr::exec_eval_expr(state, &mut slots)?
                    };
                    let nsb = ns.base_mut();
                    nsb.tts_values[*attnum as usize - 1] = r.value;
                    nsb.tts_isnull[*attnum as usize - 1] = r.isnull;
                }
                insertslot = ns;
            } else {
                insertslot = &mut oldslot;
            }

            for &attn in notnull_attrs.iter() {
                if exectuples::slot_attisnull(insertslot, attn as i32) {
                    let att = new_tupdesc.attr(attn as usize - 1);
                    let colname =
                        core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "column \"{colname}\" of relation \"{relname}\" contains \
                                 null values"
                            ),
                        )
                        .with_sqlstate(ERRCODE_NOT_NULL_VIOLATION),
                    ));
                }
            }
            for (i, state) in con_states.iter_mut() {
                let mut slots = execexpr::EvalSlots {
                    scan: Some(insertslot),
                    inner: None,
                    outer: None,
                };
                let r = execexpr::exec_eval_expr(state, &mut slots)?;
                if !r.isnull && !r.value.as_bool() {
                    let conname = tab.constraints[*i].name;
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "check constraint \"{conname}\" of relation \"{relname}\" \
                                 is violated by some row"
                            ),
                        )
                        .with_sqlstate(ERRCODE_CHECK_VIOLATION),
                    ));
                }
            }

            if let Some(nr) = &newrel {
                exectuples::exec_materialize_slot(insertslot, mcx)?;
                // C threads a BulkInsertState; the heap AM only wires bistate
                // through multi_insert — ring-buffer strategy only, same WAL.
                tableam::table_tuple_insert(mcx, nr, insertslot, mycid, ti_options, None)?;
            }
        }
        tableam::table_endscan(scan)?;
        snapmgr::UnregisterSnapshot(Some(&snapshot));
        if let Some(nr) = &newrel {
            tableam::table_finish_bulk_insert(nr, ti_options)?;
        }
    }

    oldrel.close(NoLock)?;
    if let Some(nr) = newrel {
        nr.close(NoLock)?;
    }
    Ok(())
}

fn ATExecAddColumn<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let myrelid = rel.rd_id;
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    let if_not_exists = cmd.missing_ok;
    let defnode = cmd.def.expect("AT_AddColumn ColumnDef");
    let col_def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
    debug_assert!(col_def.inhcount == 0);
    let colname = col_def.colname.expect("ColumnDef.colname");
    let relname = rel.name().to_string();

    if !check_for_column_name_collision(mcx, myrelid, &relname, colname, if_not_exists)? {
        return Ok(());
    }

    parse_utilcmd::transformAlterTableCmd(mcx, &relname, cnode)?;
    let col_def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");

    let pgclass = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(1, myrelid);
    let mut scan =
        genam::systable_beginscan(mcx, &pgclass, catalog::ClassOidIndexId, true, None, &[key])?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {myrelid}"));
    let cdesc = pgclass.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_class column under pg_class's descriptor.
    let relnatts = unsafe {
        types_tuple::heap_getattr(reltup, Anum_pg_class_relnatts as i32, cdesc, &mut isnull)
    }
    .as_i16();
    let newattnum = relnatts as i32 + 1;
    if newattnum > MaxHeapAttributeNumber {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("tables can have at most {MaxHeapAttributeNumber} columns"),
            )
            .with_sqlstate(ERRCODE_TOO_MANY_COLUMNS),
        ));
    }

    let elts = NodeList::make1(mcx, defnode)?;
    let mut tupdesc = crate::BuildDescForRelation(mcx, &elts)?;
    tupdesc.attr_mut(0).attnum = newattnum as AttrNumber;
    // CheckAttributeType surrogate; the name half re-proves what the
    // collision check established.
    catalog_heap::CheckAttributeNamesTypes(&tupdesc, RELKIND_RELATION)?;
    let attribute = tupdesc.attrs[0];

    let attrdesc = table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &attrdesc)?;
    catalog_heap::insert_pg_attribute_tuple(mcx, &attrdesc, &attribute, myrelid, &mut indstate)?;
    catalog_indexing::CatalogCloseIndexes(indstate)?;
    attrdesc.close(RowExclusiveLock)?;

    let natts = cdesc.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_class_relnatts - 1] = Datum::from_i16(newattnum as i16);
    repl[Anum_pg_class_relnatts - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, reltup, cdesc, &repl_values, &repl_isnull, &repl)?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pgclass, &otid, &mut newtup)?;
    pgclass.close(RowExclusiveLock)?;

    xact::CommandCounterIncrement()?;

    if let Some(raw_default) = col_def.raw_default {
        // AddRelationNewConstraints over the one RawColumnDefault; the rel
        // must be re-opened to see the new attribute (C rebuilds in place).
        let rel2 = table::table_open(mcx, myrelid, NoLock)?;
        crate::constraints::add_relation_new_constraints(
            mcx,
            &rel2,
            &[(newattnum as AttrNumber, raw_default, col_def.generated)],
            &NodeList::nil(),
            query_string,
        )?;
        rel2.close(NoLock)?;
        xact::CommandCounterIncrement()?;
    }

    // Phase-3 fill / attmissingval fast path. Domain and generated columns
    // are unreachable (loud at parse), so defval exists iff atthasdef.
    let mut has_missing = false;
    let rel3 = table::table_open(mcx, myrelid, NoLock)?;
    if rel3.rd_att.attr(newattnum as usize - 1).atthasdef {
        let defval = rewrite_handler::build_column_default(mcx, &rel3, newattnum as usize)?;
        let defval = clauses::eval_const_expressions(mcx, defval)?;
        tab.has_newvals = true;
        if !clauses::contain_volatile_functions(defval)? {
            let mut state = execexpr::exec_init_expr(mcx, Some(defval), execexpr::ParamBind::NONE)?
                .expect("non-nil default expression");
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let r = execexpr::exec_eval_expr(&mut state, &mut slots)?;
            if !r.isnull {
                catalog_heap::StoreAttrMissingVal(mcx, &rel3, newattnum as AttrNumber, r.value)?;
                xact::CommandCounterIncrement()?;
                has_missing = true;
            }
        } else {
            unported("ATExecAddColumn volatile default (phase-3 rewrite fill)");
        }
    }
    if !has_missing {
        tab.verify_new_notnull |= col_def.is_not_null;
    }

    let myself =
        pg_depend::ObjectAddress::sub_set(RELATION_RELATION_ID, myrelid, newattnum);
    let referenced = pg_depend::ObjectAddress::set(TYPE_RELATION_ID, attribute.atttypid);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, pg_depend::DependencyType::Normal)?;
    if attribute.attcollation != InvalidOid && attribute.attcollation != DEFAULT_COLLATION_OID {
        let referenced = pg_depend::ObjectAddress::set(CollationRelationId, attribute.attcollation);
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &referenced,
            pg_depend::DependencyType::Normal,
        )?;
    }
    rel3.close(NoLock)?;

    if find_inheritance_children_exist(mcx, myrelid)? {
        unported("ATExecAddColumn inheritance recursion");
    }
    Ok(())
}

// ATExecAddConstraint -> ATAddCheckNNConstraint, CHECK-only-cooked slice
// (recursion moot: no inheritance children on ported lanes).
// check_for_column_name_collision: deliberately not attisdropped-aware.
pub(crate) fn check_for_column_name_collision<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relname: &str,
    colname: &str,
    if_not_exists: bool,
) -> PgResult<bool> {
    let Some((attnum, _)) = attname_lookup(mcx, relid, colname, true)? else {
        return Ok(true);
    };
    if attnum <= 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column name \"{colname}\" conflicts with a system column name"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
        ));
    }
    if if_not_exists {
        elog_seams::ereport::call(
            PgError::new(
                NOTICE,
                format!("column \"{colname}\" of relation \"{relname}\" already exists, skipping"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
        )?;
        return Ok(false);
    }
    Err(Box::new(
        PgError::new(
            ERROR,
            format!("column \"{colname}\" of relation \"{relname}\" already exists"),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
    ))
}

// SearchSysCache(ATTNAME) surrogate: pg_attribute scan filtered by name.
// include_dropped mirrors SearchSysCache2 (collision check) vs
// SearchSysCacheAttName (skips dropped).
pub(crate) fn attname_lookup<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    colname: &str,
    include_dropped: bool,
) -> PgResult<Option<(i16, i16)>> {
    let attrel = table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_scankey(1, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &attrel, AttributeRelidNumIndexId, true, None, &[key])?;
    let desc = attrel.descr();
    let mut found: Option<(i16, i16)> = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_attribute columns under its descriptor.
        let name = unsafe { types_tuple::heap_getattr(tup, 2, desc, &mut isnull) };
        let name = unsafe {
            core::slice::from_raw_parts(name.as_usize() as *const u8, 64)
        };
        let len = name.iter().position(|&b| b == 0).unwrap_or(64);
        if &name[..len] != colname.as_bytes() {
            continue;
        }
        let dropped =
            unsafe { types_tuple::heap_getattr(tup, 17, desc, &mut isnull) }.as_bool();
        if dropped && !include_dropped {
            continue;
        }
        let attnum = unsafe { types_tuple::heap_getattr(tup, 5, desc, &mut isnull) }.as_i16();
        let inhcount =
            unsafe { types_tuple::heap_getattr(tup, 19, desc, &mut isnull) }.as_i16();
        found = Some((attnum, inhcount));
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    attrel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

fn ATExecDropColumn<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_DropColumn name");
    let relname = rel.name().to_string();

    let Some((attnum, attinhcount)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        if !cmd.missing_ok {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "column \"{col_name}\" of relation \"{relname}\" does not exist"
                    ),
                )
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
            ));
        }
        elog_seams::ereport_msg::call(
            NOTICE,
            format!("column \"{col_name}\" of relation \"{relname}\" does not exist, skipping"),
            None,
        )?;
        return Ok(());
    };

    if attnum <= 0 {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot drop system column \"{col_name}\""))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if attinhcount > 0 {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot drop inherited column \"{col_name}\""))
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    // has_partition_attrs: false for plain relations (partitioning unported).

    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecDropColumn inheritance recursion");
    }

    let mut addrs = catalog_dependency::ObjectAddresses::new();
    addrs.add_exact_object_address(pg_depend::ObjectAddress::sub_set(
        RELATION_RELATION_ID,
        rel.rd_id,
        attnum as i32,
    ));
    catalog_dependency::performMultipleDeletions(
        mcx,
        &addrs,
        cmd.behavior,
        0,
    )
}

// ATExecColumnDefault (SET/DROP DEFAULT).
fn ATExecColumnDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_ColumnDefault name");
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecColumnDefault inheritance recursion");
    }
    let att = rel.rd_att.attr(attnum as usize - 1);
    if att.attidentity != 0 {
        unported("ATExecColumnDefault on an identity column (C 42601 + DROP IDENTITY hint)");
    }
    if att.attgenerated != 0 {
        let e = PgError::new(
            ERROR,
            format!("column \"{col_name}\" of relation \"{relname}\" is a generated column"),
        )
        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR);
        let e = if cmd.def.is_some() {
            e.with_hint(
                "Use ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION instead.".to_string(),
            )
        } else if att.attgenerated == b's' as i8 {
            e.with_hint(
                "Use ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION instead.".to_string(),
            )
        } else {
            e
        };
        return Err(Box::new(e));
    }
    RemoveAttrDefault(mcx, rel.rd_id, attnum, false, cmd.def.is_some())?;
    if let Some(def) = cmd.def {
        crate::constraints::add_relation_new_constraints(
            mcx,
            rel,
            &[(attnum, def, 0)],
            &NodeList::nil(),
            query_string,
        )?;
    }
    Ok(())
}

// RemoveAttrDefault (pg_attrdef.c): lookup rides pg_attrdef, the deletion
// rides catalog_dependency (a direct pg_attrdef -> dependency edge cycles).
fn RemoveAttrDefault<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    complain: bool,
    internal: bool,
) -> PgResult<()> {
    let attrdef_id = pg_attrdef::GetAttrDefaultOid(mcx, relid, attnum)?;
    if attrdef_id == InvalidOid {
        if complain {
            panic!("could not find attrdef tuple for relation {relid} attnum {attnum}");
        }
        return Ok(());
    }
    let object = pg_depend::ObjectAddress::set(types_core::ATTR_DEFAULT_RELATION_ID, attrdef_id);
    catalog_dependency::performDeletion(
        mcx,
        &object,
        types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
        if internal { catalog_dependency::PERFORM_DELETION_INTERNAL } else { 0 },
    )
}

// ATExecSetExpression (tablecmds.c).
fn ATExecSetExpression<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetExpression name");
    let relname = rel.name().to_string();
    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecSetExpression inheritance recursion");
    }
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let att = rel.rd_att.attr(attnum as usize - 1);
    let attgenerated = att.attgenerated;
    if attgenerated == 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "column \"{col_name}\" of relation \"{relname}\" is not a generated column"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    if attgenerated == b'v' as i8
        && rel.rd_att.constr.as_deref().map(|c| c.num_check).unwrap_or(0) > 0
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated \
                 columns in tables with check constraints"
                    .to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(format!(
                "Column \"{col_name}\" of relation \"{relname}\" is a virtual generated \
                 column."
            )),
        ));
    }
    if attgenerated == b'v' as i8 && att.attnotnull {
        tab.verify_new_notnull = true;
    }
    // DIVERGENCE: C rejects virtual columns when GetRelationPublications is
    // non-empty (0A000 "... part of a publication"); publications unported.
    let rewrite = attgenerated == b's' as i8;

    if rewrite {
        catalog_heap::RelationClearMissing(mcx, rel.rd_id)?;
        xact::CommandCounterIncrement()?;
        remember_dependents_or_loud(mcx, rel, attnum, col_name, false)?;
    }

    let attrdefoid = pg_attrdef::GetAttrDefaultOid(mcx, rel.rd_id, attnum)?;
    if attrdefoid == InvalidOid {
        panic!(
            "could not find attrdef tuple for relation {} attnum {attnum}",
            rel.rd_id
        );
    }
    pg_depend::deleteDependencyRecordsFor(
        mcx,
        types_core::ATTR_DEFAULT_RELATION_ID,
        attrdefoid,
        false,
    )?;
    xact::CommandCounterIncrement()?;
    RemoveAttrDefault(mcx, rel.rd_id, attnum, false, false)?;

    let newexpr = cmd.def.expect("AT_SetExpression expression");
    crate::constraints::add_relation_new_constraints(
        mcx,
        rel,
        &[(attnum, newexpr, attgenerated as u8)],
        &NodeList::nil(),
        query_string,
    )?;
    xact::CommandCounterIncrement()?;

    if rewrite {
        let rel2 = table::table_open(mcx, rel.rd_id, NoLock)?;
        let defval = rewrite_handler::build_column_default(mcx, &rel2, attnum as usize)?;
        let defval = clauses::eval_const_expressions(mcx, defval)?;
        rel2.close(NoLock)?;
        tab.newvals.push(NewColumnValue { attnum, expr: defval, is_generated: true });
        tab.rewrite |= AT_REWRITE_DEFAULT_VAL;
    }

    catalog_heap::RemoveStatistics(mcx, rel.rd_id, attnum)?;
    Ok(())
}

// ATPrepDropExpression (tablecmds.c).
fn ATPrepDropExpression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    recurse: bool,
) -> PgResult<()> {
    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        if !recurse {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "ALTER TABLE / DROP EXPRESSION must be applied to child tables too"
                        .to_string(),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        unported("ATPrepDropExpression recursion");
    }
    let col_name = cmd.name.expect("AT_DropExpression name");
    let Some((_, attinhcount)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &rel.name().to_string()));
    };
    if attinhcount > 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot drop generation expression from inherited column".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    Ok(())
}

// ATExecDropExpression (tablecmds.c).
fn ATExecDropExpression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_DropExpression name");
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let attgenerated = rel.rd_att.attr(attnum as usize - 1).attgenerated;
    // C errors on 'v' even with missing_ok, so the column is never silently
    // left generated.
    if attgenerated == b'v' as i8 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated \
                 columns"
                    .to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(format!(
                "Column \"{col_name}\" of relation \"{relname}\" is a virtual generated \
                 column."
            )),
        ));
    }
    if attgenerated == 0 {
        if !cmd.missing_ok {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "column \"{col_name}\" of relation \"{relname}\" is not a \
                         generated column"
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        elog_seams::ereport_msg::call(
            NOTICE,
            format!(
                "column \"{col_name}\" of relation \"{relname}\" is not a generated \
                 column, skipping"
            ),
            None,
        )?;
        return Ok(());
    }

    // atthasdef clears via RemoveAttrDefault below, as in C.
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[(Anum_pg_attribute_attgenerated, Datum::from_i8(0))],
    )?;

    let attrdefoid = pg_attrdef::GetAttrDefaultOid(mcx, rel.rd_id, attnum)?;
    if attrdefoid == InvalidOid {
        panic!(
            "could not find attrdef tuple for relation {} attnum {attnum}",
            rel.rd_id
        );
    }
    pg_depend::deleteDependencyRecordsFor(
        mcx,
        types_core::ATTR_DEFAULT_RELATION_ID,
        attrdefoid,
        false,
    )?;
    xact::CommandCounterIncrement()?;
    RemoveAttrDefault(mcx, rel.rd_id, attnum, false, false)
}

// ATExecDropNotNull + the reachable dropconstraint_internal slice.
fn ATExecDropNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_DropNotNull name");
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum > 0 && !rel.rd_att.attr(attnum as usize - 1).attnotnull {
        return Ok(());
    }
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if rel.rd_att.attr(attnum as usize - 1).attidentity != 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column \"{col_name}\" of relation \"{relname}\" is an identity column"),
            )
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
        ));
    }
    if rel.rd_rel.relispartition {
        unported("ATExecDropNotNull on a partition");
    }
    let con = pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?
        .unwrap_or_else(|| {
            panic!(
                "cache lookup failed for not-null constraint on column \"{col_name}\" of \
                 relation \"{relname}\""
            )
        });

    let shape = pg_constraint::ConShape {
        oid: con.oid,
        contype: pg_constraint::CONSTRAINT_NOTNULL,
        conname: con.conname,
        coninhcount: con.coninhcount,
        connoinherit: con.connoinherit,
        conislocal: con.conislocal,
        convalidated: con.convalidated,
        conindid: InvalidOid,
        confrelid: InvalidOid,
        notnull_attnum: attnum,
    };
    dropconstraint_internal(
        mcx,
        rel,
        &shape,
        types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
    )
}

// dropconstraint_internal's PK / replica-identity guards over pg_index.
fn check_notnull_droppable<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    col_name: &str,
) -> PgResult<()> {
    // C reads the rd_pkindex/rd_replidindex bitmaps (key columns only), so the
    // guards see only what RelationGetIndexList validated (indisvalid etc.).
    relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    let (pkindex, replidindex) = {
        let cached = rel.rd_indexlist.borrow();
        let l = cached.as_ref().expect("rd_indexlist populated by RelationGetIndexList");
        (l.pkindex, l.replidindex)
    };
    if pkindex != InvalidOid {
        let (_, _, keys) = pg_index_shape(mcx, pkindex)?;
        if keys.contains(&attnum) {
            return Err(Box::new(
                PgError::new(ERROR, format!("column \"{col_name}\" is in a primary key"))
                    .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
    }
    if replidindex != InvalidOid {
        let (_, _, keys) = pg_index_shape(mcx, replidindex)?;
        if keys.contains(&attnum) {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "column \"{col_name}\" is in index used as replica identity"
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
    }
    Ok(())
}

const IndexRelidIndexId: Oid = 2679;
const Anum_pg_index_indnkeyatts: usize = 4;
const Anum_pg_index_indisprimary: usize = 7;
const Anum_pg_index_indisreplident: usize = 15;
const Anum_pg_index_indkey: usize = 16;

fn pg_index_shape<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
) -> PgResult<(bool, bool, PgVec<'mcx, AttrNumber>)> {
    let (p, r, keys, nkeyatts) = pg_index_shape_full(mcx, indexoid)?;
    let mut prefix: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, nkeyatts)?;
    prefix.extend(keys.iter().take(nkeyatts).copied());
    Ok((p, r, prefix))
}

fn pg_index_shape_full<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
) -> PgResult<(bool, bool, PgVec<'mcx, AttrNumber>, usize)> {
    let pg_index = table::table_open(mcx, types_core::INDEX_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_scankey(1, indexoid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexoid}"));
    let desc = pg_index.descr();
    let mut isnull = false;
    let mut get = |attnum: usize| {
        // SAFETY: fixed NOT NULL pg_index columns under its descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
        assert!(!isnull, "unexpected null pg_index attnum {attnum} for index {indexoid}");
        d
    };
    let nkeyatts = get(Anum_pg_index_indnkeyatts).as_i16();
    let isprimary = get(Anum_pg_index_indisprimary).as_bool();
    let isreplident = get(Anum_pg_index_indisreplident).as_bool();
    let d = get(Anum_pg_index_indkey);
    let p = d.as_usize() as *const u8;
    // SAFETY: indkey is a NOT NULL int2vector (null-asserted above); live through the scan.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let elems = datum::array_build::deconstruct_array_image(mcx, image, 2, true, b's')?;
    let mut keys: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    keys.extend(elems.iter().map(|d| d.as_i16()));
    genam::systable_endscan(mcx, scan)?;
    pg_index.close(types_rel::AccessShareLock)?;
    Ok((isprimary, isreplident, keys, nkeyatts as usize))
}

// ATExecSetNotNull; recursion and merge arms are unreachable (no children).
fn ATExecSetNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetNotNull name");
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if let Some(con) = pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)? {
        if con.connoinherit && cmd.recurse {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on \
                         relation \"{relname}\"",
                        con.name_str()
                    ),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if !con.conislocal {
            unported("ATExecSetNotNull conislocal flip (inheritance)");
        }
        if !con.convalidated {
            unported("ATExecSetNotNull ATExecValidateConstraint (NOT VALID lane)");
        }
        return Ok(());
    }
    let is_no_inherit = if !cmd.recurse && find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecSetNotNull inheritance children");
    } else {
        false
    };
    create_notnull_constraint(mcx, tab, rel, attnum, col_name, None, is_no_inherit)
}

// The CreateConstraintEntry + set_attnotnull tail shared by SET NOT NULL and
// the ADD CONSTRAINT ... NOT NULL arm (ATAddCheckNNConstraint's NN half).
fn create_notnull_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    col_name: &str,
    conname: Option<&str>,
    is_no_inherit: bool,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    let name_storage;
    let con_name: &str = match conname {
        Some(n) => {
            if pg_constraint::ConstraintNameIsUsed(
                mcx,
                pg_constraint::ConstraintCategory::Relation,
                rel.rd_id,
                n,
            )? {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "constraint \"{n}\" for relation \"{relname}\" already exists"
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_DUPLICATE_OBJECT),
                ));
            }
            n
        }
        None => {
            name_storage = pg_constraint::ChooseConstraintName(
                mcx,
                &relname,
                Some(col_name),
                "not_null",
                rel.rd_rel.relnamespace,
                &[],
            )?;
            name_storage.as_str()
        }
    };
    let conkey = [attnum];
    let mut entry = pg_constraint::ConstraintEntry::base(
        con_name,
        rel.rd_rel.relnamespace,
        pg_constraint::CONSTRAINT_NOTNULL,
        rel.rd_id,
    );
    entry.conkey = &conkey;
    entry.n_keys = 1;
    entry.is_no_inherit = is_no_inherit;
    pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    // AddRelationNewConstraints tail: pg_class update fires the SI message
    // peers use to rebuild relcache entries.
    crate::constraints::set_relation_num_checks(
        mcx,
        rel,
        rel.rd_att.constr.as_deref().map(|c| c.num_check as i16).unwrap_or(0),
    )?;

    // set_attnotnull: NotNullImpliedByRelConstraints proof unported — phase 3
    // always verifies (C skips the scan when existing constraints imply it).
    if !rel.rd_att.attr(attnum as usize - 1).attnotnull {
        update_pg_attribute(
            mcx,
            rel.rd_id,
            attnum,
            &[(Anum_pg_attribute_attnotnull, Datum::from_bool(true))],
        )?;
        tab.verify_new_notnull = true;
        xact::CommandCounterIncrement()?;
    }
    Ok(())
}

// ATPrepAddPrimaryKey: queue an ADD CONSTRAINT NOT NULL subcommand into
// AT_PASS_ADD_CONSTR (C's inner ATPrepCmd) for every PK column lacking a
// compatible not-null constraint; exec reschedules it to AT_PASS_COL_ATTRS
// exactly where C's ATParseTransformCmd does, preserving within-pass order.
fn ATPrepAddPrimaryKey<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    recurse: bool,
) -> PgResult<()> {
    let Some(defnode) = cmd.def else { return Ok(()) };
    let Some(constr) = defnode.as_variant::<Constraint>() else { return Ok(()) };
    if constr.contype != ConstrType::CONSTR_PRIMARY {
        return Ok(());
    }
    for keynode in constr.keys.iter() {
        let key = keynode.as_string().expect("constraint keys").sval;
        let attnum = attname_lookup(mcx, rel.rd_id, key, false)?.map(|(a, _)| a).unwrap_or(0);
        if attnum > 0 {
            if let Some(con) =
                pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?
            {
                verify_notnull_pk_compatible(&con, key, rel.name())?;
                continue;
            }
        }
        if !recurse && find_inheritance_children_exist(mcx, rel.rd_id)? {
            unported("ATPrepAddPrimaryKey: ONLY with inheritance children");
        }
        let mut nn = Node::build::<Constraint>(mcx)?;
        nn.contype = ConstrType::CONSTR_NOTNULL;
        nn.keys = NodeList::make1(mcx, keynode)?;
        nn.is_enforced = true;
        nn.skip_validation = false;
        nn.initially_valid = true;
        nn.location = -1;
        let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
        newcmd.subtype = AlterTableType::AT_AddConstraint;
        newcmd.recurse = true;
        newcmd.def = Some(nn.seal());
        tab.subcmds[AT_PASS_ADD_CONSTR].lappend(mcx, newcmd.seal())?;
    }
    Ok(())
}

fn verify_notnull_pk_compatible(
    con: &pg_constraint::NotNullConTup,
    colname: &str,
    relname: &str,
) -> PgResult<()> {
    let characteristic = if con.connoinherit {
        Some(("NO INHERIT", "You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT."))
    } else if !con.convalidated {
        Some(("NOT VALID", "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT."))
    } else {
        None
    };
    if let Some((marked, hint)) = characteristic {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot create primary key on column \"{colname}\""),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_detail(format!(
                "The constraint \"{}\" on column \"{colname}\" of table \"{relname}\", \
                 marked {marked}, is incompatible with a primary key.",
                con.name_str()
            ))
            .with_hint(hint.to_string()),
        ));
    }
    Ok(())
}

// The NOT NULL arm of ATAddCheckNNConstraint (existing-constraint merge is an
// inheritance lane — loud).
fn ATExecAddNotNullConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    constr: &Constraint<'mcx>,
) -> PgResult<()> {
    let col_name = constr.keys.nth(0).as_string().expect("not-null keys").sval;
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column \"{col_name}\" of relation \"{relname}\" does not exist"),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
        ));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?.is_some() {
        unported("ATAddCheckNNConstraint: merge with an existing not-null constraint");
    }
    if constr.is_no_inherit && find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATAddCheckNNConstraint inheritance recursion");
    }
    create_notnull_constraint(
        mcx,
        tab,
        rel,
        attnum,
        col_name,
        constr.conname,
        constr.is_no_inherit,
    )?;
    xact::CommandCounterIncrement()?;
    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATAddCheckNNConstraint inheritance recursion");
    }
    Ok(())
}

// ATExecDropConstraint + dropconstraint_internal (plain-table slice; children
// recursion is loud).
fn ATExecDropConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let constr_name = cmd.name.expect("AT_DropConstraint name");
    let relname = rel.name().to_string();
    match pg_constraint::findConstraintByName(mcx, rel.rd_id, constr_name)? {
        Some(con) => dropconstraint_internal(mcx, rel, &con, cmd.behavior),
        None => {
            if !cmd.missing_ok {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "constraint \"{constr_name}\" of relation \"{relname}\" does \
                             not exist"
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "constraint \"{constr_name}\" of relation \"{relname}\" does not \
                     exist, skipping"
                ),
                None,
            )?;
            Ok(())
        }
    }
}

fn dropconstraint_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    con: &pg_constraint::ConShape,
    behavior: types_nodes::parsenodes::DropBehavior,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    if con.coninhcount > 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot drop inherited constraint \"{}\" of relation \"{relname}\"",
                    con.name_str()
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    if con.contype == pg_constraint::CONSTRAINT_NOTNULL {
        let attnum = con.notnull_attnum;
        let att = rel.rd_att.attr(attnum as usize - 1);
        let col_name =
            core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8").to_string();
        check_notnull_droppable(mcx, rel, attnum, &col_name)?;
        if att.attidentity != 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "column \"{col_name}\" of relation \"{relname}\" is an identity \
                         column"
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        if att.attnotnull {
            update_pg_attribute(
                mcx,
                rel.rd_id,
                attnum,
                &[(Anum_pg_attribute_attnotnull, Datum::from_bool(false))],
            )?;
        }
    }
    if con.contype == pg_constraint::CONSTRAINT_FOREIGN && con.confrelid != rel.rd_id {
        // Must match the lock RemoveTriggerById takes on the referenced rel.
        // C CheckAlterTableIsSafe = CheckTableNotInUse + RELATION_IS_OTHER_TEMP
        // (const-false single-backend).
        let frel = table::table_open(mcx, con.confrelid, AccessExclusiveLock)?;
        catalog_heap::CheckTableNotInUse(&frel, "ALTER TABLE")?;
        frel.close(NoLock)?;
    }
    let object = pg_depend::ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, con.oid);
    catalog_dependency::performDeletion(mcx, &object, behavior, 0)?;
    if !con.connoinherit && find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("dropconstraint_internal inheritance recursion");
    }
    Ok(())
}

// ATExecAddIndex: the IndexStmt is already transformed; indexcmds depends on
// tablecmds, so DefineIndex rides a seam.
fn ATExecAddIndex<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let stmt_node = cmd.def.expect("AT_AddIndex IndexStmt");
    let skip_build = tab.rewrite > 0;
    indexcmds_seams::define_index_for_alter::call(mcx, rel.rd_id, stmt_node, skip_build)?;
    Ok(())
}

const MAX_STATISTICS_TARGET: i32 = 10000;
const Anum_pg_attribute_attstattarget: usize = 21;

fn ATExecSetStatistics<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let Some(col_name) = cmd.name else {
        unported("ATExecSetStatistics by column number (index lane)");
    };
    let relname = rel.name().to_string();
    let mut newtarget = 0i32;
    let mut newtarget_default = true;
    if let Some(v) = cmd.def {
        let iv = v.as_integer().expect("SET STATISTICS Integer").ival;
        if iv != -1 {
            newtarget = iv;
            newtarget_default = false;
        }
    }
    if !newtarget_default {
        if newtarget < 0 {
            return Err(Box::new(
                PgError::new(ERROR, format!("statistics target {newtarget} is too low"))
                    .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        } else if newtarget > MAX_STATISTICS_TARGET {
            newtarget = MAX_STATISTICS_TARGET;
            elog_seams::ereport::call(
                PgError::new(
                    types_error::WARNING,
                    format!("lowering statistics target to {newtarget}"),
                )
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
            )?;
        }
    }
    if cmd.recurse && find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecSetStatistics inheritance recursion");
    }
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if rel.rd_att.attr(attnum as usize - 1).attgenerated == b'v' as i8 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot alter statistics on virtual generated column \"{col_name}\""
                ),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    update_pg_attribute_nullable(
        mcx,
        rel.rd_id,
        attnum,
        &[(
            Anum_pg_attribute_attstattarget,
            Datum::from_i16(newtarget as i16),
            newtarget_default,
        )],
    )
}

fn ATExecSetStorage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetStorage name");
    let relname = rel.name().to_string();
    let storagemode = cmd
        .def
        .expect("AT_SetStorage String")
        .as_string()
        .expect("AT_SetStorage String")
        .sval;
    if cmd.recurse && find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecSetStorage inheritance recursion");
    }
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let atttypid = rel.rd_att.attr(attnum as usize - 1).atttypid;
    let newstorage = get_attribute_storage(atttypid, storagemode)?;
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[(Anum_pg_attribute_attstorage, Datum::from_i8(newstorage as i8))],
    )?;
    set_index_storage_properties(mcx, rel, attnum, newstorage)
}

// GetAttributeStorage (tablecmds.c).
fn get_attribute_storage(atttypid: Oid, storagemode: &str) -> PgResult<u8> {
    let shape = || {
        syscache_seams::lookup_pg_type_shape::call(atttypid)
            .map(|s| s.expect("pg_type row vanished"))
    };
    let cstorage = if storagemode.eq_ignore_ascii_case("plain") {
        b'p'
    } else if storagemode.eq_ignore_ascii_case("external") {
        b'e'
    } else if storagemode.eq_ignore_ascii_case("extended") {
        b'x'
    } else if storagemode.eq_ignore_ascii_case("main") {
        b'm'
    } else if storagemode.eq_ignore_ascii_case("default") {
        shape()?.typstorage as u8
    } else {
        return Err(Box::new(
            PgError::new(ERROR, format!("invalid storage type \"{storagemode}\""))
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    };
    if cstorage != b'p' && shape()?.typstorage as u8 == b'p' {
        let name = format_type::format_type_be(atttypid).unwrap_or_else(|_| "???".into());
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column data type {name} can only have storage PLAIN"),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(cstorage)
}

// SetIndexStorageProperties, setstorage half: apply to simple index columns.
fn set_index_storage_properties<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    newstorage: u8,
) -> PgResult<()> {
    let indexes = relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    for &indexoid in indexes.iter() {
        // C index_open(lockmode); AT_SetStorage's lock level is AEL.
        let indrel = relation_seams::relation_open::call(mcx, indexoid, AccessExclusiveLock)?;
        let keys = pg_index_all_keys(mcx, indexoid)?;
        let Some(pos) = keys.iter().position(|&k| k == attnum) else {
            indrel.close(AccessExclusiveLock)?;
            continue;
        };
        update_pg_attribute(
            mcx,
            indexoid,
            (pos + 1) as AttrNumber,
            &[(Anum_pg_attribute_attstorage, Datum::from_i8(newstorage as i8))],
        )?;
        indrel.close(AccessExclusiveLock)?;
    }
    Ok(())
}

fn pg_index_all_keys<'mcx>(mcx: Mcx<'mcx>, indexoid: Oid) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let (_, _, keys, _) = pg_index_shape_full(mcx, indexoid)?;
    Ok(keys)
}

// ATExecAddConstraint -> ATAddCheckNNConstraint, CHECK arm.
fn ATExecAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let defnode = cmd.def.expect("AT_AddConstraint Constraint");
    let constr = defnode.as_variant::<Constraint>().expect("Constraint");
    if constr.contype == ConstrType::CONSTR_FOREIGN {
        if let Some(item) = crate::fk::ATExecAddConstraint(mcx, rel, constr)? {
            tab.fk_checks.push(item);
        }
        return Ok(());
    }
    if constr.contype != ConstrType::CONSTR_CHECK {
        unported(&format!("ATExecAddConstraint {:?}", constr.contype));
    }
    let cooked = crate::constraints::add_relation_new_constraints(
        mcx,
        rel,
        &[],
        &NodeList::make1(mcx, defnode)?,
        query_string,
    )?;
    for c in cooked.iter() {
        if !c.skip_validation {
            tab.constraints.push(NewConstraint { name: c.name, qual: c.expr.expect("CHECK expr") });
        }
    }
    xact::CommandCounterIncrement()?;
    if !constr.is_no_inherit && find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATAddCheckNNConstraint inheritance recursion");
    }
    Ok(())
}

// ATPrepAlterColumnType: build the transform (no USING; loud upstream) and
// queue the rewrite decision.
fn ATPrepAlterColumnType<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_AlterColumnType name");
    let relname = rel.name().to_string();
    let defnode = cmd.def.expect("AT_AlterColumnType ColumnDef");
    let def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
    let tn = def.typeName.expect("ColumnDef.typeName").as_variant::<TypeName>().expect("TypeName");

    let Some((attnum, attinhcount)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let att = *rel.rd_att.attr(attnum as usize - 1);
    if att.attgenerated != 0 && (def.raw_default.is_some() || def.cooked_default.is_some()) {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot specify USING when altering type of generated column".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION)
            .with_detail(format!("Column \"{col_name}\" is a generated column.")),
        ));
    }
    if def.raw_default.is_some() || def.cooked_default.is_some() {
        unported("ATPrepAlterColumnType USING transform");
    }
    if attinhcount > 0 {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot alter inherited column \"{col_name}\""))
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let (targettype, targettypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, tn)?;
    if def.collClause.is_some() {
        unported("ATPrepAlterColumnType COLLATE clause (GetColumnDefCollation)");
    }
    // GetColumnDefCollation: no COLLATE clause -> type default.

    if att.attgenerated == b'v' as i8 {
        // C builds no transform for virtual generated columns: no newval,
        // no rewrite of the column itself.
        if find_inheritance_children_exist(mcx, rel.rd_id)? {
            unported("ATPrepAlterColumnType inheritance recursion");
        }
        return Ok(());
    }

    let mut pstate = parser_small1::make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(str_arena(mcx, query_string)?.as_bytes());
    let var = Node::mk(
        mcx,
        types_nodes::primnodes::Var {
            varno: 1,
            varattno: attnum,
            vartype: att.atttypid,
            vartypmod: att.atttypmod,
            varcollid: att.attcollation,
            varnosyn: 1,
            varattnosyn: attnum,
            location: -1,
            ..Default::default()
        },
    )?;
    let transform = match coerce::coerce_to_target_type(
        mcx,
        &mut pstate,
        var,
        att.atttypid,
        targettype,
        targettypmod,
        coerce::CoercionContext::COERCION_ASSIGNMENT,
        types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )? {
        Some(t) => t,
        None => {
            let want = format_type::format_type_be(targettype).unwrap_or_else(|_| "???".into());
            let e = PgError::new(
                ERROR,
                format!("column \"{col_name}\" cannot be cast automatically to type {want}"),
            )
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH);
            let e = if att.attgenerated == 0 {
                let withmod = format_type::format_type_with_typemod(targettype, targettypmod)
                    .unwrap_or_else(|_| "???".into());
                let qcol = format_type::quote_identifier(col_name);
                e.with_hint(format!(
                    "You might need to specify \"USING {qcol}::{withmod}\"."
                ))
            } else {
                e
            };
            return Err(Box::new(e));
        }
    };
    parse_collate::assign_expr_collations(mcx, &mut pstate, transform)?;
    // expression_planner.
    let transform = clauses::eval_const_expressions(mcx, transform)?;
    tab.newvals.push(NewColumnValue { attnum, expr: transform, is_generated: false });
    if at_column_change_requires_rewrite(transform, attnum) {
        tab.rewrite |= AT_REWRITE_COLUMN_REWRITE;
    }
    parser_small1::free_parsestate(pstate)?;

    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATPrepAlterColumnType inheritance recursion");
    }
    Ok(())
}

// ATColumnChangeRequiresRewrite; domain/timestamp fastpath arms are loud.
fn at_column_change_requires_rewrite(expr: Node<'_>, varattno: AttrNumber) -> bool {
    let mut e = expr;
    loop {
        if let Some(v) = e.as_var() {
            return v.varattno != varattno;
        }
        if let Some(r) = e.as_variant::<types_nodes::primnodes::RelabelType>() {
            e = r.arg;
            continue;
        }
        return true;
    }
}

// ATExecAlterColumnType: catalog half; dependent indexes/constraints are loud.
fn ATExecAlterColumnType<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_AlterColumnType name");
    let relname = rel.name().to_string();
    let defnode = cmd.def.expect("AT_AlterColumnType ColumnDef");
    let def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
    let tn = def.typeName.expect("ColumnDef.typeName").as_variant::<TypeName>().expect("TypeName");

    if tab.rewrite != 0 {
        catalog_heap::RelationClearMissing(mcx, rel.rd_id)?;
        xact::CommandCounterIncrement()?;
    }

    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    let att = *rel.rd_att.attr(attnum as usize - 1);
    let old_att = tab.old_desc.attr(attnum as usize - 1);
    if att.atttypid != old_att.atttypid || att.atttypmod != old_att.atttypmod {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot alter type of column \"{col_name}\" twice"),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let (targettype, targettypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, tn)?;
    let shape = syscache_seams::lookup_pg_type_shape::call(targettype)?
        .expect("pg_type row vanished");
    let targetcollid = shape.typcollation;

    // Re-coerce any stored default before the column type flips.
    let defaultexpr = if att.atthasdef {
        let defval = rewrite_handler::build_column_default(mcx, rel, attnum as usize)?;
        let defval = nodes_core::strip_implicit_coercions(defval);
        let mut pstate = parser_small1::make_parsestate(mcx, None);
        let coerced = coerce::coerce_to_target_type(
            mcx,
            &mut pstate,
            defval,
            parse_expr::expr_type(defval),
            targettype,
            targettypmod,
            coerce::CoercionContext::COERCION_ASSIGNMENT,
            types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        parser_small1::free_parsestate(pstate)?;
        match coerced {
            Some(e) => Some(e),
            None => {
                let want =
                    format_type::format_type_be(targettype).unwrap_or_else(|_| "???".into());
                let msg = if att.attgenerated != 0 {
                    format!(
                        "generation expression for column \"{col_name}\" cannot be cast \
                         automatically to type {want}"
                    )
                } else {
                    format!(
                        "default for column \"{col_name}\" cannot be cast automatically \
                         to type {want}"
                    )
                };
                return Err(Box::new(
                    PgError::new(ERROR, msg).with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
                ));
            }
        }
    } else {
        None
    };

    remember_dependents_or_loud(mcx, rel, attnum, col_name, true)?;
    delete_column_type_dependencies(mcx, rel.rd_id, attnum, &att)?;

    if att.atthasmissing && tab.rewrite == 0 {
        unported("ATExecAlterColumnType attmissingval repack (no-rewrite fast-default)");
    }

    debug_assert!(tn.arrayBounds.is_nil());
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[
            (Anum_pg_attribute_atttypid, Datum::from_oid(targettype)),
            (Anum_pg_attribute_attlen, Datum::from_i16(shape.typlen)),
            (Anum_pg_attribute_atttypmod, Datum::from_i32(targettypmod)),
            (Anum_pg_attribute_attndims, Datum::from_i16(0)),
            (Anum_pg_attribute_attbyval, Datum::from_bool(shape.typbyval)),
            (Anum_pg_attribute_attalign, Datum::from_i8(shape.typalign)),
            (Anum_pg_attribute_attstorage, Datum::from_i8(shape.typstorage)),
            (Anum_pg_attribute_attcompression, Datum::from_i8(0)),
            (Anum_pg_attribute_attcollation, Datum::from_oid(targetcollid)),
        ],
    )?;

    let myself = pg_depend::ObjectAddress::sub_set(RELATION_RELATION_ID, rel.rd_id, attnum as i32);
    let reftype = pg_depend::ObjectAddress::set(TYPE_RELATION_ID, targettype);
    pg_depend::recordDependencyOn(mcx, &myself, &reftype, pg_depend::DependencyType::Normal)?;
    if targetcollid != InvalidOid && targetcollid != DEFAULT_COLLATION_OID {
        let refcoll = pg_depend::ObjectAddress::set(CollationRelationId, targetcollid);
        pg_depend::recordDependencyOn(mcx, &myself, &refcoll, pg_depend::DependencyType::Normal)?;
    }

    catalog_heap::RemoveStatistics(mcx, rel.rd_id, attnum)?;

    if let Some(defexpr) = defaultexpr {
        // A GENERATED default's INTERNAL dependency on the column would make
        // dependency.c refuse the deletion; drop the records first.
        if att.attgenerated != 0 {
            let attrdefoid = pg_attrdef::GetAttrDefaultOid(mcx, rel.rd_id, attnum)?;
            if attrdefoid == InvalidOid {
                panic!(
                    "could not find attrdef tuple for relation {} attnum {attnum}",
                    rel.rd_id
                );
            }
            pg_depend::deleteDependencyRecordsFor(
                mcx,
                types_core::ATTR_DEFAULT_RELATION_ID,
                attrdefoid,
                false,
            )?;
        }
        xact::CommandCounterIncrement()?;
        RemoveAttrDefault(mcx, rel.rd_id, attnum, true, true)?;
        let rel2 = table::table_open(mcx, rel.rd_id, NoLock)?;
        pg_attrdef::StoreAttrDefault(mcx, &rel2, attnum, defexpr)?;
        rel2.close(NoLock)?;
    }
    Ok(())
}

// RememberAllDependentForRebuilding: any dependent object beyond pg_attrdef
// rows of this relation means an index/constraint rebuild lane — loud.
fn remember_dependents_or_loud<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    col_name: &str,
    is_alter_type: bool,
) -> PgResult<()> {
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?;
    let keys = [
        oid_scankey(4, RELATION_RELATION_ID),
        oid_scankey(5, rel.rd_id),
        int4_key(6, attnum as i32),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependReferenceIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let classid =
            unsafe { types_tuple::heap_getattr(tup, 1, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let objid = unsafe { types_tuple::heap_getattr(tup, 2, desc, &mut isnull) }.as_oid();
        if classid == types_core::ATTR_DEFAULT_RELATION_ID {
            let (adrelid, adnum) = pg_attrdef::GetAttrDefaultColumnAddress(mcx, objid)?;
            if adrelid == rel.rd_id && adnum == attnum {
                // The column's own default expression; the caller deals
                // with it.
                continue;
            }
            if !is_alter_type {
                continue;
            }
            // Only a same-table generated column can reference this column.
            assert!(adrelid == rel.rd_id, "attrdef dependency from another relation");
            let gen_att = rel.rd_att.attr(adnum as usize - 1);
            let genname =
                core::str::from_utf8(gen_att.attname.name_str()).expect("attname UTF-8");
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "cannot alter type of a column used by a generated column".to_string(),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail(format!(
                    "Column \"{col_name}\" is used by generated column \"{genname}\"."
                )),
            ));
        }
        unported(&format!(
            "RememberAllDependentForRebuilding: dependent object (class {classid}, oid \
             {objid}) — index/constraint rebuild lane"
        ));
    }
    genam::systable_endscan(mcx, scan)?;
    dep_rel.close(RowExclusiveLock)
}

// The depender-side scan in ATExecAlterColumnType: only the type (and
// possibly collation) dependencies may exist; delete them.
fn delete_column_type_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    att: &types_tuple::FormData_pg_attribute,
) -> PgResult<()> {
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?;
    let keys = [
        oid_scankey(1, RELATION_RELATION_ID),
        oid_scankey(2, relid),
        int4_key(3, attnum as i32),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    let mut tids: PgVec<'mcx, types_tuple::ItemPointerData> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refclassid =
            unsafe { types_tuple::heap_getattr(tup, 4, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let refobjid = unsafe { types_tuple::heap_getattr(tup, 5, desc, &mut isnull) }.as_oid();
        let is_type = refclassid == TYPE_RELATION_ID && refobjid == att.atttypid;
        let is_coll = refclassid == CollationRelationId && refobjid == att.attcollation;
        assert!(is_type || is_coll, "found unexpected dependency for column");
        tids.push(tup.t_self);
    }
    genam::systable_endscan(mcx, scan)?;
    for tid in tids.iter() {
        catalog_indexing::CatalogTupleDelete(&dep_rel, tid)?;
    }
    dep_rel.close(RowExclusiveLock)
}

pub(crate) fn int4_key(attno: usize, v: i32) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT4EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT4EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i32(v);
    key
}

// Single-row pg_attribute field update via heap_modify_tuple.
pub(crate) fn update_pg_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    fields: &[(usize, Datum)],
) -> PgResult<()> {
    let mut full: PgVec<'_, (usize, Datum, bool)> = mcx::vec_with_capacity_in(mcx, fields.len())?;
    full.extend(fields.iter().map(|&(a, v)| (a, v, false)));
    update_pg_attribute_nullable(mcx, relid, attnum, &full)
}

fn update_pg_attribute_nullable<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    fields: &[(usize, Datum, bool)],
) -> PgResult<()> {
    let attrel =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_scankey(1, relid), int2_key(5, attnum)];
    let mut scan =
        genam::systable_beginscan(mcx, &attrel, AttributeRelidNumIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for attribute {attnum} of relation {relid}"));
    let desc = attrel.descr();
    let natts = desc.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    for &(anum, v, isnull) in fields {
        repl_values[anum - 1] = v;
        repl_isnull[anum - 1] = isnull;
        repl[anum - 1] = true;
    }
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attrel, &otid, &mut newtup)?;
    attrel.close(RowExclusiveLock)
}

fn int2_key(attno: usize, v: i16) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT2EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT2EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i16(v);
    key
}

fn str_arena<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

#[cold]
#[inline(never)]
fn undefined_column(col_name: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("column \"{col_name}\" of relation \"{relname}\" does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
    )
}

#[cold]
#[inline(never)]
fn cannot_alter_system_column(col_name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("cannot alter system column \"{col_name}\""))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub(crate) fn find_inheritance_children_exist<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, InheritsRelationId, types_rel::AccessShareLock)?;
    let key = oid_scankey(Anum_pg_inherits_inhparent, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsParentIndexId, true, None, &[key])?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

const Anum_pg_class_relrowsecurity: usize = 24;
const Anum_pg_class_relforcerowsecurity: usize = 25;

fn set_pg_class_bool<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: usize,
    value: bool,
) -> PgResult<()> {
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(1, rel.rd_id);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {}", rel.rd_id));
    let natts = pg_class.descr().natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[attnum - 1] = Datum::from_bool(value);
    repl[attnum - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(
        mcx,
        reltup,
        pg_class.descr(),
        &repl_values,
        &repl_isnull,
        &repl,
    )?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    pg_class.close(RowExclusiveLock)
}

fn ATExecSetRowSecurity<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>, rls: bool) -> PgResult<()> {
    set_pg_class_bool(mcx, rel, Anum_pg_class_relrowsecurity, rls)
}

fn ATExecForceNoForceRowSecurity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    force_rls: bool,
) -> PgResult<()> {
    set_pg_class_bool(mcx, rel, Anum_pg_class_relforcerowsecurity, force_rls)
}

const Anum_pg_class_reloftype: usize = 5;
const Anum_pg_class_relreplident: usize = 27;
const TableSpaceRelationId: Oid = 1213;
const GLOBALTABLESPACE_OID: Oid = 1664;

fn pg_class_read_attr(mcx: Mcx<'_>, relid: Oid, attnum: usize) -> PgResult<Datum> {
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_scankey(1, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_class columns under pg_class's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, pg_class.descr(), &mut isnull) };
    debug_assert!(!isnull);
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;
    Ok(d)
}

fn set_pg_class_datum<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: usize,
    value: Datum,
) -> PgResult<()> {
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(1, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let natts = pg_class.descr().natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[attnum - 1] = value;
    repl[attnum - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(
        mcx,
        reltup,
        pg_class.descr(),
        &repl_values,
        &repl_isnull,
        &repl,
    )?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    pg_class.close(RowExclusiveLock)
}

// relation_mark_replica_identity (tablecmds.c:18402).
fn relation_mark_replica_identity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ri_type: u8,
    index_oid: Oid,
) -> PgResult<()> {
    let current = pg_class_read_attr(mcx, rel.rd_id, Anum_pg_class_relreplident)?.as_i8() as u8;
    if current != ri_type {
        set_pg_class_datum(
            mcx,
            rel.rd_id,
            Anum_pg_class_relreplident,
            Datum::from_i8(ri_type as i8),
        )?;
    }

    let pg_index = table::table_open(mcx, types_core::INDEX_RELATION_ID, RowExclusiveLock)?;
    let desc = pg_index.descr();
    for &this_index in relcache::RelationGetIndexList(mcx, rel.rd_id)?.iter() {
        let key = [oid_scankey(1, this_index)];
        let mut scan =
            genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {this_index}"));
        let mut isnull = false;
        // SAFETY: indisreplident is a fixed NOT NULL pg_index column.
        let isreplident = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_index_indisreplident as i32, desc, &mut isnull)
        }
        .as_bool();
        let want = this_index == index_oid;
        if isreplident != want {
            let natts = desc.natts as usize;
            let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[Anum_pg_index_indisreplident - 1] = Datum::from_bool(want);
            replace[Anum_pg_index_indisreplident - 1] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            let otid = tup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)?;
            inval::invalidate::CacheInvalidateRelcacheByRelid(rel.rd_id)?;
        } else {
            genam::systable_endscan(mcx, scan)?;
        }
    }
    pg_index.close(RowExclusiveLock)
}

// ATExecReplicaIdentity (tablecmds.c:18490).
fn ATExecReplicaIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    stmt: &types_nodes::parsenodes::ReplicaIdentityStmt<'_>,
) -> PgResult<()> {
    use types_nodes::parsenodes::{
        REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX,
        REPLICA_IDENTITY_NOTHING,
    };
    match stmt.identity_type {
        REPLICA_IDENTITY_DEFAULT | REPLICA_IDENTITY_FULL | REPLICA_IDENTITY_NOTHING => {
            return relation_mark_replica_identity(mcx, rel, stmt.identity_type, InvalidOid);
        }
        REPLICA_IDENTITY_INDEX => {}
        other => panic!("unexpected identity type {other}"),
    }

    let index_name = stmt.name.expect("REPLICA IDENTITY USING INDEX name");
    let index_oid = lsyscache::get_relname_relid(index_name, rel.rd_rel.relnamespace)?;
    if index_oid == InvalidOid {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "index \"{index_name}\" for table \"{}\" does not exist",
                    rel.name()
                ),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    let index_rel = indexam::index_open(mcx, index_oid, types_rel::ShareLock)?;
    let index_relname = index_rel.name().to_string();
    let wrong_type = |msg: String| -> Box<PgError> {
        Box::new(PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    };
    let not_supported = |msg: String| -> Box<PgError> {
        Box::new(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
    };
    let Some(index_form) = index_rel.rd_index.as_ref() else {
        return Err(wrong_type(format!(
            "\"{index_relname}\" is not an index for table \"{}\"",
            rel.name()
        )));
    };
    if index_form.indrelid != rel.rd_id {
        return Err(wrong_type(format!(
            "\"{index_relname}\" is not an index for table \"{}\"",
            rel.name()
        )));
    }
    // rd_indam->amcanunique: btree is the only ported AM whose handler sets
    // it (matches stock pg_am; CREATE ACCESS METHOD is unported).
    let amcanunique = index_rel.rd_rel.relam == types_core::BTREE_AM_OID;
    if (!amcanunique || !index_form.indisunique)
        && !(index_form.indisunique && index_form.indisexclusion)
    {
        return Err(wrong_type(format!(
            "cannot use non-unique index \"{index_relname}\" as replica identity"
        )));
    }
    if !index_form.indimmediate {
        return Err(not_supported(format!(
            "cannot use non-immediate index \"{index_relname}\" as replica identity"
        )));
    }
    if index_form.indexprs_src.is_some() {
        return Err(not_supported(format!(
            "cannot use expression index \"{index_relname}\" as replica identity"
        )));
    }
    if index_form.has_indpred {
        return Err(not_supported(format!(
            "cannot use partial index \"{index_relname}\" as replica identity"
        )));
    }
    for key in 0..index_form.indnkeyatts as usize {
        let attno = index_form.indkey[key];
        if attno <= 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "index \"{index_relname}\" cannot be used as replica identity \
                         because column {attno} is a system column"
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_REFERENCE),
            ));
        }
        let attr = rel.rd_att.attr(attno as usize - 1);
        if !attr.attnotnull {
            let attname =
                core::str::from_utf8(attr.attname.name_str()).expect("attname UTF-8");
            return Err(wrong_type(format!(
                "index \"{index_relname}\" cannot be used as replica identity \
                 because column \"{attname}\" is nullable"
            )));
        }
    }
    relation_mark_replica_identity(mcx, rel, stmt.identity_type, index_oid)?;
    index_rel.close(NoLock)
}

// check_of_type (tablecmds.c:7143).
fn check_of_type(mcx: Mcx<'_>, typeid: Oid) -> PgResult<()> {
    const TYPTYPE_COMPOSITE: u8 = b'c';
    if lsyscache::get_typtype(typeid)? as u8 == TYPTYPE_COMPOSITE {
        let typrelid = lsyscache::get_typ_typrelid(typeid)?;
        debug_assert!(typrelid != InvalidOid);
        let type_relation =
            relation_seams::relation_open::call(mcx, typrelid, types_rel::AccessShareLock)?;
        let type_ok = type_relation.rd_rel.relkind == types_rel::RELKIND_COMPOSITE_TYPE;
        // Keep the AccessShareLock on the parent rel until xact commit.
        type_relation.close(NoLock)?;
        if !type_ok {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "type {} is the row type of another table",
                        format_type::format_type_be(typeid)?
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(
                    "A typed table must use a stand-alone composite type created with \
                     CREATE TYPE."
                        .to_string(),
                ),
            ));
        }
        Ok(())
    } else {
        Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "type {} is not a composite type",
                    format_type::format_type_be(typeid)?
                ),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ))
    }
}

// ATExecAddOf (tablecmds.c:18216).
fn ATExecAddOf<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    of_typename: &TypeName<'_>,
) -> PgResult<()> {
    let relid = rel.rd_id;
    let (typeid, _typmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, of_typename)?;
    check_of_type(mcx, typeid)?;

    if pg_inherits::has_superclass(mcx, relid)? {
        return Err(Box::new(
            PgError::new(ERROR, "typed tables cannot inherit".to_string())
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    let type_tupdesc = typcache::lookup_rowtype_tupdesc_copy(mcx, typeid, -1)?;
    let table_tupdesc = &rel.rd_att;
    let table_natts = table_tupdesc.natts as usize;
    let mismatch = |msg: String| -> Box<PgError> {
        Box::new(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
    };
    let mut table_attno: usize = 0;
    for type_attno in 0..type_tupdesc.natts as usize {
        let type_attr = type_tupdesc.attr(type_attno);
        if type_attr.attisdropped {
            continue;
        }
        let type_attname =
            core::str::from_utf8(type_attr.attname.name_str()).expect("attname UTF-8");
        let table_attr = loop {
            if table_attno >= table_natts {
                return Err(mismatch(format!("table is missing column \"{type_attname}\"")));
            }
            let attr = table_tupdesc.attr(table_attno);
            table_attno += 1;
            if !attr.attisdropped {
                break attr;
            }
        };
        let table_attname =
            core::str::from_utf8(table_attr.attname.name_str()).expect("attname UTF-8");
        if table_attname != type_attname {
            return Err(mismatch(format!(
                "table has column \"{table_attname}\" where type requires \"{type_attname}\""
            )));
        }
        if table_attr.atttypid != type_attr.atttypid
            || table_attr.atttypmod != type_attr.atttypmod
            || table_attr.attcollation != type_attr.attcollation
        {
            return Err(mismatch(format!(
                "table \"{}\" has different type for column \"{type_attname}\"",
                rel.name()
            )));
        }
    }
    while table_attno < table_natts {
        let table_attr = table_tupdesc.attr(table_attno);
        table_attno += 1;
        if !table_attr.attisdropped {
            let attname =
                core::str::from_utf8(table_attr.attname.name_str()).expect("attname UTF-8");
            return Err(mismatch(format!("table has extra column \"{attname}\"")));
        }
    }

    let cur_reloftype = pg_class_read_attr(mcx, relid, Anum_pg_class_reloftype)?.as_oid();
    if cur_reloftype != InvalidOid {
        drop_parent_dependency_on_class(
            mcx,
            relid,
            TYPE_RELATION_ID,
            cur_reloftype,
            pg_depend::DependencyType::Normal,
        )?;
    }

    let tableobj = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, relid);
    let typeobj = pg_depend::ObjectAddress::set(TYPE_RELATION_ID, typeid);
    pg_depend::recordDependencyOn(mcx, &tableobj, &typeobj, pg_depend::DependencyType::Normal)?;

    set_pg_class_datum(mcx, relid, Anum_pg_class_reloftype, Datum::from_oid(typeid))
}

// ATExecDropOf (tablecmds.c:18358).
fn ATExecDropOf<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<()> {
    let relid = rel.rd_id;
    let reloftype = pg_class_read_attr(mcx, relid, Anum_pg_class_reloftype)?.as_oid();
    if reloftype == InvalidOid {
        return Err(Box::new(
            PgError::new(ERROR, format!("\"{}\" is not a typed table", rel.name()))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    drop_parent_dependency_on_class(
        mcx,
        relid,
        TYPE_RELATION_ID,
        reloftype,
        pg_depend::DependencyType::Normal,
    )?;
    set_pg_class_datum(mcx, relid, Anum_pg_class_reloftype, Datum::from_oid(InvalidOid))
}

// ATExecAddIndexConstraint (tablecmds.c:9704).
fn ATExecAddIndexConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    stmt: &types_nodes::rawnodes::IndexStmt<'mcx>,
) -> PgResult<()> {
    let index_oid = stmt.indexOid;
    debug_assert!(index_oid != InvalidOid);
    debug_assert!(stmt.isconstraint);

    let index_rel = indexam::index_open(mcx, index_oid, types_rel::AccessShareLock)?;
    let index_name = index_rel.name().to_string();
    let index_info = execindexing::BuildIndexInfo(mcx, &index_rel)?;
    if !index_info.ii_Unique {
        panic!("index \"{index_name}\" is not unique");
    }

    let constraint_name = match stmt.idxname {
        Some(cn) if cn != index_name => {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "ALTER TABLE / ADD CONSTRAINT USING INDEX will rename index \
                     \"{index_name}\" to \"{cn}\""
                ),
                None,
            )?;
            crate::rename::RenameRelationInternal(mcx, index_oid, cn, true)?;
            cn.to_string()
        }
        Some(cn) => cn.to_string(),
        None => index_name.clone(),
    };

    if stmt.primary {
        catalog_index::index_check_primary_key(mcx, rel, &index_info, true)?;
    }
    let constraint_type = if stmt.primary {
        pg_constraint::CONSTRAINT_PRIMARY
    } else {
        pg_constraint::CONSTRAINT_UNIQUE
    };
    let mut flags: u16 = catalog_index::INDEX_CONSTR_CREATE_UPDATE_INDEX
        | catalog_index::INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS;
    if stmt.initdeferred {
        flags |= catalog_index::INDEX_CONSTR_CREATE_INIT_DEFERRED;
    }
    if stmt.deferrable {
        flags |= catalog_index::INDEX_CONSTR_CREATE_DEFERRABLE;
    }
    if stmt.primary {
        flags |= catalog_index::INDEX_CONSTR_CREATE_MARK_AS_PRIMARY;
    }
    catalog_index::index_constraint_create(
        mcx,
        rel,
        index_oid,
        InvalidOid,
        &index_info,
        &constraint_name,
        constraint_type,
        flags,
        init_small::globals::allowSystemTableMods(),
    )?;
    index_rel.close(NoLock)
}

// ATPrepSetTableSpace (tablecmds.c:16615).
fn ATPrepSetTableSpace<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    tablespacename: &str,
) -> PgResult<()> {
    let tablespace_id = commands_tablespace::get_tablespace_oid(mcx, tablespacename, false)?;
    if tablespace_id != InvalidOid
        && tablespace_id != init_small::globals::MyDatabaseTableSpace()
    {
        let aclresult = aclchk::object_aclcheck(
            TableSpaceRelationId,
            tablespace_id,
            miscinit::GetUserId(),
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk::aclcheck_error(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE,
                tablespacename,
            )?;
        }
    }
    if tab.new_tablespace != InvalidOid {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot have multiple SET TABLESPACE subcommands".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
        ));
    }
    tab.new_tablespace = tablespace_id;
    Ok(())
}

// CheckRelationTableSpaceMove (tablecmds.c:3682); false = silent no-op.
fn CheckRelationTableSpaceMove(rel: &Relation<'_>, new_tablespace_id: Oid) -> PgResult<bool> {
    let old_tablespace_id = rel.rd_rel.reltablespace;
    if new_tablespace_id == old_tablespace_id
        || (new_tablespace_id == init_small::globals::MyDatabaseTableSpace()
            && old_tablespace_id == InvalidOid)
    {
        return Ok(false);
    }
    // RelationIsMapped: mapped relations carry relfilenode 0.
    if rel.rd_rel.relfilenode == InvalidOid {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot move system relation \"{}\"", rel.name()),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if new_tablespace_id == GLOBALTABLESPACE_OID {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "only shared relations can be placed in pg_global tablespace".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    // RELATION_IS_OTHER_TEMP: temp relations unported.
    Ok(true)
}

// ATExecSetTableSpace (tablecmds.c:16853): validation surface; the physical
// move (new relfilenumber + fork copy) rides the tablespace storage lane.
fn ATExecSetTableSpace<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
    new_tablespace: Oid,
    _lockmode: LOCKMODE,
) -> PgResult<()> {
    let rel = table::table_open(mcx, table_oid, NoLock)?;
    if !CheckRelationTableSpaceMove(&rel, new_tablespace)? {
        return rel.close(NoLock);
    }
    unported("ATExecSetTableSpace: physical relation move (tablespace storage lane)");
}

// ATPrepSetAccessMethod (tablecmds.c:16491).
fn ATPrepSetAccessMethod<'mcx>(
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    amname: Option<&str>,
) -> PgResult<()> {
    let amoid = match amname {
        Some(name) => commands_amcmds::get_table_am_oid(name, false)?,
        // Partitioned DEFAULT arm unreachable (relkind gate).
        None => commands_amcmds::get_table_am_oid(&tableam::default_table_access_method(), false)?,
    };
    if rel.rd_rel.relam == amoid {
        return Ok(());
    }
    tab.rewrite |= AT_REWRITE_ACCESS_METHOD;
    tab.new_access_method = amoid;
    tab.chg_access_method = true;
    Ok(())
}

// drop_parent_dependency (tablecmds.c:16351) generalized to refclassid;
// inherit-recurse lane's RemoveInheritance leg delegates here when it lands.
fn drop_parent_dependency_on_class<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    target_refclassid: Oid,
    refobjid: Oid,
    deptype: pg_depend::DependencyType,
) -> PgResult<()> {
    let dep_rel =
        table::table_open(mcx, pg_depend::DependRelationId, types_rel::RowExclusiveLock)?;
    let keys = [
        oid_scankey(1, RELATION_RELATION_ID),
        oid_scankey(2, relid),
        int4_key(3, 0),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    let mut tids: PgVec<'mcx, types_tuple::ItemPointerData> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refclassid =
            unsafe { types_tuple::heap_getattr(tup, 4, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let dep_refobjid =
            unsafe { types_tuple::heap_getattr(tup, 5, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let refobjsubid =
            unsafe { types_tuple::heap_getattr(tup, 6, desc, &mut isnull) }.as_i32();
        // SAFETY: as above.
        let dtype =
            unsafe { types_tuple::heap_getattr(tup, 7, desc, &mut isnull) }.as_i8();
        if refclassid == target_refclassid
            && dep_refobjid == refobjid
            && refobjsubid == 0
            && dtype == deptype.as_char()
        {
            tids.push(tup.t_self);
        }
    }
    genam::systable_endscan(mcx, scan)?;
    for tid in tids.iter() {
        catalog_indexing::CatalogTupleDelete(&dep_rel, tid)?;
    }
    dep_rel.close(types_rel::RowExclusiveLock)
}
