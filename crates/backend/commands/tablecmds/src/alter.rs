// AlterTable three-phase machinery (ATController): ADD/DROP COLUMN,
// SET/DROP DEFAULT, SET/DROP NOT NULL, ADD CONSTRAINT CHECK, ALTER TYPE,
// with inheritance/partition recursion. LOUD: other subtypes, index
// rebuilds on rewrite.
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, DEFAULT_COLLATION_OID, RELATION_RELATION_ID, TYPE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_NOT_NULL_VIOLATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERROR, NOTICE,
};
use types_nodes::parsenodes::{AlterTableCmd, AlterTableStmt, AlterTableType, ObjectType};
use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, TypeName};
use types_nodes::{Node, NodeList};
use types_rel::{AccessExclusiveLock, InplaceUpdateTupleLock, NoLock, Relation, RowExclusiveLock, ShareRowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{MaxHeapAttributeNumber, TupleDescData, ATTNULLABLE_VALID};

const AT_NUM_PASSES: usize = 12;
const AT_PASS_DROP: usize = 0;
const AT_PASS_ALTER_TYPE: usize = 1;
const AT_PASS_ADD_COL: usize = 2;
const AT_PASS_SET_EXPRESSION: usize = 3;
const AT_PASS_OLD_INDEX: usize = 4;
const AT_PASS_OLD_CONSTR: usize = 5;
const AT_PASS_ADD_CONSTR: usize = 6;
const AT_PASS_COL_ATTRS: usize = 7;
const AT_PASS_ADD_INDEXCONSTR: usize = 8;
const AT_PASS_ADD_INDEX: usize = 9;
const AT_PASS_ADD_OTHERCONSTR: usize = 10;
const AT_PASS_MISC: usize = 11;
const AT_REWRITE_ALTER_PERSISTENCE: i32 = 1 << 0;
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
const Anum_pg_attribute_attislocal: usize = 18;
const Anum_pg_attribute_attinhcount: usize = 19;
const Anum_pg_attribute_atthasmissing: usize = 14;
const Anum_pg_attribute_attidentity: usize = 15;
const Anum_pg_attribute_attgenerated: usize = 16;
const Anum_pg_attribute_attcollation: usize = 20;

const AttributeRelidNumIndexId: Oid = 2659;
const InheritsRelationId: Oid = 2611;
const InheritsParentIndexId: Oid = 2187;
const Anum_pg_inherits_inhparent: usize = 2;
const Anum_pg_class_relnatts: usize = 19;
const CollationRelationId: Oid = 3456;
pub(crate) const NamespaceRelationId: Oid = 2615;

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
            | AlterTableType::AT_AddIdentity
            | AlterTableType::AT_SetIdentity
            | AlterTableType::AT_DropIdentity
            | AlterTableType::AT_SetLogged
            | AlterTableType::AT_SetUnLogged
            | AlterTableType::AT_SetStorage
            | AlterTableType::AT_SetCompression
            | AlterTableType::AT_AddColumnToView
            | AlterTableType::AT_DropOids
            | AlterTableType::AT_AlterConstraint
            | AlterTableType::AT_AlterColumnGenericOptions
            | AlterTableType::AT_ReplaceRelOptions => AccessExclusiveLock,
            AlterTableType::AT_SetStatistics
            | AlterTableType::AT_SetOptions
            | AlterTableType::AT_ResetOptions
            | AlterTableType::AT_ValidateConstraint
            | AlterTableType::AT_ClusterOn
            | AlterTableType::AT_DropCluster => types_rel::ShareUpdateExclusiveLock,
            AlterTableType::AT_AddConstraint
            | AlterTableType::AT_ReAddConstraint
            | AlterTableType::AT_ReAddDomainConstraint => {
                match cmd.def.and_then(|d| d.as_variant::<Constraint>()) {
                    Some(constr) => match constr.contype {
                        ConstrType::CONSTR_FOREIGN => ShareRowExclusiveLock,
                        _ => AccessExclusiveLock,
                    },
                    None => AccessExclusiveLock,
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
            AlterTableType::AT_AttachPartition => types_rel::ShareUpdateExclusiveLock,
            AlterTableType::AT_DetachPartition => {
                let concurrent = cmd
                    .def
                    .and_then(|d| d.as_variant::<types_nodes::rawnodes::PartitionCmd>())
                    .map(|p| p.concurrent)
                    .unwrap_or(false);
                if concurrent { types_rel::ShareUpdateExclusiveLock } else { AccessExclusiveLock }
            }
            AlterTableType::AT_DetachPartitionFinalize => types_rel::ShareUpdateExclusiveLock,
            AlterTableType::AT_SetRelOptions | AlterTableType::AT_ResetRelOptions => {
                match cmd.def.and_then(|d| d.as_list()) {
                    Some(l) => reloptions::AlterTableGetRelOptionsLockLevel(l),
                    None => AccessExclusiveLock,
                }
            }
            AlterTableType::AT_AddInherit | AlterTableType::AT_DropInherit => {
                AccessExclusiveLock
            }
            AlterTableType::AT_EnableTrig
            | AlterTableType::AT_EnableAlwaysTrig
            | AlterTableType::AT_EnableReplicaTrig
            | AlterTableType::AT_EnableTrigAll
            | AlterTableType::AT_EnableTrigUser
            | AlterTableType::AT_DisableTrig
            | AlterTableType::AT_DisableTrigAll
            | AlterTableType::AT_DisableTrigUser => ShareRowExclusiveLock,
            AlterTableType::AT_ReplicaIdentity
            | AlterTableType::AT_AddOf
            | AlterTableType::AT_DropOf
            | AlterTableType::AT_SetTableSpace
            | AlterTableType::AT_SetAccessMethod
            | AlterTableType::AT_ChangeOwner
            | AlterTableType::AT_GenericOptions => AccessExclusiveLock,
            AlterTableType::AT_AddColumnToView
            | AlterTableType::AT_ReplaceRelOptions => AccessExclusiveLock,
            other => panic!("unrecognized alter table type: {}", other as i32),
        };
        if cmd_lockmode > lockmode {
            lockmode = cmd_lockmode;
        }
    }
    lockmode
}

// The C callback keys several checks off the calling statement's node type.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlterRelationStmtKind {
    AlterTable,
    Rename,
    AlterObjectSchema,
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
        stmt.objtype,
        AlterRelationStmtKind::AlterTable,
    )
}

pub(crate) fn AlterTableLookupRangeVar<'mcx>(
    mcx: Mcx<'mcx>,
    prv: &types_nodes::primnodes::RangeVar<'_>,
    lockmode: LOCKMODE,
    missing_ok: bool,
    objtype: ObjectType,
    stmt_kind: AlterRelationStmtKind,
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
        RangeVarCallbackForAlterRelation(mcx, rv, relOid, objtype, stmt_kind)
    };
    let flags = if missing_ok { catalog_namespace::RVR_MISSING_OK } else { 0 };
    catalog_namespace::RangeVarGetRelidExtended(&rv, lockmode, flags, Some(&mut callback))
}

fn RangeVarCallbackForAlterRelation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel_vocab::RangeVar<'_>,
    relOid: Oid,
    objtype: ObjectType,
    stmt_kind: AlterRelationStmtKind,
) -> PgResult<()> {
    if relOid == InvalidOid {
        return Ok(());
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

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relOid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(relkind),
            rel.relname,
        )?;
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
    // ALTER .. RENAME also needs (still-valid) CREATE rights on the schema.
    if stmt_kind == AlterRelationStmtKind::Rename {
        let aclresult = aclchk::object_aclcheck(
            NamespaceRelationId,
            relnamespace,
            miscinit::GetUserId(),
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            let nspname = lsyscache::get_namespace_name(mcx, relnamespace)?
                .expect("namespace has a name");
            aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_SCHEMA, &nspname)?;
        }
    }
    let wrong_type = |msg: String| -> PgResult<()> {
        Err(Box::new(
            PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ))
    };
    match objtype {
        ObjectType::OBJECT_SEQUENCE => {
            if relkind != types_rel::RELKIND_SEQUENCE {
                return wrong_type(format!("\"{}\" is not a sequence", rel.relname));
            }
        }
        ObjectType::OBJECT_VIEW => {
            if relkind != types_rel::RELKIND_VIEW {
                return wrong_type(format!("\"{}\" is not a view", rel.relname));
            }
        }
        ObjectType::OBJECT_MATVIEW => {
            if relkind != types_rel::RELKIND_MATVIEW {
                return wrong_type(format!("\"{}\" is not a materialized view", rel.relname));
            }
        }
        ObjectType::OBJECT_FOREIGN_TABLE => {
            if relkind != types_rel::RELKIND_FOREIGN_TABLE {
                return wrong_type(format!("\"{}\" is not a foreign table", rel.relname));
            }
        }
        ObjectType::OBJECT_TYPE => {
            if relkind != types_rel::RELKIND_COMPOSITE_TYPE {
                return wrong_type(format!("\"{}\" is not a composite type", rel.relname));
            }
        }
        ObjectType::OBJECT_INDEX => {
            if relkind != types_rel::RELKIND_INDEX
                && relkind != types_rel::RELKIND_PARTITIONED_INDEX
                && stmt_kind != AlterRelationStmtKind::Rename
            {
                return wrong_type(format!("\"{}\" is not an index", rel.relname));
            }
        }
        _ => {}
    }
    if objtype != ObjectType::OBJECT_TYPE && relkind == types_rel::RELKIND_COMPOSITE_TYPE {
        return Err(Box::new(
            PgError::new(ERROR, format!("\"{}\" is a composite type", rel.relname))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Use ALTER TYPE instead."),
        ));
    }
    if stmt_kind == AlterRelationStmtKind::AlterObjectSchema {
        if relkind == types_rel::RELKIND_INDEX || relkind == types_rel::RELKIND_PARTITIONED_INDEX
        {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot change schema of index \"{}\"", rel.relname),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Change the schema of the table instead."),
            ));
        } else if relkind == types_rel::RELKIND_COMPOSITE_TYPE {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot change schema of composite type \"{}\"", rel.relname),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Use ALTER TYPE instead."),
            ));
        } else if relkind == types_rel::RELKIND_TOASTVALUE {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot change schema of TOAST table \"{}\"", rel.relname),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Change the schema of the table instead."),
            ));
        }
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

pub(crate) struct AlteredTableInfo<'mcx> {
    pub(crate) relid: Oid,
    relkind: u8,
    old_desc: std::rc::Rc<TupleDescData<'mcx>>,
    subcmds: [NodeList<'mcx>; AT_NUM_PASSES],
    rewrite: i32,
    chg_persistence: bool,
    newrelpersistence: u8,
    new_tablespace: Oid,
    chg_access_method: bool,
    new_access_method: Oid,
    verify_new_notnull: bool,
    after_stmts: NodeList<'mcx>,
    newvals: PgVec<'mcx, NewColumnValue<'mcx>>,
    constraints: PgVec<'mcx, NewConstraint<'mcx>>,
    pub(crate) fk_checks: PgVec<'mcx, crate::fk::FkValidateItem<'mcx>>,
    pub(crate) partition_constraint: Option<Node<'mcx>>,
    pub(crate) validate_default: bool,
    changed_constraints: Vec<(Oid, String)>,
    changed_indexes: Vec<(Oid, String)>,
    changed_statistics: Vec<(Oid, String)>,
    replica_identity_index: Option<String>,
    cluster_on_index: Option<String>,
}

impl<'mcx> AlteredTableInfo<'mcx> {
    pub(crate) fn new(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> Self {
        AlteredTableInfo {
            relid: rel.rd_id,
            relkind: rel.rd_rel.relkind,
            old_desc: rel.rd_att.clone(),
            subcmds: core::array::from_fn(|_| NodeList::nil()),
            rewrite: 0,
            chg_persistence: false,
            newrelpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
            new_tablespace: InvalidOid,
            chg_access_method: false,
            new_access_method: InvalidOid,
            verify_new_notnull: false,
            after_stmts: NodeList::nil(),
            newvals: PgVec::new_in(mcx),
            constraints: PgVec::new_in(mcx),
            fk_checks: PgVec::new_in(mcx),
            partition_constraint: None,
            validate_default: false,
            changed_constraints: Vec::new(),
            changed_indexes: Vec::new(),
            changed_statistics: Vec::new(),
            replica_identity_index: None,
            cluster_on_index: None,
        }
    }
}

pub(crate) type Wqueue<'mcx> = PgVec<'mcx, AlteredTableInfo<'mcx>>;

pub(crate) fn ATGetQueueEntry<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
) -> usize {
    if let Some(i) = wqueue.iter().position(|t| t.relid == rel.rd_id) {
        return i;
    }
    wqueue.push(AlteredTableInfo::new(mcx, rel));
    wqueue.len() - 1
}

/// CheckAlterTableIsSafe (tablecmds.c:4449): `CheckTableNotInUse` plus a check
/// that the relation is not another session's temp table. C splits the two
/// because some `CheckTableNotInUse` callers must NOT reject other-session temp
/// relations — notably DROP, which has to be able to clean out an orphaned temp
/// schema. C's own comment points at `truncate_check_activity` as the model, and
/// that is what this mirrors. Order matches C: temp check first.
pub(crate) fn CheckAlterTableIsSafe(rel: &Relation<'_>) -> PgResult<()> {
    // Their local buffer manager cannot cope if we change the table's
    // contents, and optimizations may assume temp tables see no such
    // interference.
    if rel.is_other_temp() {
        return Err(Box::new(
            PgError::error("cannot alter temporary tables of other sessions")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    catalog_heap::CheckTableNotInUse(rel, "ALTER TABLE")
}

pub fn AlterTable<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    lockmode: LOCKMODE,
    stmt: &AlterTableStmt<'mcx>,
    query_string: &str,
    tag: types_core::CommandTag,
) -> PgResult<()> {
    let rel = relation_seams::relation_open::call(mcx, relid, NoLock)?;
    CheckAlterTableIsSafe(&rel)?;
    let recurse = stmt.relation.expect("AlterTableStmt.relation").inh;
    ATController(mcx, rel, &stmt.cmds, recurse, lockmode, query_string, Some(tag))
}

// AlterTableInternal (tablecmds.c:4563).
pub fn AlterTableInternal<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    cmds: &NodeList<'mcx>,
    recurse: bool,
) -> PgResult<()> {
    let lockmode = AlterTableGetLockLevel(cmds);
    let rel = relation_seams::relation_open::call(mcx, relid, lockmode)?;
    event_trigger::EventTriggerAlterTableRelid(relid);
    ATController(mcx, rel, cmds, recurse, lockmode, "", None)
}

fn ATController<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    cmds: &NodeList<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
    query_string: &str,
    rewrite_tag: Option<types_core::CommandTag>,
) -> PgResult<()> {
    let mut wqueue: Wqueue<'mcx> = PgVec::new_in(mcx);
    for cnode in cmds.iter() {
        ATPrepCmd(mcx, &mut wqueue, &rel, cnode, recurse, false, lockmode, query_string)?;
    }
    rel.close(NoLock)?;

    ATRewriteCatalogs(mcx, &mut wqueue, lockmode, query_string)?;
    ATRewriteTables(mcx, &mut wqueue, lockmode, rewrite_tag)
}

// ATSimpleRecursion: prep-time recursion to all inheritors.
fn ATSimpleRecursion<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    if !recurse || !rel.rd_rel.relhassubclass {
        return Ok(());
    }
    let relid = rel.rd_id;
    let children = pg_inherits::find_all_inheritors(mcx, relid, lockmode)?;
    for &childrelid in children.iter() {
        if childrelid == relid {
            continue;
        }
        let childrel = table::table_open(mcx, childrelid, NoLock)?;
        catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
        ATPrepCmd(mcx, wqueue, &childrel, cnode, false, true, lockmode, query_string)?;
        childrel.close(NoLock)?;
    }
    Ok(())
}

fn ATCheckPartitionsNotInUse<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        let inh = pg_inherits::find_all_inheritors(mcx, rel.rd_id, lockmode)?;
        for &childoid in inh.iter().skip(1) {
            let childrel = table::table_open(mcx, childoid, NoLock)?;
            catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
            childrel.close(NoLock)?;
        }
    }
    Ok(())
}

const ATT_TABLE: i32 = 0x0001;
const ATT_VIEW: i32 = 0x0002;
const ATT_MATVIEW: i32 = 0x0004;
const ATT_INDEX: i32 = 0x0008;
const ATT_COMPOSITE_TYPE: i32 = 0x0010;
const ATT_FOREIGN_TABLE: i32 = 0x0020;
const ATT_PARTITIONED_INDEX: i32 = 0x0040;
const ATT_SEQUENCE: i32 = 0x0080;
const ATT_PARTITIONED_TABLE: i32 = 0x0100;

// The ATSimplePermissions allowed_targets per ATPrepCmd case arm; None for
// arms C leaves unchecked (AT_ChangeOwner).
fn at_allowed_targets(subtype: AlterTableType) -> Option<i32> {
    use AlterTableType::*;
    Some(match subtype {
        AT_AddColumn | AT_DropColumn | AT_AlterColumnType => {
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE
        }
        AT_AddColumnToView => ATT_VIEW,
        AT_ColumnDefault | AT_AddIdentity | AT_SetIdentity | AT_DropIdentity => {
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE
        }
        AT_CookedColumnDefault
        | AT_DropNotNull
        | AT_SetNotNull
        | AT_SetExpression
        | AT_DropExpression
        | AT_AddConstraint
        | AT_DropConstraint
        | AT_ValidateConstraint
        | AT_DropOids
        | AT_AddInherit
        | AT_DropInherit
        | AT_EnableTrig
        | AT_EnableAlwaysTrig
        | AT_EnableReplicaTrig
        | AT_EnableTrigAll
        | AT_EnableTrigUser
        | AT_DisableTrig
        | AT_DisableTrigAll
        | AT_DisableTrigUser => ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        AT_SetStatistics => {
            ATT_TABLE
                | ATT_PARTITIONED_TABLE
                | ATT_MATVIEW
                | ATT_INDEX
                | ATT_PARTITIONED_INDEX
                | ATT_FOREIGN_TABLE
        }
        AT_SetOptions | AT_ResetOptions | AT_SetStorage => {
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE
        }
        AT_SetCompression | AT_ClusterOn | AT_DropCluster | AT_SetAccessMethod
        | AT_ReplicaIdentity => ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW,
        AT_AddIndex | AT_AddIndexConstraint | AT_AlterConstraint => {
            ATT_TABLE | ATT_PARTITIONED_TABLE
        }
        AT_AlterColumnGenericOptions | AT_GenericOptions => ATT_FOREIGN_TABLE,
        AT_SetLogged | AT_SetUnLogged => ATT_TABLE | ATT_SEQUENCE,
        AT_SetTableSpace => {
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_PARTITIONED_INDEX
        }
        AT_SetRelOptions | AT_ResetRelOptions | AT_ReplaceRelOptions => {
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_MATVIEW | ATT_INDEX
        }
        AT_EnableRule | AT_EnableAlwaysRule | AT_EnableReplicaRule | AT_DisableRule | AT_AddOf
        | AT_DropOf | AT_EnableRowSecurity | AT_DisableRowSecurity | AT_ForceRowSecurity
        | AT_NoForceRowSecurity => ATT_TABLE | ATT_PARTITIONED_TABLE,
        AT_AttachPartition => ATT_PARTITIONED_TABLE | ATT_PARTITIONED_INDEX,
        AT_DetachPartition | AT_DetachPartitionFinalize => ATT_PARTITIONED_TABLE,
        AT_ChangeOwner | AT_ReAddIndex | AT_ReAddConstraint | AT_ReAddDomainConstraint
        | AT_ReAddComment | AT_ReAddStatistics => return None,
    })
}

fn alter_table_type_to_string(cmdtype: AlterTableType) -> Option<&'static str> {
    use AlterTableType::*;
    Some(match cmdtype {
        AT_AddColumn | AT_AddColumnToView => "ADD COLUMN",
        AT_ColumnDefault | AT_CookedColumnDefault => "ALTER COLUMN ... SET DEFAULT",
        AT_DropNotNull => "ALTER COLUMN ... DROP NOT NULL",
        AT_SetNotNull => "ALTER COLUMN ... SET NOT NULL",
        AT_SetExpression => "ALTER COLUMN ... SET EXPRESSION",
        AT_DropExpression => "ALTER COLUMN ... DROP EXPRESSION",
        AT_SetStatistics => "ALTER COLUMN ... SET STATISTICS",
        AT_SetOptions => "ALTER COLUMN ... SET",
        AT_ResetOptions => "ALTER COLUMN ... RESET",
        AT_SetStorage => "ALTER COLUMN ... SET STORAGE",
        AT_SetCompression => "ALTER COLUMN ... SET COMPRESSION",
        AT_DropColumn => "DROP COLUMN",
        AT_AddConstraint | AT_ReAddConstraint | AT_ReAddDomainConstraint
        | AT_AddIndexConstraint => "ADD CONSTRAINT",
        AT_AlterConstraint => "ALTER CONSTRAINT",
        AT_ValidateConstraint => "VALIDATE CONSTRAINT",
        AT_DropConstraint => "DROP CONSTRAINT",
        AT_AlterColumnType => "ALTER COLUMN ... SET DATA TYPE",
        AT_AlterColumnGenericOptions => "ALTER COLUMN ... OPTIONS",
        AT_ChangeOwner => "OWNER TO",
        AT_ClusterOn => "CLUSTER ON",
        AT_DropCluster => "SET WITHOUT CLUSTER",
        AT_SetAccessMethod => "SET ACCESS METHOD",
        AT_SetLogged => "SET LOGGED",
        AT_SetUnLogged => "SET UNLOGGED",
        AT_DropOids => "SET WITHOUT OIDS",
        AT_SetTableSpace => "SET TABLESPACE",
        AT_SetRelOptions => "SET",
        AT_ResetRelOptions => "RESET",
        AT_EnableTrig => "ENABLE TRIGGER",
        AT_EnableAlwaysTrig => "ENABLE ALWAYS TRIGGER",
        AT_EnableReplicaTrig => "ENABLE REPLICA TRIGGER",
        AT_DisableTrig => "DISABLE TRIGGER",
        AT_EnableTrigAll => "ENABLE TRIGGER ALL",
        AT_DisableTrigAll => "DISABLE TRIGGER ALL",
        AT_EnableTrigUser => "ENABLE TRIGGER USER",
        AT_DisableTrigUser => "DISABLE TRIGGER USER",
        AT_EnableRule => "ENABLE RULE",
        AT_EnableAlwaysRule => "ENABLE ALWAYS RULE",
        AT_EnableReplicaRule => "ENABLE REPLICA RULE",
        AT_DisableRule => "DISABLE RULE",
        AT_AddInherit => "INHERIT",
        AT_DropInherit => "NO INHERIT",
        AT_AddOf => "OF",
        AT_DropOf => "NOT OF",
        AT_ReplicaIdentity => "REPLICA IDENTITY",
        AT_EnableRowSecurity => "ENABLE ROW SECURITY",
        AT_DisableRowSecurity => "DISABLE ROW SECURITY",
        AT_ForceRowSecurity => "FORCE ROW SECURITY",
        AT_NoForceRowSecurity => "NO FORCE ROW SECURITY",
        AT_GenericOptions => "OPTIONS",
        AT_AttachPartition => "ATTACH PARTITION",
        AT_DetachPartition => "DETACH PARTITION",
        AT_DetachPartitionFinalize => "DETACH PARTITION ... FINALIZE",
        AT_AddIdentity => "ALTER COLUMN ... ADD IDENTITY",
        AT_SetIdentity => "ALTER COLUMN ... SET",
        AT_DropIdentity => "ALTER COLUMN ... DROP IDENTITY",
        AT_AddIndex | AT_ReAddIndex | AT_ReAddComment | AT_ReplaceRelOptions
        | AT_ReAddStatistics => return None,
    })
}

fn ATSimplePermissions(
    cmdtype: AlterTableType,
    rel: &Relation<'_>,
    allowed_targets: i32,
) -> PgResult<()> {
    let actual_target = match rel.rd_rel.relkind {
        RELKIND_RELATION => ATT_TABLE,
        types_rel::RELKIND_PARTITIONED_TABLE => ATT_PARTITIONED_TABLE,
        types_rel::RELKIND_VIEW => ATT_VIEW,
        types_rel::RELKIND_MATVIEW => ATT_MATVIEW,
        types_rel::RELKIND_INDEX => ATT_INDEX,
        types_rel::RELKIND_PARTITIONED_INDEX => ATT_PARTITIONED_INDEX,
        types_rel::RELKIND_COMPOSITE_TYPE => ATT_COMPOSITE_TYPE,
        types_rel::RELKIND_FOREIGN_TABLE => ATT_FOREIGN_TABLE,
        types_rel::RELKIND_SEQUENCE => ATT_SEQUENCE,
        _ => 0,
    };
    if actual_target & allowed_targets == 0 {
        let Some(action_str) = alter_table_type_to_string(cmdtype) else {
            panic!("invalid ALTER action attempted on relation \"{}\"", rel.name());
        };
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "ALTER action {action_str} cannot be performed on relation \"{}\"",
                    rel.name()
                ),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(pg_class_seams::errdetail_relkind_not_supported::call(
                rel.rd_rel.relkind,
            )?),
        ));
    }
    if !aclchk::object_ownercheck(RELATION_RELATION_ID, rel.rd_id, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(rel.rd_rel.relkind),
            rel.name(),
        )?;
    }
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(rel) {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied: \"{}\" is a system catalog", rel.name()),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn ATPrepCmd<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    // C copyObject boundary (tablecmds.c:4934): each table scribbles on its
    // own copy of the subcommand.
    let cnode = copyfuncs::copy_object(mcx, cnode)?;
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    // Only DETACH FINALIZE may run on a partition pending detach
    // (tablecmds.c:4919-4926).
    if rel.rd_rel.relispartition
        && cmd.subtype != AlterTableType::AT_DetachPartitionFinalize
        && pg_inherits::PartitionHasPendingDetach(mcx, rel.rd_id)?
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot alter partition \"{}\" with an incomplete detach", rel.name()),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending \
                 detach operation.",
            ),
        ));
    }
    if let Some(allowed) = at_allowed_targets(cmd.subtype) {
        ATSimplePermissions(cmd.subtype, rel, allowed)?;
    }
    // Ownercheck backstop (view-ACL SEV-1 fix): must run here, not only at
    // RangeVar lookup — AlterTableInternal callers reach ATPrepCmd with no
    // prior ownercheck. Redundant with ATSimplePermissions' internal check
    // when a target mask exists; unconditional coverage is the point.
    if !aclchk::object_ownercheck(RELATION_RELATION_ID, rel.rd_id, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(rel.rd_rel.relkind),
            rel.name(),
        )?;
    }
    let tabidx = ATGetQueueEntry(mcx, wqueue, rel);
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
            if !recursing && rel_reloftype(rel.rd_id)? != InvalidOid {
                return Err(typed_table_err("cannot add column to typed table"));
            }
            if rel.rd_rel.relkind == types_rel::RELKIND_COMPOSITE_TYPE {
                ATTypedTableRecursion(mcx, wqueue, rel, cnode, cmd.behavior, lockmode, query_string)?;
            }
            set_recurse();
            AT_PASS_ADD_COL
        }
        AlterTableType::AT_DropColumn => {
            if !recursing && rel_reloftype(rel.rd_id)? != InvalidOid {
                return Err(typed_table_err("cannot drop column from typed table"));
            }
            if rel.rd_rel.relkind == types_rel::RELKIND_COMPOSITE_TYPE {
                ATTypedTableRecursion(mcx, wqueue, rel, cnode, cmd.behavior, lockmode, query_string)?;
            }
            set_recurse();
            AT_PASS_DROP
        }
        AlterTableType::AT_ColumnDefault => {
            ATSimpleRecursion(mcx, wqueue, rel, cnode, recurse, lockmode, query_string)?;
            if cmd.def.is_some() { AT_PASS_ADD_OTHERCONSTR } else { AT_PASS_DROP }
        }
        AlterTableType::AT_DropNotNull => {
            set_recurse();
            AT_PASS_DROP
        }
        AlterTableType::AT_SetNotNull => {
            set_recurse();
            AT_PASS_COL_ATTRS
        }
        AlterTableType::AT_AddConstraint => {
            ATPrepAddPrimaryKey(mcx, &mut wqueue[tabidx], rel, cmd, recurse, lockmode)?;
            if recurse {
                // Recurses at exec time; lock descendants now.
                pg_inherits::find_all_inheritors(mcx, rel.rd_id, lockmode)?;
            }
            set_recurse();
            AT_PASS_ADD_CONSTR
        }
        AlterTableType::AT_DropConstraint => {
            ATCheckPartitionsNotInUse(mcx, rel, lockmode)?;
            set_recurse();
            AT_PASS_DROP
        }
        // Recursion occurs during execution.
        AlterTableType::AT_AlterConstraint => {
            set_recurse();
            AT_PASS_MISC
        }
        // Recursion occurs during execution.
        AlterTableType::AT_ValidateConstraint => {
            set_recurse();
            AT_PASS_MISC
        }
        AlterTableType::AT_AlterColumnType => {
            // ATParseTransformCmd: the identity ALTER SEQUENCE ... AS retype
            // runs before prep (C executes beforeStmts here).
            let relname = rel.name().to_string();
            let cxt = parse_utilcmd::transformAlterTableCmd(mcx, rel, &relname, cnode, query_string)?;
            run_seq_stmts(mcx, &cxt.blist)?;
            debug_assert!(cxt.alist.is_nil());
            debug_assert!(cxt.ckconstraints.is_nil() && cxt.nnconstraints.is_nil());
            debug_assert!(cxt.ixstmts.is_nil() && cxt.fkconstraints.is_nil());
            ATPrepAlterColumnType(
                mcx,
                wqueue,
                tabidx,
                rel,
                recurse,
                recursing,
                cnode,
                lockmode,
                query_string,
            )?;
            AT_PASS_ALTER_TYPE
        }
        AlterTableType::AT_SetExpression => {
            ATSimpleRecursion(mcx, wqueue, rel, cnode, recurse, lockmode, query_string)?;
            AT_PASS_SET_EXPRESSION
        }
        AlterTableType::AT_DropExpression => {
            ATSimpleRecursion(mcx, wqueue, rel, cnode, recurse, lockmode, query_string)?;
            ATPrepDropExpression(mcx, rel, cmd, recurse, recursing, lockmode)?;
            AT_PASS_DROP
        }
        AlterTableType::AT_CookedColumnDefault => AT_PASS_ADD_OTHERCONSTR,
        AlterTableType::AT_AddIdentity => {
            set_recurse();
            AT_PASS_ADD_OTHERCONSTR
        }
        AlterTableType::AT_SetIdentity => {
            set_recurse();
            // C: after AddIdentity, so MISC.
            AT_PASS_MISC
        }
        AlterTableType::AT_DropIdentity => {
            set_recurse();
            AT_PASS_DROP
        }
        AlterTableType::AT_ClusterOn | AlterTableType::AT_DropCluster => AT_PASS_MISC,
        AlterTableType::AT_ChangeOwner => AT_PASS_MISC,
        AlterTableType::AT_SetLogged | AlterTableType::AT_SetUnLogged => {
            if wqueue[tabidx].chg_persistence {
                return Err(Box::new(
                    PgError::new(ERROR, "cannot change persistence setting twice".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            ATPrepChangePersistence(
                mcx,
                &mut wqueue[tabidx],
                rel,
                cmd.subtype == AlterTableType::AT_SetLogged,
            )?;
            AT_PASS_MISC
        }
        AlterTableType::AT_EnableRule
        | AlterTableType::AT_EnableAlwaysRule
        | AlterTableType::AT_EnableReplicaRule
        | AlterTableType::AT_DisableRule => AT_PASS_MISC,
        AlterTableType::AT_EnableTrig
        | AlterTableType::AT_EnableAlwaysTrig
        | AlterTableType::AT_EnableReplicaTrig
        | AlterTableType::AT_EnableTrigAll
        | AlterTableType::AT_EnableTrigUser
        | AlterTableType::AT_DisableTrig
        | AlterTableType::AT_DisableTrigAll
        | AlterTableType::AT_DisableTrigUser => {
            set_recurse();
            AT_PASS_MISC
        }
        AlterTableType::AT_EnableRowSecurity
        | AlterTableType::AT_DisableRowSecurity
        | AlterTableType::AT_ForceRowSecurity
        | AlterTableType::AT_NoForceRowSecurity => AT_PASS_MISC,
        AlterTableType::AT_SetStatistics => {
            ATSimpleRecursion(mcx, wqueue, rel, cnode, recurse, lockmode, query_string)?;
            AT_PASS_MISC
        }
        AlterTableType::AT_SetStorage => {
            ATSimpleRecursion(mcx, wqueue, rel, cnode, recurse, lockmode, query_string)?;
            AT_PASS_MISC
        }
        // These commands never recurse; no command-specific prep.
        AlterTableType::AT_SetCompression
        | AlterTableType::AT_SetOptions
        | AlterTableType::AT_ResetOptions => AT_PASS_MISC,
        AlterTableType::AT_SetRelOptions
        | AlterTableType::AT_ResetRelOptions
        | AlterTableType::AT_ReplaceRelOptions => AT_PASS_MISC,
        // ATPrepAddColumn recursion is a no-op for views (no children).
        AlterTableType::AT_AddColumnToView => AT_PASS_ADD_COL,
        AlterTableType::AT_AttachPartition
        | AlterTableType::AT_DetachPartition
        | AlterTableType::AT_DetachPartitionFinalize => AT_PASS_MISC,
        AlterTableType::AT_AddInherit => {
            ATPrepAddInherit(rel)?;
            AT_PASS_MISC
        }
        AlterTableType::AT_DropInherit => AT_PASS_MISC,
        AlterTableType::AT_DropOids => AT_PASS_DROP,
        // These commands never recurse; no command-specific prep.
        AlterTableType::AT_ReplicaIdentity
        | AlterTableType::AT_AddOf
        | AlterTableType::AT_DropOf => AT_PASS_MISC,
        AlterTableType::AT_AddIndexConstraint => AT_PASS_ADD_INDEXCONSTR,
        AlterTableType::AT_SetTableSpace => {
            ATPrepSetTableSpace(mcx, &mut wqueue[tabidx], cmd.name.expect("SET TABLESPACE name"))?;
            AT_PASS_MISC
        }
        AlterTableType::AT_SetAccessMethod => {
            if wqueue[tabidx].chg_access_method {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot have multiple SET ACCESS METHOD subcommands".to_string(),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            ATPrepSetAccessMethod(&mut wqueue[tabidx], rel, cmd.name)?;
            AT_PASS_MISC
        }
        // ATSimplePermissions(ATT_FOREIGN_TABLE) ran above; never recurses.
        AlterTableType::AT_GenericOptions | AlterTableType::AT_AlterColumnGenericOptions => {
            AT_PASS_MISC
        }
        // unported: remaining ATPrepCmd subcommand arms
        _ => {
            return Err(Box::new(
                PgError::new(ERROR, "this form of ALTER TABLE is not supported yet".to_string())
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ))
        }
    };
    wqueue[tabidx].subcmds[pass].lappend(mcx, cnode)?;
    Ok(())
}

// ATPrepAddInherit (tablecmds.c:17239).
fn ATPrepAddInherit(rel: &Relation<'_>) -> PgResult<()> {
    if rel_reloftype(rel.rd_id)? != InvalidOid {
        return Err(typed_table_err("cannot change inheritance of typed table"));
    }
    if rel.rd_rel.relispartition {
        return Err(Box::new(
            PgError::new(ERROR, "cannot change inheritance of a partition".to_string())
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot change inheritance of partitioned table".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    Ok(())
}

// pg_class.reloftype probe; C reads rd_rel->reloftype (the trimmed
// FormData_pg_class here does not carry it).
pub(crate) fn rel_reloftype(relid: Oid) -> PgResult<Oid> {
    Ok(syscache_seams::pg_class_reloftype::call(relid)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}")))
}

#[cold]
#[inline(never)]
pub(crate) fn typed_table_err(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string())
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
    )
}

fn ATRewriteCatalogs<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    for pass in 0..AT_NUM_PASSES {
        let mut tabidx = 0;
        while tabidx < wqueue.len() {
        if wqueue[tabidx].subcmds[pass].is_nil() {
            tabidx += 1;
            continue;
        }
        let mut nodes: mcx::PgVec<'_, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        for c in wqueue[tabidx].subcmds[pass].iter() {
            nodes.push(c);
        }
        for &cnode in nodes.iter() {
            let mut rel = relation_seams::relation_open::call(mcx, wqueue[tabidx].relid, NoLock)?;
            let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
            match cmd.subtype {
                AlterTableType::AT_AddColumn | AlterTableType::AT_AddColumnToView => {
                    ATExecAddColumn(
                        mcx,
                        wqueue,
                        tabidx,
                        &rel,
                        cnode,
                        cmd.recurse,
                        false,
                        lockmode,
                        query_string,
                    )?;
                }
                AlterTableType::AT_DropColumn => {
                    ATExecDropColumn(
                        mcx,
                        &rel,
                        cmd.name.expect("AT_DropColumn name"),
                        cmd.behavior,
                        cmd.recurse,
                        false,
                        cmd.missing_ok,
                        lockmode,
                        None,
                    )?;
                }
                AlterTableType::AT_ColumnDefault => {
                    ATExecColumnDefault(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_DropNotNull => {
                    ATExecDropNotNull(mcx, &rel, cmd, lockmode)?;
                }
                AlterTableType::AT_SetNotNull => {
                    let col_name = cmd.name.expect("AT_SetNotNull name");
                    ATExecSetNotNull(
                        mcx, wqueue, &rel, None, col_name, cmd.recurse, false, lockmode,
                    )?;
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
                        ConstrType::CONSTR_PRIMARY
                        | ConstrType::CONSTR_UNIQUE
                        | ConstrType::CONSTR_EXCLUSION => {
                            let (istmt, nnconstraints) =
                                parse_utilcmd::transformIndexConstraintForAlter(
                                    mcx, &rel, defnode, query_string,
                                )?;
                            // ATParseTransformCmd fabricates the stmt's
                            // relation with inh = recurse (tablecmds.c:5827);
                            // DefineIndex's partitioned ONLY arm reads it.
                            {
                                let nsp = lsyscache::get_namespace_name(
                                    mcx,
                                    rel.rd_rel.relnamespace,
                                )?
                                .unwrap_or_else(|| {
                                    panic!(
                                        "cache lookup failed for namespace {}",
                                        rel.rd_rel.relnamespace
                                    )
                                });
                                let rv = types_nodes::RangeVar {
                                    catalogname: None,
                                    schemaname: Some(str_in_mcx(mcx, nsp.as_str())?),
                                    relname: Some(str_in_mcx(mcx, rel.name())?),
                                    inh: cmd.recurse,
                                    relpersistence: rel.rd_rel.relpersistence,
                                    alias: None,
                                    location: -1,
                                };
                                let rv: &'mcx types_nodes::RangeVar<'mcx> =
                                    types_nodes::Node::mk_mut(mcx, rv)?.seal_ref();
                                // SAFETY: statement-owned parse tree; no
                                // derived refs live yet.
                                unsafe {
                                    istmt
                                        .with_mut::<types_nodes::rawnodes::IndexStmt, _>(
                                            |s| s.relation = Some(rv),
                                        )
                                        .expect("IndexStmt");
                                }
                            }
                            let is_existing = istmt
                                .as_variant::<types_nodes::rawnodes::IndexStmt>()
                                .expect("IndexStmt")
                                .indexOid
                                != InvalidOid;
                            if !is_existing {
                                parse_clause::transformIndexStmt(
                                    mcx,
                                    wqueue[tabidx].relid,
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
                                wqueue[tabidx].subcmds[AT_PASS_COL_ATTRS]
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
                            wqueue[tabidx].subcmds[target_pass].lappend(mcx, newcmd.seal())?;
                        }
                        ConstrType::CONSTR_NOTNULL if pass == AT_PASS_ADD_CONSTR => {
                            wqueue[tabidx].subcmds[AT_PASS_COL_ATTRS].lappend(mcx, cnode)?;
                        }
                        // ATParseTransformCmd (tablecmds.c): at ADD_CONSTR,
                        // non-index constraints are requeued for the
                        // ADD_OTHERCONSTR pass, after ADD_INDEX.
                        _ if pass == AT_PASS_ADD_CONSTR => {
                            wqueue[tabidx].subcmds[AT_PASS_ADD_OTHERCONSTR]
                                .lappend(mcx, cnode)?;
                        }
                        ConstrType::CONSTR_NOTNULL | ConstrType::CONSTR_CHECK => {
                            ATAddCheckNNConstraint(
                                mcx,
                                wqueue,
                                tabidx,
                                &rel,
                                defnode,
                                cmd.recurse,
                                false,
                                false,
                                lockmode,
                                query_string,
                            )?;
                        }
                        _ => ATExecAddConstraint(
                            mcx,
                            wqueue,
                            tabidx,
                            &rel,
                            cmd,
                            cmd.recurse,
                            query_string,
                            lockmode,
                        )?,
                    }
                }
                AlterTableType::AT_DropConstraint => {
                    ATExecDropConstraint(mcx, &rel, cmd, lockmode)?;
                }
                AlterTableType::AT_AlterConstraint => {
                    let cmdcon = cmd
                        .def
                        .expect("AT_AlterConstraint def")
                        .as_variant::<types_nodes::parsenodes::ATAlterConstraint>()
                        .expect("ATAlterConstraint");
                    crate::fk::ATExecAlterConstraint(
                        mcx, wqueue, &rel, cmdcon, cmd.recurse, lockmode,
                    )?;
                }
                AlterTableType::AT_ValidateConstraint => {
                    let name = cmd.name.expect("AT_ValidateConstraint name");
                    ATExecValidateConstraint(
                        mcx, wqueue, &rel, name, cmd.recurse, false, lockmode,
                    )?;
                }
                AlterTableType::AT_AddIndex => {
                    ATExecAddIndex(mcx, &mut wqueue[tabidx], &rel, cmd, false)?;
                }
                AlterTableType::AT_ReAddIndex => {
                    ATExecAddIndex(mcx, &mut wqueue[tabidx], &rel, cmd, true)?;
                }
                AlterTableType::AT_ReAddStatistics => {
                    // ATExecAddStatistics (tablecmds.c:9683); the stmt has
                    // been through transformStatsStmt. check_rights=false:
                    // C's rebuild arm (tablecmds.c:9693, !is_rebuild).
                    let stmt = cmd
                        .def
                        .expect("AT_ReAddStatistics CreateStatsStmt")
                        .as_variant::<types_nodes::rawnodes::CreateStatsStmt>()
                        .expect("CreateStatsStmt");
                    statscmds::CreateStatistics(mcx, stmt, false)?;
                }
                AlterTableType::AT_ReAddConstraint => {
                    let defnode = cmd.def.expect("AT_ReAddConstraint Constraint");
                    let constr = defnode.as_variant::<Constraint>().expect("Constraint");
                    match constr.contype {
                        ConstrType::CONSTR_NOTNULL | ConstrType::CONSTR_CHECK => {
                            ATAddCheckNNConstraint(
                                mcx,
                                wqueue,
                                tabidx,
                                &rel,
                                defnode,
                                true,
                                false,
                                true,
                                lockmode,
                                query_string,
                            )?;
                        }
                        _ => ATExecAddConstraint(
                            mcx,
                            wqueue,
                            tabidx,
                            &rel,
                            cmd,
                            true,
                            query_string,
                            lockmode,
                        )?,
                    }
                }
                AlterTableType::AT_ReAddDomainConstraint => {
                    let stmt = cmd
                        .def
                        .expect("AT_ReAddDomainConstraint AlterDomainStmt")
                        .as_variant::<types_nodes::parsenodes::AlterDomainStmt>()
                        .expect("AlterDomainStmt");
                    typecmds_seams::alter_domain_add_constraint::call(
                        mcx,
                        &stmt.typeName,
                        stmt.def.expect("ALTER DOMAIN ADD CONSTRAINT def"),
                    )?;
                }
                AlterTableType::AT_ReAddComment => {
                    let stmt = cmd
                        .def
                        .expect("AT_ReAddComment CommentStmt")
                        .as_variant::<types_nodes::parsenodes::CommentStmt>()
                        .expect("CommentStmt");
                    commands_comment::CommentObject(mcx, stmt)?;
                }
                AlterTableType::AT_AlterColumnType => {
                    ATExecAlterColumnType(mcx, &mut wqueue[tabidx], &rel, cmd)?;
                }
                AlterTableType::AT_SetExpression => {
                    ATExecSetExpression(mcx, &mut wqueue[tabidx], &rel, cmd)?;
                }
                AlterTableType::AT_DropExpression => {
                    ATExecDropExpression(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_EnableTrig
                | AlterTableType::AT_EnableAlwaysTrig
                | AlterTableType::AT_EnableReplicaTrig
                | AlterTableType::AT_EnableTrigAll
                | AlterTableType::AT_EnableTrigUser
                | AlterTableType::AT_DisableTrig
                | AlterTableType::AT_DisableTrigAll
                | AlterTableType::AT_DisableTrigUser => {
                    use types_trigger::{
                        TRIGGER_DISABLED, TRIGGER_FIRES_ALWAYS, TRIGGER_FIRES_ON_ORIGIN,
                        TRIGGER_FIRES_ON_REPLICA,
                    };
                    let (fires_when, skip_system, named) = match cmd.subtype {
                        AlterTableType::AT_EnableTrig => (TRIGGER_FIRES_ON_ORIGIN, false, true),
                        AlterTableType::AT_EnableAlwaysTrig => (TRIGGER_FIRES_ALWAYS, false, true),
                        AlterTableType::AT_EnableReplicaTrig => {
                            (TRIGGER_FIRES_ON_REPLICA, false, true)
                        }
                        AlterTableType::AT_DisableTrig => (TRIGGER_DISABLED, false, true),
                        AlterTableType::AT_EnableTrigAll => (TRIGGER_FIRES_ON_ORIGIN, false, false),
                        AlterTableType::AT_DisableTrigAll => (TRIGGER_DISABLED, false, false),
                        AlterTableType::AT_EnableTrigUser => (TRIGGER_FIRES_ON_ORIGIN, true, false),
                        _ => (TRIGGER_DISABLED, true, false),
                    };
                    let name = if named {
                        Some(cmd.name.expect("ENABLE/DISABLE TRIGGER has a name"))
                    } else {
                        None
                    };
                    trigger::EnableDisableTrigger(
                        mcx,
                        &rel,
                        name,
                        types_core::InvalidOid,
                        fires_when,
                        skip_system,
                        cmd.recurse,
                        types_rel::ShareRowExclusiveLock,
                    )?;
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
                AlterTableType::AT_SetCompression => {
                    ATExecSetCompression(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_SetOptions => {
                    ATExecSetOptions(mcx, &rel, cmd, false)?;
                }
                AlterTableType::AT_ResetOptions => {
                    ATExecSetOptions(mcx, &rel, cmd, true)?;
                }
                AlterTableType::AT_SetRelOptions
                | AlterTableType::AT_ResetRelOptions
                | AlterTableType::AT_ReplaceRelOptions => {
                    let empty = types_nodes::NodeList::nil();
                    let defs = cmd.def.and_then(|d| d.as_list()).unwrap_or(&empty);
                    crate::setrelopts::ATExecSetRelOptions(mcx, &rel, defs, cmd.subtype, lockmode)?;
                }
                AlterTableType::AT_AttachPartition => {
                    let pcmd = cmd
                        .def
                        .expect("AT_AttachPartition PartitionCmd")
                        .as_variant::<types_nodes::rawnodes::PartitionCmd>()
                        .expect("PartitionCmd");
                    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
                        crate::attach::ATExecAttachPartition(
                            mcx, wqueue, &rel, pcmd, query_string,
                        )?;
                    } else {
                        // transformPartitionCmd (parse_utilcmd.c:4239): ALTER
                        // TABLE grammar allows a bound on a partitioned index;
                        // a partitioned index cannot have one.
                        if pcmd.bound.is_some() {
                            return Err(Box::new(
                                PgError::new(
                                    ERROR,
                                    format!(
                                        "\"{}\" is not a partitioned table",
                                        rel.name()
                                    ),
                                )
                                .with_sqlstate(
                                    types_error::ERRCODE_INVALID_OBJECT_DEFINITION,
                                ),
                            ));
                        }
                        crate::attach::ATExecAttachPartitionIdx(
                            mcx,
                            &rel,
                            pcmd.name.expect("PartitionCmd.name"),
                        )?;
                    }
                }
                AlterTableType::AT_DetachPartition => {
                    let pcmd = cmd
                        .def
                        .expect("AT_DetachPartition PartitionCmd")
                        .as_variant::<types_nodes::rawnodes::PartitionCmd>()
                        .expect("PartitionCmd");
                    // Concurrent detach commits mid-command and reopens the
                    // parent; the returned handle carries the reopened rel.
                    rel = crate::attach::ATExecDetachPartition(
                        mcx, wqueue, rel, pcmd, query_string,
                    )?;
                }
                AlterTableType::AT_DetachPartitionFinalize => {
                    let pcmd = cmd
                        .def
                        .expect("AT_DetachPartitionFinalize PartitionCmd")
                        .as_variant::<types_nodes::rawnodes::PartitionCmd>()
                        .expect("PartitionCmd");
                    crate::attach::ATExecDetachPartitionFinalize(
                        mcx,
                        &rel,
                        pcmd.name.expect("PartitionCmd.name"),
                    )?;
                }
                AlterTableType::AT_AddInherit => {
                    ATExecAddInherit(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_DropInherit => {
                    ATExecDropInherit(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_AddIdentity => {
                    let relname = rel.name().to_string();
                    let cxt = parse_utilcmd::transformAlterTableCmd(mcx, &rel, &relname, cnode, query_string)?;
                    run_seq_stmts(mcx, &cxt.blist)?;
                    debug_assert!(cxt.alist.is_nil());
                    debug_assert!(cxt.ckconstraints.is_nil() && cxt.nnconstraints.is_nil());
                    debug_assert!(cxt.ixstmts.is_nil() && cxt.fkconstraints.is_nil());
                    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
                    ATExecAddIdentity(mcx, &rel, cmd, lockmode)?;
                }
                AlterTableType::AT_SetIdentity => {
                    let relname = rel.name().to_string();
                    let cxt = parse_utilcmd::transformAlterTableCmd(mcx, &rel, &relname, cnode, query_string)?;
                    run_seq_stmts(mcx, &cxt.blist)?;
                    debug_assert!(cxt.alist.is_nil());
                    debug_assert!(cxt.ckconstraints.is_nil() && cxt.nnconstraints.is_nil());
                    debug_assert!(cxt.ixstmts.is_nil() && cxt.fkconstraints.is_nil());
                    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
                    ATExecSetIdentity(mcx, &rel, cmd, lockmode)?;
                }
                AlterTableType::AT_DropIdentity => {
                    ATExecDropIdentity(
                        mcx,
                        &rel,
                        cmd.name.expect("AT_DropIdentity name"),
                        cmd.missing_ok,
                        lockmode,
                        cmd.recurse,
                        false,
                    )?;
                }
                AlterTableType::AT_ClusterOn => {
                    ATExecClusterOn(mcx, &rel, cmd, lockmode)?;
                }
                AlterTableType::AT_DropCluster => {
                    commands_cluster::mark_index_clustered(mcx, &rel, InvalidOid, false)?;
                }
                AlterTableType::AT_SetLogged | AlterTableType::AT_SetUnLogged => {}
                // Nothing to do here; oid columns don't exist anymore.
                AlterTableType::AT_DropOids => {}
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
                AlterTableType::AT_ChangeOwner => {
                    let newowner = cmd
                        .newowner
                        .expect("AT_ChangeOwner RoleSpec")
                        .as_role_spec()
                        .expect("RoleSpec");
                    crate::owner::ATExecChangeOwner(
                        mcx,
                        rel.rd_id,
                        aclchk::get_rolespec_oid(newowner, false)?,
                        false,
                        lockmode,
                    )?;
                }
                AlterTableType::AT_GenericOptions => {
                    if let Some(options) = cmd.def.and_then(|d| d.as_list()) {
                        ATExecGenericOptions(mcx, &rel, &options)?;
                        // Cached plans may depend on the old options.
                        inval::invalidate::CacheInvalidateRelcache(&rel)?;
                    }
                }
                AlterTableType::AT_AlterColumnGenericOptions => {
                    if let Some(options) = cmd.def.and_then(|d| d.as_list()) {
                        ATExecAlterColumnGenericOptions(
                            mcx,
                            &rel,
                            cmd.name.expect("AT_AlterColumnGenericOptions column name"),
                            &options,
                        )?;
                    }
                }
                // Phase-2 arm only fires for partitioned relkinds (no
                // storage); phase 3 does the work otherwise.
                AlterTableType::AT_SetTableSpace => {
                    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE
                        || rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_INDEX
                    {
                        ATExecSetTableSpaceNoStorage(
                            mcx,
                            &rel,
                            wqueue[tabidx].new_tablespace,
                        )?;
                    }
                }
                AlterTableType::AT_SetAccessMethod => {
                    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE
                        && wqueue[tabidx].chg_access_method
                    {
                        ATExecSetAccessMethodNoStorage(
                            mcx,
                            &rel,
                            wqueue[tabidx].new_access_method,
                        )?;
                    }
                }
            }
            // C threads each ATExec* return address; only the subcmd count is
            // observable through the ported SRF surface.
            event_trigger::EventTriggerCollectAlterTableSubcmd(
                pg_depend::ObjectAddress::set(RELATION_RELATION_ID, wqueue[tabidx].relid),
            );
            rel.close(NoLock)?;
            xact::CommandCounterIncrement()?;
        }
        if pass == AT_PASS_ALTER_TYPE || pass == AT_PASS_SET_EXPRESSION {
            ATPostAlterTypeCleanup(mcx, wqueue, tabidx)?;
        }
        tabidx += 1;
        }
    }
    // AlterTableCreateToastTable: a no-op when a toast table already exists
    // or none is needed; opened with the statement lockmode (the AEL open in
    // NewRelationCreateToastTable blocked SUE-level VALIDATE behind writers);
    // partitioned parents and ATTACH source tabs skip per tablecmds.c:5364-5368.
    for tabidx in 0..wqueue.len() {
        let tab = &wqueue[tabidx];
        if ((tab.relkind == RELKIND_RELATION
            || tab.relkind == types_rel::RELKIND_PARTITIONED_TABLE)
            && tab.partition_constraint.is_none())
            || tab.relkind == types_rel::RELKIND_MATVIEW
        {
            catalog_toasting::AlterTableCreateToastTable(mcx, tab.relid, None, lockmode)?;
        }
    }
    Ok(())
}

fn ATRewriteTables<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    lockmode: LOCKMODE,
    rewrite_tag: Option<types_core::CommandTag>,
) -> PgResult<()> {
    for tabidx in 0..wqueue.len() {
        ATRewriteTableOne(mcx, &mut wqueue[tabidx], lockmode, rewrite_tag)?;
    }
    for tab in wqueue.iter() {
        run_seq_stmts(mcx, &tab.after_stmts)?;
    }
    Ok(())
}

fn ATRewriteTableOne<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    lockmode: LOCKMODE,
    rewrite_tag: Option<types_core::CommandTag>,
) -> PgResult<()> {
    if !types_rel::RELKIND_HAS_STORAGE(tab.relkind) {
        return Ok(());
    }
    if !tab.newvals.is_empty() || tab.rewrite > 0 {
        let rel = table::table_open(mcx, tab.relid, NoLock)?;
        find_composite_type_dependencies_rel(mcx, rel.rd_rel.reltype, &rel)?;
        rel.close(NoLock)?;
    }
    if tab.rewrite > 0 && tab.relkind != types_rel::RELKIND_SEQUENCE {
        if tab.rewrite
            & !(AT_REWRITE_COLUMN_REWRITE
                | AT_REWRITE_DEFAULT_VAL
                | AT_REWRITE_ALTER_PERSISTENCE
                | AT_REWRITE_ACCESS_METHOD)
            != 0
        {
            unported("ATRewriteTable rewrite flags");
        }
        let old_heap = table::table_open(mcx, tab.relid, NoLock)?;
        if catalog::IsSystemRelation(&old_heap) {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot rewrite system relation \"{}\"", old_heap.name()),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if old_heap.rd_options.as_ref().and_then(|o| o.std()).is_some_and(|o| o.user_catalog_table)
        {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cannot rewrite table \"{}\" used as a catalog table",
                        old_heap.name()
                    ),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        let persistence = if tab.chg_persistence {
            tab.newrelpersistence
        } else {
            old_heap.rd_rel.relpersistence
        };
        let new_tablespace = if tab.new_tablespace != InvalidOid {
            tab.new_tablespace
        } else {
            old_heap.rd_rel.reltablespace
        };
        let new_access_method = if tab.chg_access_method {
            tab.new_access_method
        } else {
            old_heap.rd_rel.relam
        };
        old_heap.close(NoLock)?;
        // Fire the table_rewrite event trigger before rewriting; parsetree is
        // NULL (no tag) when coming from AlterTableInternal, and it fires
        // only once (tablecmds.c:5950-5964).
        if let Some(tag) = rewrite_tag {
            event_trigger::EventTriggerTableRewrite(mcx, tag, tab.relid, tab.rewrite)?;
        }
        let oid_new_heap = commands_cluster::make_new_heap(
            mcx,
            tab.relid,
            new_tablespace,
            new_access_method,
            persistence,
            lockmode,
        )?;
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
    } else if tab.rewrite > 0 {
        if tab.chg_persistence {
            sequence_seams::sequence_change_persistence::call(
                mcx,
                tab.relid,
                tab.newrelpersistence,
            )?;
        }
    } else {
        if !tab.constraints.is_empty()
            || tab.verify_new_notnull
            || tab.partition_constraint.is_some()
        {
            ATRewriteTable(mcx, tab, InvalidOid)?;
        }
        if tab.new_tablespace != InvalidOid {
            ATExecSetTableSpace(mcx, tab.relid, tab.new_tablespace, lockmode)?;
        }
    }
    if tab.chg_persistence {
        for &seq_relid in pg_depend::getOwnedSequences(mcx, tab.relid)?.iter() {
            sequence_seams::sequence_change_persistence::call(
                mcx,
                seq_relid,
                tab.newrelpersistence,
            )?;
        }
    }

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
        // C verifies the qual with virtual generated columns expanded
        // (tablecmds.c:6193).
        let qual =
            planner::prepjointree::expand_generated_columns_in_expr(mcx, con.qual, &oldrel, 1)?;
        // ExecPrepareExpr: expression_planner + init.
        let planned = clauses::eval_const_expressions(mcx, qual)?;
        let mut state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("check constraint expr");
        state.arm_result_mcx(mcx);
        con_states.push((i, state));
    }
    let mut newval_states: PgVec<
        'mcx,
        (AttrNumber, bool, mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>),
    > = PgVec::new_in(mcx);
    for nv in tab.newvals.iter() {
        let mut state = execexpr::exec_init_expr(mcx, Some(nv.expr), execexpr::ParamBind::NONE)?
            .expect("transform expr");
        // By-ref transform results land in the statement arena (C resets a
        // per-tuple context each row; watch memory on huge rewrites).
        state.arm_result_mcx(mcx);
        newval_states.push((nv.attnum, nv.is_generated, state));
    }

    let mut partqualstate = match tab.partition_constraint {
        Some(q) => {
            needscan = true;
            let planned = clauses::eval_const_expressions(mcx, q)?;
            let mut state =
                execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
                    .expect("partition constraint expr");
            // Expression partition keys call by-ref-returning functions
            // (same statement-arena caveat as the transform states above).
            state.arm_result_mcx(mcx);
            Some(state)
        }
        None => None,
    };

    let mut notnull_attrs: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);
    let mut notnull_virtual_attrs: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);
    if newrel.is_some() || tab.verify_new_notnull {
        // C reads CompactAttribute.attnullability: invalid not-null
        // constraints are not verified.
        for i in 0..new_tupdesc.natts as usize {
            let att = new_tupdesc.attr(i);
            if new_tupdesc.compact_attr(i).attnullability == ATTNULLABLE_VALID
                && !att.attisdropped
            {
                if att.attgenerated == b'v' as i8 {
                    notnull_virtual_attrs.push(att.attnum);
                } else {
                    notnull_attrs.push(att.attnum);
                }
            }
        }
        if !notnull_attrs.is_empty() || !notnull_virtual_attrs.is_empty() {
            needscan = true;
        }
    }

    // ExecRelGenVirtualNotNull (execMain.c), reimplemented locally: a
    // resolved-once NullTest over each virtual column's generation
    // expression (tablecmds cannot dep nodemodifytable — cycle).
    let mut nn_virtual_states: PgVec<
        'mcx,
        (AttrNumber, mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>),
    > = PgVec::new_in(mcx);
    for &attnum in notnull_virtual_attrs.iter() {
        let genexpr = rewrite_handler::build_generation_expression(mcx, &oldrel, attnum as usize)?;
        let nulltest = Node::mk(
            mcx,
            types_nodes::primnodes::NullTest {
                arg: Some(genexpr),
                nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                argisrow: false,
                location: -1,
            },
        )?;
        let planned = clauses::eval_const_expressions(mcx, nulltest)?;
        let mut state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("virtual not-null test expr");
        state.arm_result_mcx(mcx);
        nn_virtual_states.push((attnum, state));
    }

    if newrel.is_some() || needscan {
        let relname = oldrel.name().to_string();
        if newrel.is_some() {
            elog_seams::ereport::call(PgError::new(
                types_error::DEBUG1,
                format!("rewriting table \"{relname}\""),
            ))?;
        } else {
            elog_seams::ereport::call(PgError::new(
                types_error::DEBUG1,
                format!("verifying table \"{relname}\""),
            ))?;
        }
        if newrel.is_some() {
            // Tuples move during rewrite; tuple/page SIREAD locks must be
            // promoted to a relation lock on the old heap.
            predicate_seams::transfer_predicate_locks_to_heap_relation::call(&oldrel)?;
        }
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
            for (attnum, state) in nn_virtual_states.iter_mut() {
                let mut slots = execexpr::EvalSlots {
                    scan: Some(insertslot),
                    inner: None,
                    outer: None,
                };
                let r = execexpr::exec_eval_expr(state, &mut slots)?;
                // ExecCheck semantics: NULL results pass.
                if !r.isnull && !r.value.as_bool() {
                    let att = new_tupdesc.attr(*attnum as usize - 1);
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

            if let Some(state) = partqualstate.as_mut() {
                let mut slots = execexpr::EvalSlots {
                    scan: Some(insertslot),
                    inner: None,
                    outer: None,
                };
                let r = execexpr::exec_eval_expr(state, &mut slots)?;
                if !r.isnull && !r.value.as_bool() {
                    let msg = if tab.validate_default {
                        format!(
                            "updated partition constraint for default partition \"{relname}\" \
                             would be violated by some row"
                        )
                    } else {
                        format!(
                            "partition constraint of relation \"{relname}\" is violated by \
                             some row"
                        )
                    };
                    return Err(Box::new(
                        PgError::new(ERROR, msg).with_sqlstate(ERRCODE_CHECK_VIOLATION),
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

#[allow(clippy::too_many_arguments)]
fn ATExecAddColumn<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    tabidx: usize,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    let myrelid = rel.rd_id;
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    let if_not_exists = cmd.missing_ok;
    let defnode = cmd.def.expect("AT_AddColumn ColumnDef");
    let col_def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
    let colname = col_def.colname.expect("ColumnDef.colname");
    let relname = rel.name().to_string();

    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_AddColumn,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }
    if rel.rd_rel.relispartition && !recursing {
        return Err(Box::new(
            PgError::new(ERROR, "cannot add column to a partition".to_string())
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    if col_def.inhcount > 0 {
        if let Some((childattnum, childinhcount)) =
            attname_lookup(mcx, myrelid, colname, false)?
        {
            let childatt = *rel.rd_att.attr(childattnum as usize - 1);
            let tn = col_def
                .typeName
                .expect("ColumnDef.typeName")
                .as_variant::<TypeName>()
                .expect("TypeName");
            let (ctype_id, ctypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, tn)?;
            if ctype_id != childatt.atttypid || ctypmod != childatt.atttypmod {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "child table \"{relname}\" has different type for column \
                             \"{colname}\""
                        ),
                    )
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
                ));
            }
            let ccollid = crate::GetColumnDefCollation(col_def, ctype_id)?;
            if ccollid != childatt.attcollation {
                let collname = |oid| {
                    lsyscache::misc::get_collation_name(mcx, oid)
                        .ok()
                        .flatten()
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default()
                };
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "child table \"{relname}\" has different collation for column \
                             \"{colname}\""
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_COLLATION_MISMATCH)
                    .with_detail(format!(
                        "\"{}\" versus \"{}\"",
                        collname(ccollid),
                        collname(childatt.attcollation)
                    )),
                ));
            }
            if childinhcount == i16::MAX {
                return Err(Box::new(
                    PgError::new(ERROR, "too many inheritance parents".to_string())
                        .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                ));
            }
            update_pg_attribute(
                mcx,
                myrelid,
                childattnum,
                &[(Anum_pg_attribute_attinhcount, Datum::from_i16(childinhcount + 1))],
            )?;
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "merging definition of column \"{colname}\" for child \"{relname}\""
                ),
                None,
            )?;
            xact::CommandCounterIncrement()?;
            return Ok(());
        }
    }

    if !check_for_column_name_collision(mcx, myrelid, &relname, colname, if_not_exists)? {
        return Ok(());
    }

    if !recursing && cmd.subtype != AlterTableType::AT_AddColumnToView {
        let cxt = parse_utilcmd::transformAlterTableCmd(mcx, rel, &relname, cnode, query_string)?;
        // ATParseTransformCmd (tablecmds.c:5738-5745): serial/identity
        // CreateSeqStmts run before the subcommand; the AlterSeqStmts wait
        // in tab->afterStmts until the end of phase 3 (tablecmds.c:6103).
        run_seq_stmts(mcx, &cxt.blist)?;
        for s in cxt.alist.iter() {
            wqueue[tabidx].after_stmts.lappend(mcx, s)?;
        }
        // transformAlterTableStmt folds IndexStmts into AT_AddIndex (or
        // AT_AddIndexConstraint for USING INDEX) subcommands after running
        // transformIndexStmt (parse_utilcmd.c:3817-3838); ATParseTransformCmd
        // schedules them (tablecmds.c:5765-5771).
        for istmt in cxt.ixstmts.iter() {
            parse_clause::transformIndexStmt(mcx, wqueue[tabidx].relid, istmt, query_string)?;
            let is_existing = istmt
                .as_variant::<types_nodes::rawnodes::IndexStmt>()
                .expect("IndexStmt")
                .indexOid
                != InvalidOid;
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
            wqueue[tabidx].subcmds[target_pass].lappend(mcx, newcmd.seal())?;
        }
        // ATParseTransformCmd: generated AT_AddConstraint subcommands are
        // scheduled into later passes of the same wqueue entry.
        for def in cxt
            .ckconstraints
            .iter()
            .chain(cxt.nnconstraints.iter())
            .chain(cxt.fkconstraints.iter())
        {
            let constr = def.as_variant::<Constraint>().expect("Constraint");
            let target_pass = match constr.contype {
                ConstrType::CONSTR_NOTNULL => AT_PASS_COL_ATTRS,
                ConstrType::CONSTR_PRIMARY
                | ConstrType::CONSTR_UNIQUE
                | ConstrType::CONSTR_EXCLUSION => AT_PASS_ADD_INDEXCONSTR,
                _ => AT_PASS_ADD_OTHERCONSTR,
            };
            let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
            newcmd.subtype = AlterTableType::AT_AddConstraint;
            newcmd.recurse = recurse;
            newcmd.def = Some(def);
            wqueue[tabidx].subcmds[target_pass].lappend(mcx, newcmd.seal())?;
        }
    }
    let col_def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");

    // tablecmds.c:7346-7356: regular inheritance children do not inherit
    // identity; partitions do.
    if col_def.identity != 0
        && recurse
        && rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE
        && !pg_inherits::find_inheritance_children(mcx, myrelid, NoLock)?.is_empty()
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot recursively add identity column to table that has child tables"
                    .to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }

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
    let attribute = tupdesc.attrs[0];
    {
        let attname = core::str::from_utf8(attribute.attname.name_str())
            .expect("non-UTF-8 attname");
        let mut rowtypes: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
        rowtypes.push(rel.rd_rel.reltype);
        catalog_heap::CheckAttributeType(
            mcx,
            attname,
            attribute.atttypid,
            attribute.attcollation,
            &mut rowtypes,
            if attribute.attgenerated == b'v' as i8 { catalog_heap::CHKATYPE_IS_VIRTUAL } else { 0 },
        )?;
    }

    let attrdesc = table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    catalog_heap::InsertPgAttributeTuples(
        mcx,
        &attrdesc,
        core::slice::from_ref(&attribute),
        myrelid,
        None,
        None,
    )?;
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
        // relation_open, not table_open: C's rel may be a composite type.
        let rel2 = relation_seams::relation_open::call(mcx, myrelid, NoLock)?;
        crate::constraints::add_relation_new_constraints(
            mcx,
            &rel2,
            &[(newattnum as AttrNumber, raw_default, col_def.generated)],
            &NodeList::nil(),
            None,
        )?;
        rel2.close(NoLock)?;
        xact::CommandCounterIncrement()?;
    }

    // Relations without storage skip the phase-3 fill decision entirely.
    // relation_open, not table_open: C's rel may be a composite type.
    let rel3 = relation_seams::relation_open::call(mcx, myrelid, NoLock)?;
    if types_rel::RELKIND_HAS_STORAGE(rel3.rd_rel.relkind) {
        add_column_phase3_fill(
            mcx,
            &mut wqueue[tabidx],
            &rel3,
            newattnum as AttrNumber,
            col_def,
            &attribute,
        )?;
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

    let children = pg_inherits::find_inheritance_children(mcx, myrelid, lockmode)?;
    if !children.is_empty() && !recurse {
        return Err(Box::new(
            PgError::new(ERROR, "column must be added to child tables too".to_string())
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    // Children see the column as singly inherited; the clone mirrors C's
    // copyObject so the parent's queued cmd stays untouched.
    let childcmd = if !recursing {
        let copy = copyfuncs::copy_object(mcx, cnode)?;
        let copy_def = copy
            .as_variant::<AlterTableCmd>()
            .expect("AlterTableCmd")
            .def
            .expect("AT_AddColumn ColumnDef");
        // SAFETY: freshly copied tree; no derived refs live.
        unsafe {
            copy_def
                .with_mut::<ColumnDef, _>(|d| {
                    d.inhcount = 1;
                    d.is_local = false;
                })
                .expect("ColumnDef");
        }
        copy
    } else {
        cnode
    };
    for &childrelid in children.iter() {
        let childrel = table::table_open(mcx, childrelid, NoLock)?;
        catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
        let childtabidx = ATGetQueueEntry(mcx, wqueue, &childrel);
        ATExecAddColumn(
            mcx,
            wqueue,
            childtabidx,
            &childrel,
            childcmd,
            recurse,
            true,
            lockmode,
            query_string,
        )?;
        childrel.close(NoLock)?;
    }
    Ok(())
}

// ATExecAddColumn's defval leg: queue the phase-3 fill, or store the value
// as attmissingval and skip the rewrite.
fn add_column_phase3_fill<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    col_def: &ColumnDef<'mcx>,
    attribute: &types_tuple::FormData_pg_attribute,
) -> PgResult<()> {
    let mut has_missing = false;
    let has_domain_constraints = typcache::domain::DomainHasConstraints(attribute.atttypid)?;
    // tablecmds.c:7472-7483: identity columns build a NextValueExpr manually
    // (sequence ownership isn't set yet, so build_column_default can't).
    let mut defval = if col_def.identity != 0 {
        let prv = col_def.identitySequence.expect("identity column sequence");
        let rv = rel_vocab::RangeVar {
            catalogname: prv.catalogname,
            schemaname: prv.schemaname,
            relname: prv.relname.expect("RangeVar.relname"),
            inh: prv.inh,
            relpersistence: prv.relpersistence,
            location: prv.location,
        };
        let seqid = catalog_namespace::RangeVarGetRelid(&rv, NoLock, false)?;
        Some(Node::mk(
            mcx,
            types_nodes::primnodes::NextValueExpr { seqid, typeId: attribute.atttypid },
        )?)
    } else {
        // build_column_default falls back to the column type's own default
        // (domains), so it runs regardless of atthasdef (tablecmds.c:7440).
        rewrite_handler::build_column_default(mcx, rel, attnum as usize)?
    };
    if defval.is_none() && has_domain_constraints {
        // NULL::basetype through CoerceToDomain so phase 3 evaluates the
        // domain constraints (C keeps the historical only-if-rows failure).
        let mut base_type_mod = attribute.atttypmod;
        let base_type_id =
            lsyscache::getBaseTypeAndTypmod(attribute.atttypid, &mut base_type_mod)?;
        let base_type_coll = lsyscache::get_typcollation(base_type_id)?;
        let (typlen, typbyval) = lsyscache::get_typlenbyval(base_type_id)?;
        let nullconst = Node::mk(
            mcx,
            types_nodes::primnodes::Const {
                consttype: base_type_id,
                consttypmod: base_type_mod,
                constcollid: base_type_coll,
                constlen: typlen as i32,
                constvalue: Datum::null(),
                constisnull: true,
                constbyval: typbyval,
                location: -1,
            },
        )?;
        let pstate = parser_small1::make_parsestate(mcx, None);
        defval = Some(
            coerce::coerce_to_target_type(
                mcx,
                &pstate,
                nullconst,
                base_type_id,
                attribute.atttypid,
                attribute.atttypmod,
                coerce::CoercionContext::COERCION_ASSIGNMENT,
                types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?
            .unwrap_or_else(|| panic!("failed to coerce base type to domain")),
        );
    }
    if let Some(defval) = defval {
        let defval = clauses::eval_const_expressions(mcx, defval)?;
        tab.newvals.push(NewColumnValue {
            attnum,
            expr: defval,
            is_generated: col_def.generated != 0,
        });
        // Coercion to a constrained domain counts as volatile here: it may
        // fail, which must only happen when the table has rows.
        if rel.rd_rel.relkind == RELKIND_RELATION
            && col_def.generated == 0
            && !has_domain_constraints
            && !clauses::contain_volatile_functions(defval)?
        {
            let mut state =
                execexpr::exec_init_expr(mcx, Some(defval), execexpr::ParamBind::NONE)?
                    .expect("non-nil default expression");
            state.arm_result_mcx(mcx);
            let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let r = execexpr::exec_eval_expr(&mut state, &mut slots)?;
            if !r.isnull {
                catalog_heap::StoreAttrMissingVal(mcx, rel, attnum, r.value)?;
                xact::CommandCounterIncrement()?;
                has_missing = true;
            }
        } else if col_def.generated != b'v' {
            tab.rewrite |= AT_REWRITE_DEFAULT_VAL;
        }
    }
    if !has_missing {
        tab.verify_new_notnull |= col_def.is_not_null;
    }
    Ok(())
}

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

#[allow(clippy::too_many_arguments)]
fn ATExecDropColumn<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    col_name: &str,
    behavior: types_nodes::parsenodes::DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
    addrs: Option<&mut catalog_dependency::ObjectAddresses>,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_DropColumn,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }
    debug_assert!(!recursing || addrs.is_some());
    let mut own_addrs;
    let addrs: &mut catalog_dependency::ObjectAddresses = match addrs {
        Some(a) => a,
        None => {
            own_addrs = catalog_dependency::ObjectAddresses::new();
            &mut own_addrs
        }
    };

    let Some((attnum, attinhcount)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        if !missing_ok {
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
    if attinhcount > 0 && !recursing {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot drop inherited column \"{col_name}\""))
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let mut is_expr = false;
    let mut attset = types_nodes::Bitmapset::empty();
    attset.add_member(
        mcx,
        attnum as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
    )?;
    if crate::partition::has_partition_attrs(mcx, rel, &attset, &mut is_expr)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot drop column \"{col_name}\" because it is part of the partition key of relation \"{relname}\""
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }

    let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
    if !children.is_empty() {
        if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE && !recurse {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "cannot drop column from only the partitioned table when partitions \
                     exist"
                        .to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION)
                .with_hint("Do not specify the ONLY keyword.".to_string()),
            ));
        }
        for &childrelid in children.iter() {
            let childrel = table::table_open(mcx, childrelid, NoLock)?;
            catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
            let Some((childattnum, childinhcount)) =
                attname_lookup(mcx, childrelid, col_name, false)?
            else {
                panic!(
                    "cache lookup failed for attribute \"{col_name}\" of relation \
                     {childrelid}"
                );
            };
            if childinhcount <= 0 {
                panic!("relation {childrelid} has non-inherited attribute \"{col_name}\"");
            }
            let childislocal = childrel.rd_att.attr(childattnum as usize - 1).attislocal;
            if recurse {
                if childinhcount == 1 && !childislocal {
                    ATExecDropColumn(
                        mcx,
                        &childrel,
                        col_name,
                        behavior,
                        true,
                        true,
                        false,
                        lockmode,
                        Some(addrs),
                    )?;
                } else {
                    update_pg_attribute(
                        mcx,
                        childrelid,
                        childattnum,
                        &[(Anum_pg_attribute_attinhcount, Datum::from_i16(childinhcount - 1))],
                    )?;
                    xact::CommandCounterIncrement()?;
                }
            } else {
                update_pg_attribute(
                    mcx,
                    childrelid,
                    childattnum,
                    &[
                        (Anum_pg_attribute_attinhcount, Datum::from_i16(childinhcount - 1)),
                        (Anum_pg_attribute_attislocal, Datum::from_bool(true)),
                    ],
                )?;
                xact::CommandCounterIncrement()?;
            }
            childrel.close(NoLock)?;
        }
    }

    addrs.add_exact_object_address(pg_depend::ObjectAddress::sub_set(
        RELATION_RELATION_ID,
        rel.rd_id,
        attnum as i32,
    ));
    if !recursing {
        catalog_dependency::performMultipleDeletions(mcx, addrs, behavior, 0)?;
    }
    Ok(())
}

// ATExecColumnDefault (SET/DROP DEFAULT).
fn ATExecColumnDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_ColumnDefault name");
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let att = rel.rd_att.attr(attnum as usize - 1);
    if att.attidentity != 0 {
        let mut e = PgError::new(
            ERROR,
            format!("column \"{col_name}\" of relation \"{relname}\" is an identity column"),
        )
        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR);
        if cmd.def.is_none() {
            e = e.with_hint(
                "Use ALTER TABLE ... ALTER COLUMN ... DROP IDENTITY instead.".to_string(),
            );
        }
        return Err(Box::new(e));
    }
    if att.attgenerated != 0 {
        let mut e = PgError::new(
            ERROR,
            format!("column \"{col_name}\" of relation \"{relname}\" is a generated column"),
        )
        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR);
        if cmd.def.is_some() {
            e = e.with_hint(
                "Use ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION instead.".to_string(),
            );
        } else if att.attgenerated == types_core::catalog::ATTRIBUTE_GENERATED_STORED as i8 {
            e = e.with_hint(
                "Use ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION instead.".to_string(),
            );
        }
        return Err(Box::new(e));
    }
    RemoveAttrDefault(mcx, rel.rd_id, attnum, false, cmd.def.is_some())?;
    if let Some(def) = cmd.def {
        // C queryString: NULL (tablecmds.c:8196).
        crate::constraints::add_relation_new_constraints(
            mcx,
            rel,
            &[(attnum, def, 0)],
            &NodeList::nil(),
            None,
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
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetExpression name");
    let relname = rel.name().to_string();
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
    // C: a changed expression could inject nodes not permitted in a row filter.
    if attgenerated == b'v' as i8
        && !pg_publication::GetRelationPublications(mcx, rel.rd_id)?.is_empty()
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated \
                 columns in tables that are part of a publication"
                    .to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(format!(
                "Column \"{col_name}\" of relation \"{relname}\" is a virtual generated \
                 column."
            )),
        ));
    }
    let rewrite = attgenerated == b's' as i8;

    if rewrite {
        catalog_heap::RelationClearMissing(mcx, rel.rd_id)?;
        xact::CommandCounterIncrement()?;
        RememberAllDependentForRebuilding(mcx, tab, rel, attnum, col_name, false)?;
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
    // C queryString: NULL (tablecmds.c:8721).
    crate::constraints::add_relation_new_constraints(
        mcx,
        rel,
        &[(attnum, newexpr, attgenerated as u8)],
        &NodeList::nil(),
        None,
    )?;
    xact::CommandCounterIncrement()?;

    if rewrite {
        let rel2 = table::table_open(mcx, rel.rd_id, NoLock)?;
        let defval = rewrite_handler::build_column_default(mcx, &rel2, attnum as usize)?
            .expect("generated column has a generation expression");
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
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if !recurse && !pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?.is_empty() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "ALTER TABLE / DROP EXPRESSION must be applied to child tables too".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if !recursing {
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

// ATParseTransformCmd's beforeStmts/afterStmts legs. transformAlterTableStmt
// folds IndexStmts into AT_AddIndex[Constraint] subcommands and emits only
// CreateSeqStmt (blist) / AlterSeqStmt (alist) for identity columns, so the
// fallback stays loud; sequence depends on tablecmds, so execution rides
// sequence_seams.
fn run_seq_stmts<'mcx>(mcx: Mcx<'mcx>, stmts: &NodeList<'mcx>) -> PgResult<()> {
    for s in stmts.iter() {
        // ProcessUtilityForAlterTable: the enclosing ALTER TABLE package
        // closes before each sub-statement collects and reopens after
        // (utility.c:1959-1989).
        let saved = event_trigger::EventTriggerAlterTableSuspend();
        // Queued AlterTableStmt (per-column FDW options from ADD COLUMN ...
        // OPTIONS): recurse as ProcessUtilityForAlterTable does; the nested
        // AlterTable collects its own commands.
        if let Some(at) = s.as_variant::<types_nodes::parsenodes::AlterTableStmt>() {
            let tag = match at.objtype {
                types_nodes::parsenodes::ObjectType::OBJECT_FOREIGN_TABLE => {
                    types_core::CommandTag(13)
                }
                _ => types_core::CommandTag(34),
            };
            let lockmode = AlterTableGetLockLevel(&at.cmds);
            let relid = AlterTableLookupRelation(mcx, at, lockmode)?;
            event_trigger::EventTriggerAlterTableStart(tag);
            event_trigger::EventTriggerAlterTableRelid(relid);
            let res = AlterTable(mcx, relid, lockmode, at, "", tag);
            event_trigger::EventTriggerAlterTableEnd();
            res?;
            if let Some((t, relid)) = saved {
                event_trigger::EventTriggerAlterTableStart(t);
                event_trigger::EventTriggerAlterTableRelid(relid);
            }
            xact::CommandCounterIncrement()?;
            continue;
        }
        let (seqoid, tag) = if let Some(cs) = s.as_variant::<types_nodes::rawnodes::CreateSeqStmt>()
        {
            (
                sequence_seams::define_sequence::call(mcx, cs)?,
                types_core::CommandTag::CREATE_SEQUENCE,
            )
        } else if let Some(alt) = s.as_variant::<types_nodes::AlterSeqStmt>() {
            (
                sequence_seams::alter_sequence::call(mcx, alt)?,
                types_core::CommandTag::ALTER_SEQUENCE,
            )
        } else {
            unported(&format!(
                "ATParseTransformCmd queued statement {:?}",
                s.node_tag()
            ));
        };
        event_trigger::EventTriggerCollectSimpleCommand(
            pg_depend::ObjectAddress::set(RELATION_RELATION_ID, seqoid),
            pg_depend::ObjectAddress::set(InvalidOid, InvalidOid),
            tag,
        );
        if let Some((t, relid)) = saved {
            event_trigger::EventTriggerAlterTableStart(t);
            event_trigger::EventTriggerAlterTableRelid(relid);
        }
        xact::CommandCounterIncrement()?;
    }
    Ok(())
}

fn ATExecAddIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_AddIdentity name");
    let cdef = cmd
        .def
        .expect("AT_AddIdentity ColumnDef")
        .as_variant::<ColumnDef>()
        .expect("ColumnDef");
    add_identity_internal(mcx, rel, col_name, cdef.identity as i8, lockmode, cmd.recurse, false)
}

fn add_identity_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    col_name: &str,
    identity: i8,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    let ispartitioned = rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE;
    if ispartitioned && !recurse {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot add identity to a column of only the partitioned table".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION)
            .with_hint("Do not specify the ONLY keyword."),
        ));
    }
    if rel.rd_rel.relispartition && !recursing {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot add identity to a column of a partition".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let att = rel.rd_att.attr(attnum as usize - 1);
    if !att.attnotnull {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "column \"{col_name}\" of relation \"{relname}\" must be declared \
                     NOT NULL before identity can be added"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    let con = pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?
        .unwrap_or_else(|| {
            panic!(
                "cache lookup failed for not-null constraint on column \"{col_name}\" of \
                 relation \"{relname}\""
            )
        });
    if !con.convalidated {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{relname}\"",
                    con.name_str()
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT."
                    .to_string(),
            ),
        ));
    }
    if att.attidentity != 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "column \"{col_name}\" of relation \"{relname}\" is already an \
                     identity column"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    if att.atthasdef {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "column \"{col_name}\" of relation \"{relname}\" already has a \
                     default value"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[(Anum_pg_attribute_attidentity, Datum::from_i8(identity))],
    )?;

    // Identity is not inherited in regular inheritance children; recurse to
    // partitions only (tablecmds.c:8345-8362).
    if recurse && ispartitioned {
        let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for &childoid in children.iter() {
            let childrel = table::table_open(mcx, childoid, NoLock)?;
            add_identity_internal(mcx, &childrel, col_name, identity, lockmode, recurse, true)?;
            childrel.close(NoLock)?;
        }
    }
    Ok(())
}

fn ATExecSetIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    set_identity_internal(mcx, rel, cmd, lockmode, cmd.recurse, false)
}

fn set_identity_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetIdentity name");
    let relname = rel.name().to_string();
    let ispartitioned = rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE;
    if ispartitioned && !recurse {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot change identity column of only the partitioned table".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION)
            .with_hint("Do not specify the ONLY keyword."),
        ));
    }
    if rel.rd_rel.relispartition && !recursing {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot change identity column of a partition".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let mut generated_el: Option<&types_nodes::parsenodes::DefElem<'_>> = None;
    if let Some(defnode) = cmd.def {
        for opt in defnode.as_list().expect("DefElem list").iter() {
            let defel =
                opt.as_variant::<types_nodes::parsenodes::DefElem>().expect("DefElem");
            match defel.defname.expect("defname") {
                "generated" => {
                    if generated_el.is_some() {
                        return Err(Box::new(
                            PgError::new(
                                ERROR,
                                "conflicting or redundant options".to_string(),
                            )
                            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
                        ));
                    }
                    generated_el = Some(defel);
                }
                other => panic!("option \"{other}\" not recognized"),
            }
        }
    }
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if rel.rd_att.attr(attnum as usize - 1).attidentity == 0 {
        return Err(not_an_identity_column(col_name, &relname));
    }
    if let Some(g) = generated_el {
        let v = g.arg.expect("generated arg").as_integer().expect("Integer").ival;
        update_pg_attribute(
            mcx,
            rel.rd_id,
            attnum,
            &[(Anum_pg_attribute_attidentity, Datum::from_i8(v as i8))],
        )?;

        // Identity is not inherited in regular inheritance children; recurse
        // to partitions only (tablecmds.c:8462-8479).
        if recurse && ispartitioned {
            let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
            for &childoid in children.iter() {
                let childrel = table::table_open(mcx, childoid, NoLock)?;
                set_identity_internal(mcx, &childrel, cmd, lockmode, recurse, true)?;
                childrel.close(NoLock)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn ATExecDropIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    col_name: &str,
    missing_ok: bool,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    let ispartitioned = rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE;
    if ispartitioned && !recurse {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot drop identity from a column of only the partitioned table".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION)
            .with_hint("Do not specify the ONLY keyword."),
        ));
    }
    if rel.rd_rel.relispartition && !recursing {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot drop identity from a column of a partition".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if rel.rd_att.attr(attnum as usize - 1).attidentity == 0 {
        if !missing_ok {
            return Err(not_an_identity_column(col_name, &relname));
        }
        elog_seams::ereport_msg::call(
            NOTICE,
            format!(
                "column \"{col_name}\" of relation \"{relname}\" is not an identity \
                 column, skipping"
            ),
            None,
        )?;
        return Ok(());
    }
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[(Anum_pg_attribute_attidentity, Datum::from_i8(0))],
    )?;

    // Identity is not inherited in regular inheritance children; recurse to
    // partitions only.
    if recurse && ispartitioned {
        let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for &childoid in children.iter() {
            let childrel = table::table_open(mcx, childoid, NoLock)?;
            ATExecDropIdentity(mcx, &childrel, col_name, false, lockmode, recurse, true)?;
            childrel.close(NoLock)?;
        }
    }

    if !recursing {
        let seqid = pg_depend::getIdentitySequence(mcx, rel.rd_id, attnum as i32, false)?;
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            RELATION_RELATION_ID,
            seqid,
            RELATION_RELATION_ID,
            pg_depend::DependencyType::Internal,
        )?;
        xact::CommandCounterIncrement()?;
        let seqaddress = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, seqid);
        catalog_dependency::performDeletion(
            mcx,
            &seqaddress,
            types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
            catalog_dependency::PERFORM_DELETION_INTERNAL,
        )?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_an_identity_column(col_name: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("column \"{col_name}\" of relation \"{relname}\" is not an identity column"),
        )
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

fn ATExecClusterOn<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let index_name = cmd.name.expect("AT_ClusterOn name");
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
    commands_cluster::check_index_is_clusterable(mcx, rel, index_oid, lockmode)?;
    commands_cluster::mark_index_clustered(mcx, rel, index_oid, false)
}

// ATPrepChangePersistence. GetRelationPublications is const-empty
// (publications unported), so the publication guard cannot fire.
fn ATPrepChangePersistence<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    to_logged: bool,
) -> PgResult<()> {
    match rel.rd_rel.relpersistence {
        types_core::catalog::RELPERSISTENCE_TEMP => {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cannot change logged status of table \"{}\" because it is \
                         temporary",
                        rel.name()
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        types_core::catalog::RELPERSISTENCE_PERMANENT if to_logged => return Ok(()),
        types_core::RELPERSISTENCE_UNLOGGED if !to_logged => return Ok(()),
        _ => {}
    }

    let pg_con =
        table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, types_rel::AccessShareLock)?;
    let keyattno = if to_logged {
        pg_constraint::Anum_pg_constraint_conrelid
    } else {
        pg_constraint::Anum_pg_constraint_confrelid
    };
    let key = [oid_scankey(keyattno as usize, rel.rd_id)];
    // C uses ConstraintRelidTypidNameIndexId only for the conrelid scan.
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_con,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        to_logged,
        None,
        &key,
    )?;
    let desc = pg_con.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_constraint columns under its descriptor.
        let contype = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_constraint::Anum_pg_constraint_contype as i32,
                desc,
                &mut isnull,
            )
        }
        .as_i8() as u8;
        if contype != pg_constraint::CONSTRAINT_FOREIGN {
            continue;
        }
        // SAFETY: as above.
        let conrelid = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_constraint::Anum_pg_constraint_conrelid as i32,
                desc,
                &mut isnull,
            )
        }
        .as_oid();
        // SAFETY: as above.
        let confrelid = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_constraint::Anum_pg_constraint_confrelid as i32,
                desc,
                &mut isnull,
            )
        }
        .as_oid();
        let foreignrelid = if to_logged { confrelid } else { conrelid };
        if foreignrelid == rel.rd_id {
            continue;
        }
        let foreignrel =
            relation_seams::relation_open::call(mcx, foreignrelid, types_rel::AccessShareLock)?;
        let foreign_permanent = foreignrel.rd_rel.relpersistence
            == types_core::catalog::RELPERSISTENCE_PERMANENT;
        let fname = foreignrel.name().to_string();
        if to_logged && !foreign_permanent {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "could not change table \"{}\" to logged because it references \
                         unlogged table \"{fname}\"",
                        rel.name()
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        if !to_logged && foreign_permanent {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "could not change table \"{}\" to unlogged because it references \
                         logged table \"{fname}\"",
                        rel.name()
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        foreignrel.close(types_rel::AccessShareLock)?;
    }
    genam::systable_endscan(mcx, scan)?;
    pg_con.close(types_rel::AccessShareLock)?;

    tab.rewrite |= AT_REWRITE_ALTER_PERSISTENCE;
    tab.newrelpersistence = if to_logged {
        types_core::catalog::RELPERSISTENCE_PERMANENT
    } else {
        types_core::RELPERSISTENCE_UNLOGGED
    };
    tab.chg_persistence = true;
    Ok(())
}

// ATExecDropNotNull (tablecmds.c).
fn ATExecDropNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
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
        let parent_id = pg_inherits::get_partition_parent(mcx, rel.rd_id, false)?;
        let parent = table::table_open(mcx, parent_id, types_rel::AccessShareLock)?;
        let parent_attnum = attname_lookup(mcx, parent_id, col_name, false)?
            .map(|(a, _)| a)
            .expect("partition column exists in parent");
        if parent.rd_att.attr(parent_attnum as usize - 1).attnotnull {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("column \"{col_name}\" is marked NOT NULL in parent table"),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        parent.close(types_rel::AccessShareLock)?;
    }
    let con = pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?
        .unwrap_or_else(|| {
            panic!(
                "cache lookup failed for not-null constraint on column \"{col_name}\" of \
                 relation \"{relname}\""
            )
        });

    dropconstraint_internal(
        mcx,
        rel,
        &nn_con_shape(&con),
        types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
        cmd.recurse,
        false,
        lockmode,
    )
}

fn nn_con_shape(con: &pg_constraint::NotNullConTup) -> pg_constraint::ConShape {
    pg_constraint::ConShape {
        oid: con.oid,
        contype: pg_constraint::CONSTRAINT_NOTNULL,
        conname: con.conname,
        coninhcount: con.coninhcount,
        connoinherit: con.connoinherit,
        conislocal: con.conislocal,
        condeferrable: false,
        condeferred: false,
        conenforced: true,
        convalidated: con.convalidated,
        // NotNullConTup does not carry conparentid; callers of this shape do
        // not read it.
        conparentid: InvalidOid,
        conindid: InvalidOid,
        confrelid: InvalidOid,
        notnull_attnum: con.attnum,
    }
}

// findNotNullConstraint (pg_constraint.c): by-column-name variant.
pub(crate) fn find_notnull_constraint_by_colname<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    colname: &str,
) -> PgResult<Option<pg_constraint::NotNullConTup>> {
    let Some((attnum, _)) = attname_lookup(mcx, relid, colname, false)? else {
        return Ok(None);
    };
    pg_constraint::findNotNullConstraintAttnum(mcx, relid, attnum)
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

// ATExecSetNotNull (tablecmds.c): exec-time recursion, one level at a time.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ATExecSetNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    conname: Option<&str>,
    col_name: &str,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_AddConstraint,
            rel,
            ATT_PARTITIONED_TABLE | ATT_TABLE | ATT_FOREIGN_TABLE,
        )?;
        debug_assert!(conname.is_some());
    }
    let relname = rel.name().to_string();
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    if let Some(con) = pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)? {
        if con.connoinherit && recurse {
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
        if recursing {
            if con.coninhcount == i16::MAX {
                return Err(Box::new(
                    PgError::new(ERROR, "too many inheritance parents".to_string())
                        .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                ));
            }
            pg_constraint::update_constraint_fields(
                mcx,
                con.oid,
                &[(
                    pg_constraint::Anum_pg_constraint_coninhcount,
                    Datum::from_i16(con.coninhcount + 1),
                )],
            )?;
        } else if !con.conislocal {
            pg_constraint::update_constraint_fields(
                mcx,
                con.oid,
                &[(pg_constraint::Anum_pg_constraint_conislocal, Datum::from_bool(true))],
            )?;
        } else if !con.convalidated {
            return ATExecValidateConstraint(
                mcx,
                wqueue,
                rel,
                con.name_str(),
                recurse,
                recursing,
                lockmode,
            );
        }
        return Ok(());
    }
    let mut is_no_inherit = false;
    if !recurse && find_inheritance_children_exist(mcx, rel.rd_id)? {
        if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            return Err(Box::new(
                PgError::new(ERROR, "constraint must be added to child tables too".to_string())
                    .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION)
                    .with_hint("Do not specify the ONLY keyword.".to_string()),
            ));
        }
        is_no_inherit = true;
    }
    let name_storage;
    let con_name: &str = match conname {
        Some(n) => n,
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
    create_notnull_constraint(mcx, wqueue, rel, attnum, con_name, !recursing, is_no_inherit, true)?;
    if recurse {
        let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for &childoid in children.iter() {
            let childrel = table::table_open(mcx, childoid, NoLock)?;
            xact::CommandCounterIncrement()?;
            ATExecSetNotNull(
                mcx,
                wqueue,
                &childrel,
                Some(con_name),
                col_name,
                recurse,
                true,
                lockmode,
            )?;
            childrel.close(NoLock)?;
        }
    }
    Ok(())
}

// The CreateConstraintEntry + set_attnotnull tail of C's AddRelationNewConstraints
// NOT NULL leg as reached from ATExecSetNotNull.
#[allow(clippy::too_many_arguments)]
fn create_notnull_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    con_name: &str,
    is_local: bool,
    is_no_inherit: bool,
    initially_valid: bool,
) -> PgResult<()> {
    let conkey = [attnum];
    let mut entry = pg_constraint::ConstraintEntry::base(
        con_name,
        rel.rd_rel.relnamespace,
        pg_constraint::CONSTRAINT_NOTNULL,
        rel.rd_id,
    );
    entry.conkey = &conkey;
    entry.n_keys = 1;
    entry.is_local = is_local;
    entry.inhcount = if is_local { 0 } else { 1 };
    entry.is_no_inherit = is_no_inherit;
    entry.is_validated = initially_valid;
    pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    // AddRelationNewConstraints tail: pg_class update fires the SI message
    // peers use to rebuild relcache entries.
    crate::constraints::set_relation_num_checks(
        mcx,
        rel,
        rel.rd_att.constr.as_deref().map(|c| c.num_check as i16).unwrap_or(0),
    )?;

    // An invalid constraint sets attnotnull without queueing verification.
    set_attnotnull(mcx, wqueue, rel, attnum, initially_valid)?;
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
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let Some(defnode) = cmd.def else { return Ok(()) };
    let Some(constr) = defnode.as_variant::<Constraint>() else { return Ok(()) };
    if constr.contype != ConstrType::CONSTR_PRIMARY {
        return Ok(());
    }
    let mut children: Option<PgVec<'mcx, Oid>> = None;
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
        if !recurse {
            // ONLY: verify every direct child already carries a compatible
            // not-null constraint (children searched once).
            if children.is_none() {
                children =
                    Some(pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?);
            }
            for &childrelid in children.as_ref().expect("children fetched").iter() {
                let child_name = lsyscache::relation::get_rel_name(mcx, childrelid)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let Some(con) = find_notnull_constraint_by_colname(mcx, childrelid, key)?
                else {
                    return Err(Box::new(PgError::new(
                        ERROR,
                        format!(
                            "column \"{key}\" of table \"{child_name}\" is not marked \
                             NOT NULL"
                        ),
                    )));
                };
                verify_notnull_pk_compatible(&con, key, &child_name)?;
            }
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

// ATAddCheckNNConstraint (tablecmds.c): CHECK and NOT NULL constraints with
// exec-time recursion, one inheritance level at a time.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ATAddCheckNNConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    tabidx: usize,
    rel: &Relation<'mcx>,
    defnode: Node<'mcx>,
    recurse: bool,
    recursing: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_AddConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }
    let constr = defnode.as_variant::<Constraint>().expect("Constraint");
    let contype = constr.contype;
    let conname_was_none = constr.conname.is_none();
    // C copyObject boundary: transformExpr must not scribble the queued tree.
    let constr_copy = copyfuncs::copy_object(mcx, defnode)?;
    let cooked = crate::constraints::add_relation_new_constraints_ext(
        mcx,
        rel,
        &[],
        &NodeList::make1(mcx, constr_copy)?,
        recursing || is_readd,
        !recursing,
        None,
    )?;
    debug_assert!(cooked.len() <= 1);
    for c in cooked.iter() {
        if !c.skip_validation && c.contype != ConstrType::CONSTR_NOTNULL {
            wqueue[tabidx]
                .constraints
                .push(NewConstraint { name: c.name, qual: c.expr.expect("CHECK expr") });
        }
        if conname_was_none {
            let assigned = c.name;
            // SAFETY: parse tree is statement-owned; children must reuse the
            // parent's assigned constraint name.
            unsafe {
                defnode
                    .with_mut::<Constraint, _>(|cc| cc.conname = Some(assigned))
                    .expect("Constraint");
            }
        }
        if contype == ConstrType::CONSTR_NOTNULL {
            let skip = constr.skip_validation;
            set_attnotnull(mcx, wqueue, rel, c.attnum, !skip)?;
        }
    }
    xact::CommandCounterIncrement()?;

    // Merged with an existing constraint: children already have it, and
    // recursing again would double-count coninhcount.
    if cooked.is_empty() {
        return Ok(());
    }
    if defnode.as_variant::<Constraint>().expect("Constraint").is_no_inherit {
        return Ok(());
    }
    let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
    if !recurse && !children.is_empty() {
        return Err(Box::new(
            PgError::new(ERROR, "constraint must be added to child tables too".to_string())
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    for &childrelid in children.iter() {
        let childrel = table::table_open(mcx, childrelid, NoLock)?;
        catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
        let childtabidx = ATGetQueueEntry(mcx, wqueue, &childrel);
        ATAddCheckNNConstraint(
            mcx,
            wqueue,
            childtabidx,
            &childrel,
            defnode,
            recurse,
            true,
            is_readd,
            lockmode,
            query_string,
        )?;
        childrel.close(NoLock)?;
    }
    Ok(())
}

// set_attnotnull (tablecmds.c); NotNullImpliedByRelConstraints proof unported
// so phase 3 always verifies when queue_validation.
fn set_attnotnull<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    queue_validation: bool,
) -> PgResult<()> {
    CheckAlterTableIsSafe(rel)?;
    let att = rel.rd_att.attr(attnum as usize - 1);
    if att.attisdropped {
        return Ok(());
    }
    if !att.attnotnull {
        update_pg_attribute(
            mcx,
            rel.rd_id,
            attnum,
            &[(Anum_pg_attribute_attnotnull, Datum::from_bool(true))],
        )?;
        if queue_validation {
            let tabidx = ATGetQueueEntry(mcx, wqueue, rel);
            wqueue[tabidx].verify_new_notnull = true;
        }
        xact::CommandCounterIncrement()?;
    } else {
        inval::invalidate::CacheInvalidateRelcacheByRelid(rel.rd_id)?;
    }
    Ok(())
}

// ATExecDropConstraint + dropconstraint_internal (tablecmds.c).
fn ATExecDropConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let constr_name = cmd.name.expect("AT_DropConstraint name");
    let relname = rel.name().to_string();
    match pg_constraint::findConstraintByName(mcx, rel.rd_id, constr_name)? {
        Some(con) => {
            dropconstraint_internal(mcx, rel, &con, cmd.behavior, cmd.recurse, false, lockmode)
        }
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
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_DropConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }
    let relname = rel.name().to_string();
    if con.coninhcount > 0 && !recursing {
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
    let mut colname: Option<String> = None;
    if con.contype == pg_constraint::CONSTRAINT_NOTNULL {
        let attnum = con.notnull_attnum;
        let att = rel.rd_att.attr(attnum as usize - 1);
        let col_name =
            core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8").to_string();
        colname = Some(col_name.clone());
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
        // C calls CheckAlterTableIsSafe here (tablecmds.c:14209).
        let frel = table::table_open(mcx, con.confrelid, AccessExclusiveLock)?;
        CheckAlterTableIsSafe(&frel)?;
        frel.close(NoLock)?;
    }
    let object = pg_depend::ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, con.oid);
    catalog_dependency::performDeletion(mcx, &object, behavior, 0)?;
    // Partitioned tables drop non-CHECK, non-NOT-NULL inherited constraints
    // via the dependency mechanism.
    if con.contype != pg_constraint::CONSTRAINT_CHECK
        && con.contype != pg_constraint::CONSTRAINT_NOTNULL
        && rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE
    {
        return Ok(());
    }
    let children = if !con.connoinherit {
        pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?
    } else {
        PgVec::new_in(mcx)
    };
    for &childrelid in children.iter() {
        let childrel = table::table_open(mcx, childrelid, NoLock)?;
        catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
        let childcon = if con.contype == pg_constraint::CONSTRAINT_NOTNULL {
            let col = colname.as_deref().expect("colname saved for NOT NULL constraint");
            match find_notnull_constraint_by_colname(mcx, childrelid, col)? {
                Some(nn) => nn_con_shape(&nn),
                None => panic!(
                    "cache lookup failed for not-null constraint on column \"{col}\" of \
                     relation {childrelid}"
                ),
            }
        } else {
            match pg_constraint::findConstraintByName(mcx, childrelid, con.name_str())? {
                Some(c) => c,
                None => {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "constraint \"{}\" of relation \"{}\" does not exist",
                                con.name_str(),
                                childrel.name()
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
                    ));
                }
            }
        };
        if childcon.contype != pg_constraint::CONSTRAINT_CHECK
            && childcon.contype != pg_constraint::CONSTRAINT_NOTNULL
        {
            panic!("inherited constraint is not a CHECK or not-null constraint");
        }
        if childcon.coninhcount <= 0 {
            panic!(
                "relation {childrelid} has non-inherited constraint \"{}\"",
                childcon.name_str()
            );
        }
        if recurse {
            if childcon.coninhcount == 1 && !childcon.conislocal {
                dropconstraint_internal(
                    mcx, &childrel, &childcon, behavior, recurse, true, lockmode,
                )?;
            } else {
                pg_constraint::update_constraint_fields(
                    mcx,
                    childcon.oid,
                    &[(
                        pg_constraint::Anum_pg_constraint_coninhcount,
                        Datum::from_i16(childcon.coninhcount - 1),
                    )],
                )?;
                xact::CommandCounterIncrement()?;
            }
        } else {
            let newcount = childcon.coninhcount - 1;
            let mut fields: PgVec<'_, (AttrNumber, Datum)> = PgVec::new_in(mcx);
            fields.push((pg_constraint::Anum_pg_constraint_coninhcount, Datum::from_i16(newcount)));
            if newcount == 0 {
                fields.push((pg_constraint::Anum_pg_constraint_conislocal, Datum::from_bool(true)));
            }
            pg_constraint::update_constraint_fields(mcx, childcon.oid, &fields)?;
            xact::CommandCounterIncrement()?;
        }
        childrel.close(NoLock)?;
    }
    Ok(())
}

// ATExecAddIndex: the IndexStmt is already transformed; indexcmds depends on
// tablecmds, so DefineIndex rides a seam.
fn str_in_mcx<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v = mcx::PgString::new_in(mcx);
    v.try_push_str(s)?;
    // SAFETY: PgString invariant — bytes are valid UTF-8.
    Ok(unsafe { core::str::from_utf8_unchecked(v.into_bytes().leak()) })
}

fn ATExecAddIndex<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    is_rebuild: bool,
) -> PgResult<()> {
    let stmt_node = cmd.def.expect("AT_AddIndex IndexStmt");
    let old_number = stmt_node
        .as_variant::<types_nodes::rawnodes::IndexStmt>()
        .expect("IndexStmt")
        .oldNumber;
    let skip_build = tab.rewrite > 0 || old_number != 0;
    indexcmds_seams::define_index_for_alter::call(mcx, rel.rd_id, stmt_node, is_rebuild, skip_build)?;
    Ok(())
}

const MAX_STATISTICS_TARGET: i32 = 10000;
const Anum_pg_attribute_attstattarget: usize = 21;

fn ATExecSetStatistics<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let is_index = rel.rd_rel.relkind == types_rel::RELKIND_INDEX
        || rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_INDEX;
    if !is_index && cmd.name.is_none() {
        return Err(Box::new(
            PgError::new(ERROR, "cannot refer to non-index column by number".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
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
    let attnum = match cmd.name {
        Some(col_name) => {
            let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
                return Err(undefined_column(col_name, &relname));
            };
            if attnum <= 0 {
                return Err(cannot_alter_system_column(col_name));
            }
            attnum
        }
        None => {
            if cmd.num <= 0 || cmd.num as usize > rel.rd_att.natts as usize {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "column number {} of relation \"{relname}\" does not exist",
                            cmd.num
                        ),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
                ));
            }
            cmd.num
        }
    };
    let att = rel.rd_att.attr(attnum as usize - 1);
    let attname = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
    if att.attgenerated == b'v' as i8 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot alter statistics on virtual generated column \"{attname}\""),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if is_index {
        let (_, _, keys, nkeyatts) = pg_index_shape_full(mcx, rel.rd_id)?;
        if attnum as usize > nkeyatts {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cannot alter statistics on included column \"{attname}\" of index \
                         \"{relname}\""
                    ),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if keys[attnum as usize - 1] != 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cannot alter statistics on non-expression column \"{attname}\" of \
                         index \"{relname}\""
                    ),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint("Alter statistics on table column instead.".to_string()),
            ));
        }
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
    set_index_storage_properties(
        mcx,
        rel,
        attnum,
        &[(Anum_pg_attribute_attstorage, Datum::from_i8(newstorage as i8))],
    )
}

// ATExecSetCompression (tablecmds.c:18744).
fn ATExecSetCompression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetCompression name");
    let relname = rel.name().to_string();
    let compression = cmd
        .def
        .expect("AT_SetCompression String")
        .as_string()
        .expect("AT_SetCompression String")
        .sval;
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        return Err(cannot_alter_system_column(col_name));
    }
    let atttypid = rel.rd_att.attr(attnum as usize - 1).atttypid;
    let cmethod = crate::GetAttributeCompression(atttypid, Some(compression))?;
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[(Anum_pg_attribute_attcompression, Datum::from_i8(cmethod))],
    )?;
    set_index_storage_properties(
        mcx,
        rel,
        attnum,
        &[(Anum_pg_attribute_attcompression, Datum::from_i8(cmethod))],
    )?;
    xact::CommandCounterIncrement()
}

const Anum_pg_attribute_attoptions: usize = 23;

// ATExecSetOptions (tablecmds.c:9051), both SET and RESET forms.
fn ATExecSetOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    is_reset: bool,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_SetOptions name");
    let relname = rel.name().to_string();
    let empty = NodeList::nil();
    let options = cmd.def.and_then(|d| d.as_list()).unwrap_or(&empty);

    let attrel = table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let Some((attnum, _)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        attrel.close(RowExclusiveLock)?;
        return Err(undefined_column(col_name, &relname));
    };
    if attnum <= 0 {
        attrel.close(RowExclusiveLock)?;
        return Err(cannot_alter_system_column(col_name));
    }
    let keys = [oid_scankey(1, rel.rd_id), int2_key(5, attnum)];
    let mut scan =
        genam::systable_beginscan(mcx, &attrel, AttributeRelidNumIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for attribute {attnum} of relation {}", rel.rd_id));
    let desc = attrel.descr();
    let mut isnull = false;
    // SAFETY: attoptions under pg_attribute's descriptor; null-checked.
    let old = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_attribute_attoptions as i32, desc, &mut isnull)
    };
    let old_options = if isnull {
        None
    } else {
        let p = old.as_usize() as *const u8;
        // SAFETY: non-null text[] varlena; live through the scan.
        Some(unsafe {
            core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
        })
    };
    let new_options =
        reloptions::transformRelOptions(mcx, old_options, options, None, &[], false, is_reset)?;
    reloptions::attribute_reloptions(mcx, new_options.as_ref().map(|v| &v[..]), true)?;

    let natts = desc.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    match &new_options {
        Some(img) => {
            repl_values[Anum_pg_attribute_attoptions - 1] =
                Datum::from_usize(img.as_ptr() as usize)
        }
        None => repl_isnull[Anum_pg_attribute_attoptions - 1] = true,
    }
    repl[Anum_pg_attribute_attoptions - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attrel, &otid, &mut newtup)?;
    attrel.close(RowExclusiveLock)
}

// GetAttributeStorage (tablecmds.c).
pub(crate) fn get_attribute_storage(atttypid: Oid, storagemode: &str) -> PgResult<u8> {
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

// SetIndexStorageProperties: apply storage/compression to simple index columns.
fn set_index_storage_properties<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    fields: &[(usize, Datum)],
) -> PgResult<()> {
    let indexes = relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    for &indexoid in indexes.iter() {
        // C index_open(lockmode); SET STORAGE/COMPRESSION's lock level is AEL.
        let indrel = relation_seams::relation_open::call(mcx, indexoid, AccessExclusiveLock)?;
        let keys = pg_index_all_keys(mcx, indexoid)?;
        let Some(pos) = keys.iter().position(|&k| k == attnum) else {
            indrel.close(AccessExclusiveLock)?;
            continue;
        };
        update_pg_attribute(mcx, indexoid, (pos + 1) as AttrNumber, fields)?;
        indrel.close(AccessExclusiveLock)?;
    }
    Ok(())
}

fn pg_index_all_keys<'mcx>(mcx: Mcx<'mcx>, indexoid: Oid) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let (_, _, keys, _) = pg_index_shape_full(mcx, indexoid)?;
    Ok(keys)
}

// ATExecAddConstraint residue: FK only (CHECK/NN ride ATAddCheckNNConstraint).
fn ATExecAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    tabidx: usize,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    recurse: bool,
    query_string: &str,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let _ = query_string;
    let defnode = cmd.def.expect("AT_AddConstraint Constraint");
    let constr = defnode.as_variant::<Constraint>().expect("Constraint");
    if constr.contype == ConstrType::CONSTR_FOREIGN {
        let old_desc = wqueue[tabidx].old_desc.clone();
        return crate::fk::ATExecAddConstraint(
            mcx, wqueue, rel, constr, recurse, &old_desc, lockmode,
        );
    }
    // unported: ATExecAddConstraint non-FOREIGN constraint types
    // (CHECK/NOT NULL ALTER lane)
    let _ = constr.contype;
    Err(Box::new(
        PgError::new(
            ERROR,
            "ALTER TABLE ... ADD CONSTRAINT for this constraint type is not supported yet"
                .to_string(),
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}

// ATExecAddInherit / ATExecDropInherit exec wrappers; the catalog work lives
// in inheritance.rs.
fn ATExecAddInherit<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let prv = cmd
        .def
        .expect("AT_AddInherit RangeVar")
        .as_variant::<types_nodes::primnodes::RangeVar>()
        .expect("RangeVar");
    crate::inheritance::ATExecAddInherit(mcx, rel, prv)
}

fn ATExecDropInherit<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let prv = cmd
        .def
        .expect("AT_DropInherit RangeVar")
        .as_variant::<types_nodes::primnodes::RangeVar>()
        .expect("RangeVar");
    crate::inheritance::ATExecDropInherit(mcx, rel, prv)
}

// ATExecValidateConstraint + Queue{Check,NN}ConstraintValidation
// (tablecmds.c); the FK arm rides fk::queue_fk_constraint_validation.
#[allow(clippy::too_many_arguments)]
fn ATExecValidateConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    constr_name: &str,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    let Some(con) = pg_constraint::findConstraintByName(mcx, rel.rd_id, constr_name)? else {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "constraint \"{constr_name}\" of relation \"{relname}\" does not exist"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    if con.contype != pg_constraint::CONSTRAINT_FOREIGN
        && con.contype != pg_constraint::CONSTRAINT_CHECK
        && con.contype != pg_constraint::CONSTRAINT_NOTNULL
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot validate constraint \"{constr_name}\" of relation \"{relname}\""),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail("This operation is not supported for this type of constraint."),
        ));
    }
    if !con.conenforced {
        return Err(Box::new(
            PgError::new(ERROR, "cannot validate NOT ENFORCED constraint".to_string())
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    if con.convalidated {
        return Ok(());
    }
    match con.contype {
        pg_constraint::CONSTRAINT_FOREIGN => {
            let (form, _) = crate::fk::read_fk_constraint(mcx, con.oid)?;
            return crate::fk::queue_fk_constraint_validation(
                mcx,
                wqueue,
                rel,
                con.confrelid,
                &form,
                lockmode,
            );
        }
        pg_constraint::CONSTRAINT_CHECK => {
            validate_constraint_children(
                mcx, wqueue, rel, &con, None, recurse, recursing, lockmode,
            )?;
            let conbin = pg_constraint::constraint_conbin(mcx, con.oid)?;
            let qual = readfuncs::stringToNode(mcx, conbin.as_str())?;
            let qual = planner::prepjointree::expand_generated_columns_in_expr(mcx, qual, rel, 1)?;
            let tabidx = ATGetQueueEntry(mcx, wqueue, rel);
            wqueue[tabidx]
                .constraints
                .push(NewConstraint { name: str_arena(mcx, con.name_str())?, qual });
            inval::invalidate::CacheInvalidateRelcacheByRelid(rel.rd_id)?;
        }
        _ => {
            let colname = core::str::from_utf8(
                rel.rd_att.attr(con.notnull_attnum as usize - 1).attname.name_str(),
            )
            .expect("attname UTF-8")
            .to_string();
            validate_constraint_children(
                mcx, wqueue, rel, &con, Some(&colname), recurse, recursing, lockmode,
            )?;
            // QueueNNConstraintValidation: attnotnull was set by the invalid
            // ADD, so set_attnotnull reduces to its relcache-inval arm.
            debug_assert!(rel.rd_att.attr(con.notnull_attnum as usize - 1).attnotnull);
            let tabidx = ATGetQueueEntry(mcx, wqueue, rel);
            wqueue[tabidx].verify_new_notnull = true;
            inval::invalidate::CacheInvalidateRelcacheByRelid(rel.rd_id)?;
        }
    }
    pg_constraint::SetConstraintValidated(mcx, con.oid)
}

// Queue{Check,NN}ConstraintValidation children legs (tablecmds.c): validate
// on all inheritors before flipping the parent's convalidated.
#[allow(clippy::too_many_arguments)]
fn validate_constraint_children<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    con: &pg_constraint::ConShape,
    nn_colname: Option<&str>,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if recursing || con.connoinherit {
        return Ok(());
    }
    let children = pg_inherits::find_all_inheritors(mcx, rel.rd_id, lockmode)?;
    for &childoid in children.iter() {
        if childoid == rel.rd_id {
            continue;
        }
        if !recurse {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "constraint must be validated on child tables too".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        match nn_colname {
            None => {
                let childrel = table::table_open(mcx, childoid, NoLock)?;
                ATExecValidateConstraint(
                    mcx,
                    wqueue,
                    &childrel,
                    con.name_str(),
                    false,
                    true,
                    lockmode,
                )?;
                childrel.close(NoLock)?;
            }
            Some(colname) => {
                let childcon = find_notnull_constraint_by_colname(mcx, childoid, colname)?
                    .unwrap_or_else(|| {
                        panic!(
                            "cache lookup failed for not-null constraint on column \
                             \"{colname}\" of relation \"{}\"",
                            lsyscache::relation::get_rel_name(mcx, childoid)
                                .ok()
                                .flatten()
                                .map(|s| s.as_str().to_string())
                                .unwrap_or_default()
                        )
                    });
                if childcon.convalidated {
                    continue;
                }
                let childrel = table::table_open(mcx, childoid, NoLock)?;
                let conname = childcon.name_str().to_string();
                ATExecValidateConstraint(
                    mcx, wqueue, &childrel, &conname, false, true, lockmode,
                )?;
                childrel.close(NoLock)?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn ATPrepAlterColumnType<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    tabidx: usize,
    rel: &Relation<'mcx>,
    recurse: bool,
    recursing: bool,
    cnode: Node<'mcx>,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    let col_name = cmd.name.expect("AT_AlterColumnType name");
    let relname = rel.name().to_string();
    let defnode = cmd.def.expect("AT_AlterColumnType ColumnDef");
    let def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
    let tn = def.typeName.expect("ColumnDef.typeName").as_variant::<TypeName>().expect("TypeName");

    let with_pos = |e: PgError, location: i32| -> Box<PgError> {
        let pos = parser_small1::parser_errposition_source(
            Some(query_string.as_bytes()),
            location,
            mbutils::GetDatabaseEncoding(),
        );
        Box::new(if pos > 0 { e.with_cursor_position(pos) } else { e })
    };

    if rel_reloftype(rel.rd_id)? != InvalidOid && !recursing {
        return Err(with_pos(*typed_table_err("cannot alter column type of typed table"), def.location));
    }

    let Some((attnum, attinhcount)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        return Err(with_pos(*undefined_column(col_name, &relname), def.location));
    };
    if attnum <= 0 {
        return Err(with_pos(*cannot_alter_system_column(col_name), def.location));
    }
    let att = *rel.rd_att.attr(attnum as usize - 1);
    if att.attgenerated != 0 && (def.raw_default.is_some() || def.cooked_default.is_some()) {
        return Err(with_pos(
            PgError::new(
                ERROR,
                "cannot specify USING when altering type of generated column".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION)
            .with_detail(format!("Column \"{col_name}\" is a generated column.")),
            def.location,
        ));
    }
    if attinhcount > 0 && !recursing {
        return Err(with_pos(
            PgError::new(ERROR, format!("cannot alter inherited column \"{col_name}\""))
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            def.location,
        ));
    }
    let mut is_expr = false;
    let mut attset = types_nodes::Bitmapset::empty();
    attset.add_member(
        mcx,
        attnum as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
    )?;
    if crate::partition::has_partition_attrs(mcx, rel, &attset, &mut is_expr)? {
        let mut e = PgError::new(
            ERROR,
            format!(
                "cannot alter column \"{col_name}\" because it is part of the partition key of relation \"{relname}\""
            ),
        )
        .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION);
        let pos = parser_small1::parser_errposition_source(
            Some(query_string.as_bytes()),
            def.location,
            mbutils::GetDatabaseEncoding(),
        );
        if pos > 0 {
            e = e.with_cursor_position(pos);
        }
        return Err(Box::new(e));
    }
    let mut pstate = parser_small1::make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(str_arena(mcx, query_string)?.as_bytes());

    let (targettype, targettypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, Some(&pstate), tn)?;

    let aclresult = aclchk::object_aclcheck(
        types_core::TYPE_RELATION_ID,
        targettype,
        miscinit::GetUserId(),
        adt_acl::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        crate::aclcheck_error_type(aclresult, targettype)?;
    }

    let targetcollid =
        crate::GetColumnDefCollationPos(Some(query_string.as_bytes()), def, targettype)?;
    {
        let mut rowtypes: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
        rowtypes.push(rel.rd_rel.reltype);
        catalog_heap::CheckAttributeType(
            mcx,
            col_name,
            targettype,
            targetcollid,
            &mut rowtypes,
            if att.attgenerated == b'v' as i8 { catalog_heap::CHKATYPE_IS_VIRTUAL } else { 0 },
        )?;
    }

    // C builds no transform for virtual generated columns: no newval, no
    // rewrite of the column itself.
    let tab_relkind = wqueue[tabidx].relkind;
    if att.attgenerated != b'v' as i8
        && (tab_relkind == RELKIND_RELATION
            || tab_relkind == types_rel::RELKIND_PARTITIONED_TABLE)
    {
    let using = match (def.raw_default, def.cooked_default) {
        (Some(raw), _) => {
            let nsitem = parse_relation::addRangeTableEntryForRelation(
                mcx,
                &mut pstate,
                rel,
                types_rel::AccessShareLock,
                None,
                false,
                true,
            )?;
            parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;
            let transformed = parse_expr::transformExpr(
                mcx,
                &mut pstate,
                raw,
                parser_small1::ParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM,
            )?;
            // C transforms USING once against the altered table and stores
            // it in cooked_default (parse_utilcmd.c:3643-3648); recursion
            // maps the transformed tree per child (tablecmds.c:14626-14646,
            // incl. the whole-row reject) — never re-parses raw against a
            // child whose column set differs.
            // SAFETY: defnode is this command's own tree; the `def` shared
            // ref is not used after this point in this scope's reads of
            // raw/cooked_default.
            unsafe {
                defnode
                    .with_mut::<ColumnDef, _>(|d| d.cooked_default = Some(transformed))
                    .expect("ColumnDef");
            }
            Some(transformed)
        }
        (None, Some(cooked)) => Some(cooked),
        (None, None) => None,
    };
    let pre_transform = match using {
        Some(t) => t,
        None => Node::mk(
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
        )?,
    };
    let transform = match coerce::coerce_to_target_type(
        mcx,
        &mut pstate,
        pre_transform,
        parse_expr::expr_type(pre_transform),
        targettype,
        targettypmod,
        coerce::CoercionContext::COERCION_ASSIGNMENT,
        types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )? {
        Some(t) => t,
        None => {
            let want = format_type::format_type_be(targettype).unwrap_or_else(|_| "???".into());
            let e = if using.is_some() {
                PgError::new(
                    ERROR,
                    format!(
                        "result of USING clause for column \"{col_name}\" cannot be cast \
                         automatically to type {want}"
                    ),
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                .with_hint("You might need to add an explicit cast.".to_string())
            } else {
                let e = PgError::new(
                    ERROR,
                    format!("column \"{col_name}\" cannot be cast automatically to type {want}"),
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH);
                if att.attgenerated == 0 {
                    let withmod = format_type::format_type_with_typemod(targettype, targettypmod)
                        .unwrap_or_else(|_| "???".into());
                    let qcol = format_type::quote_identifier(col_name);
                    e.with_hint(format!(
                        "You might need to specify \"USING {qcol}::{withmod}\"."
                    ))
                } else {
                    e
                }
            };
            return Err(Box::new(e));
        }
    };
    parse_collate::assign_expr_collations(mcx, &mut pstate, transform)?;
    // expression_planner.
    let transform = clauses::eval_const_expressions(mcx, transform)?;
    wqueue[tabidx].newvals.push(NewColumnValue { attnum, expr: transform, is_generated: false });
    if at_column_change_requires_rewrite(transform, attnum)? {
        wqueue[tabidx].rewrite |= AT_REWRITE_COLUMN_REWRITE;
    }
    parser_small1::free_parsestate(pstate)?;
    } else if att.attgenerated != b'v' as i8
        && (def.raw_default.is_some() || def.cooked_default.is_some())
    {
        return Err(Box::new(
            PgError::new(ERROR, format!("\"{relname}\" is not a table"))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    // tablecmds.c:14548: storage-less relations and virtual columns check
    // composite-type uses now; stored ones check at rewrite time.
    if !types_rel::RELKIND_HAS_STORAGE(tab_relkind) || att.attgenerated == b'v' as i8 {
        find_composite_type_dependencies_rel(mcx, rel.rd_rel.reltype, rel)?;
    }

    // Manual recursion: attribute numbers in the USING expression must be
    // remapped per child, so ATSimpleRecursion cannot apply.
    if recurse {
        let (child_oids, child_numparents) =
            pg_inherits::find_all_inheritors_numparents(mcx, rel.rd_id, lockmode)?;
        for (i, &childrelid) in child_oids.iter().enumerate() {
            if childrelid == rel.rd_id {
                continue;
            }
            let numparents = child_numparents[i];
            let childrel = relation_seams::relation_open::call(mcx, childrelid, NoLock)?;
            catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
            let Some((_, childinhcount)) =
                attname_lookup(mcx, childrelid, col_name, false)?
            else {
                return Err(undefined_column(col_name, &childrel.name().to_string()));
            };
            if childinhcount as i32 > numparents {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "cannot alter inherited column \"{col_name}\" of relation \
                             \"{}\"",
                            childrel.name()
                        ),
                    )
                    .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
                ));
            }
            let def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
            let childcmd = match def.cooked_default {
                Some(_) => {
                    // C copyObject boundary: each child gets its own USING
                    // tree with remapped attnos.
                    let copy = copyfuncs::copy_object(mcx, cnode)?;
                    let copy_cooked = copy
                        .as_variant::<AlterTableCmd>()
                        .expect("AlterTableCmd")
                        .def
                        .expect("AT_AlterColumnType ColumnDef")
                        .as_variant::<ColumnDef>()
                        .expect("ColumnDef")
                        .cooked_default
                        .expect("cooked_default copied");
                    let attmap =
                        tupdesc::build_attrmap_by_name(mcx, childrel.descr(), rel.descr())?;
                    let (mapped, found_whole_row) = rewrite_manip::map_variable_attnos(
                        mcx, copy_cooked, 1, 0, &attmap, InvalidOid,
                    )?;
                    if found_whole_row {
                        return Err(Box::new(
                            PgError::new(
                                ERROR,
                                "cannot convert whole-row table reference".to_string(),
                            )
                            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                            .with_detail(
                                "USING expression contains a whole-row table reference."
                                    .to_string(),
                            ),
                        ));
                    }
                    let copy_def = copy
                        .as_variant::<AlterTableCmd>()
                        .expect("AlterTableCmd")
                        .def
                        .expect("AT_AlterColumnType ColumnDef");
                    // SAFETY: freshly copied tree; no derived refs live.
                    unsafe {
                        copy_def
                            .with_mut::<ColumnDef, _>(|d| d.cooked_default = Some(mapped))
                            .expect("ColumnDef");
                    }
                    copy
                }
                None => cnode,
            };
            ATPrepCmd(mcx, wqueue, &childrel, childcmd, false, true, lockmode, query_string)?;
            childrel.close(NoLock)?;
        }
    } else if !recursing && find_inheritance_children_exist(mcx, rel.rd_id)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "type of inherited column \"{col_name}\" must be changed in child \
                     tables too"
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }

    if tab_relkind == types_rel::RELKIND_COMPOSITE_TYPE {
        ATTypedTableRecursion(mcx, wqueue, rel, cnode, cmd.behavior, lockmode, query_string)?;
    }
    Ok(())
}

// ATTypedTableRecursion (tablecmds.c): propagate ALTER TYPE operations to
// the typed tables of that type, honoring RESTRICT/CASCADE.
fn ATTypedTableRecursion<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    behavior: types_nodes::parsenodes::DropBehavior,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    debug_assert!(rel.rd_rel.relkind == types_rel::RELKIND_COMPOSITE_TYPE);
    let children = crate::rename::find_typed_table_dependencies(
        mcx,
        rel.rd_rel.reltype,
        rel.name(),
        behavior,
    )?;
    for &childrelid in children.iter() {
        let childrel = relation_seams::relation_open::call(mcx, childrelid, lockmode)?;
        catalog_heap::CheckTableNotInUse(&childrel, "ALTER TABLE")?;
        ATPrepCmd(mcx, wqueue, &childrel, cnode, true, true, lockmode, query_string)?;
        childrel.close(NoLock)?;
    }
    Ok(())
}

// ATColumnChangeRequiresRewrite (tablecmds.c:14679).
const F_TIMESTAMPTZ_TIMESTAMP: types_core::Oid = 2027;
const F_TIMESTAMP_TIMESTAMPTZ: types_core::Oid = 2028;

fn at_column_change_requires_rewrite(expr: Node<'_>, varattno: AttrNumber) -> PgResult<bool> {
    let mut e = expr;
    loop {
        if let Some(v) = e.as_var() {
            return Ok(v.varattno != varattno);
        }
        if let Some(r) = e.as_variant::<types_nodes::primnodes::RelabelType>() {
            e = r.arg;
            continue;
        }
        if let Some(d) = e.as_variant::<types_nodes::primnodes::CoerceToDomain>() {
            if typcache::DomainHasConstraints(d.resulttype)? {
                return Ok(true);
            }
            e = d.arg;
            continue;
        }
        if let Some(f) = e.as_func_expr() {
            match f.funcid {
                F_TIMESTAMPTZ_TIMESTAMP | F_TIMESTAMP_TIMESTAMPTZ => {
                    if adt_timestamp::TimestampTimestampTzRequiresRewrite() {
                        return Ok(true);
                    }
                    e = f.args.nth(0);
                    continue;
                }
                _ => return Ok(true),
            }
        }
        return Ok(true);
    }
}

// ATExecAlterColumnType: catalog half; dependent objects rebuild via
// ATPostAlterTypeCleanup.
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
    let targetcollid = crate::GetColumnDefCollation(def, targettype)?;

    // Re-coerce any stored default before the column type flips.
    let defaultexpr = if att.atthasdef {
        let defval = rewrite_handler::build_column_default(mcx, rel, attnum as usize)?
            .expect("atthasdef column has a default");
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

    RememberAllDependentForRebuilding(mcx, tab, rel, attnum, col_name, true)?;
    delete_column_type_dependencies(mcx, rel.rd_id, attnum, &att)?;

    let missing_elem = if att.atthasmissing && tab.rewrite == 0 {
        fetch_missing_element(mcx, rel.rd_id, attnum, &att)?
    } else {
        None
    };

    // C (ATExecAlterColumnType): attndims = list_length(typeName->arrayBounds),
    // guarded by the PG_INT16_MAX dimension cap. typenameTypeIdAndMod above
    // already resolved arrayBounds to the array type's OID, so an
    // `ALTER COLUMN c TYPE text[]` lands here with one bound entry.
    if tn.arrayBounds.len() > i16::MAX as usize {
        return Err(Box::new(
            PgError::new(ERROR, "too many array dimensions")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    let attndims = tn.arrayBounds.len() as i16;
    update_pg_attribute(
        mcx,
        rel.rd_id,
        attnum,
        &[
            (Anum_pg_attribute_atttypid, Datum::from_oid(targettype)),
            (Anum_pg_attribute_attlen, Datum::from_i16(shape.typlen)),
            (Anum_pg_attribute_atttypmod, Datum::from_i32(targettypmod)),
            (Anum_pg_attribute_attndims, Datum::from_i16(attndims)),
            (Anum_pg_attribute_attbyval, Datum::from_bool(shape.typbyval)),
            (Anum_pg_attribute_attalign, Datum::from_i8(shape.typalign)),
            (Anum_pg_attribute_attstorage, Datum::from_i8(shape.typstorage)),
            (Anum_pg_attribute_attcompression, Datum::from_i8(0)),
            (Anum_pg_attribute_attcollation, Datum::from_oid(targetcollid)),
        ],
    )?;

    // C repacks attmissingval in the same tuple write; two writes with a CCI
    // between reach the identical row image (array metadata swaps to the new
    // type, the element datum is untouched — no rewrite means binary reuse).
    if let Some(elem) = missing_elem {
        xact::CommandCounterIncrement()?;
        catalog_heap::StoreAttrMissingVal(mcx, rel, attnum, elem)?;
    }

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

const ConstraintRelationId: Oid = 2606;
const ConstraintOidIndexId: Oid = 2667;
const ProcedureRelationId: Oid = 1255;
const RewriteRelationId: Oid = 2618;
const TriggerRelationId: Oid = 2620;
const PolicyRelationId: Oid = 3256;
const StatisticExtRelationId: Oid = 3381;
const PublicationRelRelationId: Oid = 6106;

fn depends_on_column_err(
    mcx: Mcx<'_>,
    msg: &str,
    obj: &pg_depend::ObjectAddress,
    col_name: &str,
) -> Box<PgError> {
    let desc = catalog_dependency::getObjectDescription(mcx, obj)
        .ok()
        .flatten()
        .unwrap_or_else(|| "???".to_string());
    Box::new(
        PgError::new(ERROR, msg.to_string())
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(format!("{desc} depends on column \"{col_name}\"")),
    )
}

fn RememberAllDependentForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
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
    let mut found: Vec<(Oid, Oid, i32)> = Vec::new();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let classid =
            unsafe { types_tuple::heap_getattr(tup, 1, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let objid = unsafe { types_tuple::heap_getattr(tup, 2, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let objsubid =
            unsafe { types_tuple::heap_getattr(tup, 3, desc, &mut isnull) }.as_i32();
        found.push((classid, objid, objsubid));
    }
    genam::systable_endscan(mcx, scan)?;
    dep_rel.close(RowExclusiveLock)?;

    for (classid, objid, objsubid) in found {
        let obj = pg_depend::ObjectAddress::sub_set(classid, objid, objsubid);
        match classid {
            RELATION_RELATION_ID => {
                let relkind = lsyscache::get_rel_relkind(objid)? as u8;
                if relkind == types_rel::RELKIND_INDEX
                    || relkind == types_rel::RELKIND_PARTITIONED_INDEX
                {
                    RememberIndexForRebuilding(mcx, tab, objid)?;
                } else if relkind == types_rel::RELKIND_SEQUENCE {
                    // A SERIAL column's sequence; nothing to do.
                } else {
                    panic!("unexpected object depending on column: class {classid} oid {objid}");
                }
            }
            ConstraintRelationId => RememberConstraintForRebuilding(mcx, tab, objid)?,
            ProcedureRelationId => {
                if is_alter_type {
                    return Err(depends_on_column_err(
                        mcx,
                        "cannot alter type of a column used by a function or procedure",
                        &obj,
                        col_name,
                    ));
                }
            }
            RewriteRelationId => {
                if is_alter_type {
                    return Err(depends_on_column_err(
                        mcx,
                        "cannot alter type of a column used by a view or rule",
                        &obj,
                        col_name,
                    ));
                }
            }
            TriggerRelationId => {
                if is_alter_type {
                    return Err(depends_on_column_err(
                        mcx,
                        "cannot alter type of a column used in a trigger definition",
                        &obj,
                        col_name,
                    ));
                }
            }
            PolicyRelationId => {
                if is_alter_type {
                    return Err(depends_on_column_err(
                        mcx,
                        "cannot alter type of a column used in a policy definition",
                        &obj,
                        col_name,
                    ));
                }
            }
            types_core::ATTR_DEFAULT_RELATION_ID => {
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
            StatisticExtRelationId => {
                RememberStatisticsForRebuilding(mcx, tab, objid)?;
            }
            PublicationRelRelationId => {
                if is_alter_type {
                    return Err(depends_on_column_err(
                        mcx,
                        "cannot alter type of a column used by a publication WHERE clause",
                        &obj,
                        col_name,
                    ));
                }
            }
            _ => panic!("unexpected object depending on column: class {classid} oid {objid}"),
        }
    }
    Ok(())
}

fn RememberConstraintForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    conoid: Oid,
) -> PgResult<()> {
    if tab.changed_constraints.iter().any(|(o, _)| *o == conoid) {
        return Ok(());
    }
    let defstring = ruleutils::pg_get_constraintdef_command(mcx, conoid)?;
    // Not-null constraints must be recreated ahead of primary key indexes,
    // else the PK would create them under the wrong name.
    if lsyscache::get_constraint_type(conoid)? as u8 == pg_constraint::CONSTRAINT_NOTNULL {
        tab.changed_constraints.insert(0, (conoid, defstring));
    } else {
        tab.changed_constraints.push((conoid, defstring));
    }
    let indoid = lsyscache::get_constraint_index(conoid)?;
    if indoid != InvalidOid {
        RememberReplicaIdentityForRebuilding(mcx, tab, indoid)?;
        RememberClusterOnForRebuilding(mcx, tab, indoid)?;
    }
    Ok(())
}

fn RememberIndexForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    indoid: Oid,
) -> PgResult<()> {
    if tab.changed_indexes.iter().any(|(o, _)| *o == indoid) {
        return Ok(());
    }
    let conoid = pg_depend::get_index_constraint(mcx, indoid)?;
    if conoid != InvalidOid {
        return RememberConstraintForRebuilding(mcx, tab, conoid);
    }
    let defstring = ruleutils::pg_get_indexdef_string(mcx, indoid)?;
    tab.changed_indexes.push((indoid, defstring));
    RememberReplicaIdentityForRebuilding(mcx, tab, indoid)?;
    RememberClusterOnForRebuilding(mcx, tab, indoid)
}

// RememberStatisticsForRebuilding (tablecmds.c:15407): capture the definition
// string before any of the type changes apply.
fn RememberStatisticsForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    stxoid: Oid,
) -> PgResult<()> {
    if tab.changed_statistics.iter().any(|(o, _)| *o == stxoid) {
        return Ok(());
    }
    let defstring = ruleutils::pg_get_statisticsobj_worker(mcx, stxoid, false, false)?
        .unwrap_or_else(|| panic!("cache lookup failed for statistics object {stxoid}"));
    tab.changed_statistics.push((stxoid, defstring));
    Ok(())
}

fn RememberReplicaIdentityForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    indoid: Oid,
) -> PgResult<()> {
    if !lsyscache::get_index_isreplident(indoid)? {
        return Ok(());
    }
    if tab.replica_identity_index.is_some() {
        panic!("relation {} has multiple indexes marked as replica identity", tab.relid);
    }
    let name = lsyscache::get_rel_name(mcx, indoid)?.expect("index has a name");
    tab.replica_identity_index = Some(name.to_string());
    Ok(())
}

fn RememberClusterOnForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    indoid: Oid,
) -> PgResult<()> {
    if !lsyscache::get_index_isclustered(indoid)? {
        return Ok(());
    }
    if tab.cluster_on_index.is_some() {
        panic!("relation {} has multiple clustered indexes", tab.relid);
    }
    let name = lsyscache::get_rel_name(mcx, indoid)?.expect("index has a name");
    tab.cluster_on_index = Some(name.to_string());
    Ok(())
}

fn ATPostAlterTypeCleanup<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    tabidx: usize,
) -> PgResult<()> {
    if wqueue[tabidx].changed_constraints.is_empty()
        && wqueue[tabidx].changed_indexes.is_empty()
        && wqueue[tabidx].changed_statistics.is_empty()
        && wqueue[tabidx].replica_identity_index.is_none()
        && wqueue[tabidx].cluster_on_index.is_none()
    {
        return Ok(());
    }
    let tab_relid = wqueue[tabidx].relid;
    let tab_rewrite = wqueue[tabidx].rewrite;
    let changed_constraints = std::mem::take(&mut wqueue[tabidx].changed_constraints);
    let changed_indexes = std::mem::take(&mut wqueue[tabidx].changed_indexes);
    let changed_statistics = std::mem::take(&mut wqueue[tabidx].changed_statistics);
    let mut objects = catalog_dependency::ObjectAddresses::new();
    for (conoid, def) in &changed_constraints {
        let (conrelid, contypid, confrelid, conislocal) = constraint_rebuild_shape(mcx, *conoid)?;
        objects
            .add_exact_object_address(pg_depend::ObjectAddress::set(ConstraintRelationId, *conoid));
        if !conislocal {
            continue;
        }
        let relid = if conrelid != InvalidOid {
            conrelid
        } else {
            // must be a domain constraint
            let relid = lsyscache::get_typ_typrelid(lsyscache::getBaseType(contypid)?)?;
            if relid == InvalidOid {
                panic!("could not identify relation associated with constraint {conoid}");
            }
            relid
        };
        // AccessExclusiveLock: the DROP CONSTRAINT below needs it anyway.
        if relid != tab_relid {
            lmgr::LockRelationOid(relid, AccessExclusiveLock)?;
        }
        ATPostAlterTypeParse(mcx, wqueue, *conoid, relid, confrelid, def, tab_rewrite)?;
    }
    for (indoid, def) in &changed_indexes {
        let relid = catalog_index::IndexGetRelation(mcx, *indoid, false)?;
        if relid != tab_relid {
            lmgr::LockRelationOid(relid, AccessExclusiveLock)?;
        }
        ATPostAlterTypeParse(mcx, wqueue, *indoid, relid, InvalidOid, def, tab_rewrite)?;
        objects
            .add_exact_object_address(pg_depend::ObjectAddress::set(RELATION_RELATION_ID, *indoid));
    }
    for (stxoid, def) in &changed_statistics {
        let relid = statscmds::StatisticsGetRelation(*stxoid, false)?;
        // ShareUpdateExclusiveLock aligns with CreateStatistics and
        // RemoveStatisticsById; taken after all AccessExclusiveLock cases.
        if relid != tab_relid {
            lmgr::LockRelationOid(relid, ShareUpdateExclusiveLock)?;
        }
        ATPostAlterTypeParse(mcx, wqueue, *stxoid, relid, InvalidOid, def, tab_rewrite)?;
        objects.add_exact_object_address(pg_depend::ObjectAddress::set(
            StatisticExtRelationId,
            *stxoid,
        ));
    }
    if let Some(idxname) = wqueue[tabidx].replica_identity_index.take() {
        let mut sub = Node::build::<types_nodes::parsenodes::ReplicaIdentityStmt>(mcx)?;
        sub.identity_type = types_nodes::parsenodes::REPLICA_IDENTITY_INDEX;
        sub.name = Some(str_arena(mcx, &idxname)?);
        let subnode = sub.seal();
        let mut cmd = Node::build::<AlterTableCmd>(mcx)?;
        cmd.subtype = AlterTableType::AT_ReplicaIdentity;
        cmd.def = Some(subnode);
        wqueue[tabidx].subcmds[AT_PASS_OLD_CONSTR].lappend(mcx, cmd.seal())?;
    }
    if let Some(idxname) = wqueue[tabidx].cluster_on_index.take() {
        let mut cmd = Node::build::<AlterTableCmd>(mcx)?;
        cmd.subtype = AlterTableType::AT_ClusterOn;
        cmd.name = Some(str_arena(mcx, &idxname)?);
        wqueue[tabidx].subcmds[AT_PASS_OLD_CONSTR].lappend(mcx, cmd.seal())?;
    }
    catalog_dependency::performMultipleDeletions(
        mcx,
        &objects,
        catalog_dependency::DropBehavior::DROP_RESTRICT,
        catalog_dependency::PERFORM_DELETION_INTERNAL,
    )
}

fn constraint_rebuild_shape(mcx: Mcx<'_>, conoid: Oid) -> PgResult<(Oid, Oid, Oid, bool)> {
    let con_rel = table::table_open(mcx, ConstraintRelationId, types_rel::AccessShareLock)?;
    let keys = [oid_scankey(1, conoid)];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, ConstraintOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {conoid}"));
    let desc = con_rel.descr();
    let get = |anum: AttrNumber| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
        unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
    };
    let conrelid = get(pg_constraint::Anum_pg_constraint_conrelid).as_oid();
    let contypid = get(pg_constraint::Anum_pg_constraint_contypid).as_oid();
    let confrelid = get(pg_constraint::Anum_pg_constraint_confrelid).as_oid();
    let conislocal = get(pg_constraint::Anum_pg_constraint_conislocal).as_bool();
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok((conrelid, contypid, confrelid, conislocal))
}

fn ATPostAlterTypeParse<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut Wqueue<'mcx>,
    old_id: Oid,
    rel_id: Oid,
    ref_rel_id: Oid,
    def: &str,
    rewrite: i32,
) -> PgResult<()> {
    let def = str_arena(mcx, def)?;
    let raw_list = parser_seams::raw_parser::call(
        mcx,
        def,
        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
    )?;
    // C relation_open (tablecmds.c:15680): rel_id can be a composite type's
    // relation when a domain constraint depends on an altered attribute.
    let rel = relation_seams::relation_open::call(mcx, rel_id, NoLock)?;
    let qidx = ATGetQueueEntry(mcx, wqueue, &rel);
    let tab = &mut wqueue[qidx];
    for rs in raw_list.iter() {
        let stmt = rs.stmt.expect("RawStmt.stmt");
        if stmt.as_variant::<types_nodes::rawnodes::IndexStmt>().is_some() {
            parse_clause::transformIndexStmt(mcx, rel_id, stmt, def)?;
            if rewrite == 0 {
                TryReuseIndex(mcx, old_id, stmt)?;
            }
            readd_index_fixups(mcx, old_id, stmt)?;
            let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
            newcmd.subtype = AlterTableType::AT_ReAddIndex;
            newcmd.def = Some(stmt);
            tab.subcmds[AT_PASS_OLD_INDEX].lappend(mcx, newcmd.seal())?;
        } else if let Some(atstmt) = stmt.as_variant::<AlterTableStmt>() {
            for cnode in atstmt.cmds.iter() {
                let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
                if cmd.subtype != AlterTableType::AT_AddConstraint {
                    panic!("unexpected statement subtype: {:?}", cmd.subtype);
                }
                let connode = cmd.def.expect("AT_AddConstraint Constraint");
                let con = connode.as_variant::<Constraint>().expect("Constraint");
                match con.contype {
                    ConstrType::CONSTR_PRIMARY
                    | ConstrType::CONSTR_UNIQUE
                    | ConstrType::CONSTR_EXCLUSION => {
                        let (istmt, nnconstraints) =
                            parse_utilcmd::transformIndexConstraintForAlter(
                                mcx, &rel, connode, def,
                            )?;
                        assert!(
                            istmt
                                .as_variant::<types_nodes::rawnodes::IndexStmt>()
                                .expect("IndexStmt")
                                .indexOid
                                == InvalidOid,
                            "re-added constraint names an existing index"
                        );
                        parse_clause::transformIndexStmt(mcx, rel_id, istmt, def)?;
                        let indoid = lsyscache::get_constraint_index(old_id)?;
                        if rewrite == 0 {
                            TryReuseIndex(mcx, indoid, istmt)?;
                        }
                        readd_index_fixups(mcx, indoid, istmt)?;
                        let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
                        newcmd.subtype = AlterTableType::AT_ReAddIndex;
                        newcmd.def = Some(istmt);
                        tab.subcmds[AT_PASS_OLD_INDEX].lappend(mcx, newcmd.seal())?;
                        let idxname = istmt
                            .as_variant::<types_nodes::rawnodes::IndexStmt>()
                            .expect("IndexStmt")
                            .idxname
                            .expect("re-added constraint index has a name");
                        RebuildConstraintComment(
                            mcx,
                            tab,
                            AT_PASS_OLD_INDEX,
                            old_id,
                            Some(&rel),
                            None,
                            idxname,
                        )?;
                        for nn in nnconstraints.iter() {
                            let mut nncmd = Node::build::<AlterTableCmd>(mcx)?;
                            nncmd.subtype = AlterTableType::AT_ReAddConstraint;
                            nncmd.def = Some(nn);
                            tab.subcmds[AT_PASS_OLD_CONSTR].lappend(mcx, nncmd.seal())?;
                        }
                    }
                    ConstrType::CONSTR_CHECK
                    | ConstrType::CONSTR_NOTNULL
                    | ConstrType::CONSTR_FOREIGN => {
                        let contype = con.contype;
                        let conname = con.conname;
                        if contype == ConstrType::CONSTR_FOREIGN {
                            let old_pfeqop = if rewrite == 0 && tab.rewrite == 0 {
                                Some(TryReuseForeignKey(mcx, old_id)?)
                            } else {
                                None
                            };
                            // SAFETY: parse tree is arena-owned; no derived
                            // refs live.
                            unsafe {
                                connode
                                    .with_mut::<Constraint, _>(|c| {
                                        c.old_pktable_oid = ref_rel_id;
                                        if let Some(l) = old_pfeqop {
                                            c.old_conpfeqop = l;
                                        }
                                    })
                                    .expect("Constraint");
                            }
                        }
                        // SAFETY: as above.
                        unsafe {
                            cnode
                                .with_mut::<AlterTableCmd, _>(|c| {
                                    c.subtype = AlterTableType::AT_ReAddConstraint;
                                })
                                .expect("AlterTableCmd");
                        }
                        tab.subcmds[AT_PASS_OLD_CONSTR].lappend(mcx, cnode)?;
                        if let Some(conname) = conname {
                            RebuildConstraintComment(
                                mcx,
                                tab,
                                AT_PASS_OLD_CONSTR,
                                old_id,
                                Some(&rel),
                                None,
                                conname,
                            )?;
                        } else {
                            debug_assert!(contype == ConstrType::CONSTR_NOTNULL);
                        }
                    }
                    other => panic!("unexpected constraint type: {other:?}"),
                }
            }
        } else if let Some(adstmt) = stmt.as_variant::<types_nodes::parsenodes::AlterDomainStmt>() {
            if adstmt.subtype != b'C' {
                panic!("unexpected statement subtype: {}", adstmt.subtype);
            }
            let con = adstmt
                .def
                .expect("ALTER DOMAIN ADD CONSTRAINT def")
                .as_variant::<Constraint>()
                .expect("Constraint");
            let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
            newcmd.subtype = AlterTableType::AT_ReAddDomainConstraint;
            newcmd.def = Some(stmt);
            tab.subcmds[AT_PASS_OLD_CONSTR].lappend(mcx, newcmd.seal())?;
            RebuildConstraintComment(
                mcx,
                tab,
                AT_PASS_OLD_CONSTR,
                old_id,
                None,
                Some(&adstmt.typeName),
                con.conname.expect("deparsed domain constraint has a name"),
            )?;
        } else if stmt.as_variant::<types_nodes::rawnodes::CreateStatsStmt>().is_some() {
            parse_clause::transformStatsStmt(mcx, rel_id, stmt, def)?;
            // keep the statistics object's comment
            let comment = commands_comment::GetComment(mcx, old_id, StatisticExtRelationId, 0)?;
            if let Some(c) = comment {
                let c = str_arena(mcx, c.as_str())?;
                // SAFETY: parse tree is arena-owned; no derived refs live.
                unsafe {
                    stmt.with_mut::<types_nodes::rawnodes::CreateStatsStmt, _>(|st| {
                        st.stxcomment = Some(c);
                    })
                    .expect("CreateStatsStmt");
                }
            }
            let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
            newcmd.subtype = AlterTableType::AT_ReAddStatistics;
            newcmd.def = Some(stmt);
            tab.subcmds[AT_PASS_MISC].lappend(mcx, newcmd.seal())?;
        } else {
            panic!("unexpected statement type in ATPostAlterTypeParse");
        }
    }
    rel.close(NoLock)
}

fn RebuildConstraintComment<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    pass: usize,
    objid: Oid,
    rel: Option<&Relation<'mcx>>,
    domname: Option<&NodeList<'mcx>>,
    conname: &str,
) -> PgResult<()> {
    let Some(comment_str) = commands_comment::GetComment(mcx, objid, ConstraintRelationId, 0)?
    else {
        return Ok(());
    };
    let mut cmd = Node::build::<types_nodes::parsenodes::CommentStmt>(mcx)?;
    let mut object = NodeList::nil();
    match rel {
        Some(rel) => {
            cmd.objtype = ObjectType::OBJECT_TABCONSTRAINT;
            let nspname = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
                .expect("relation namespace has a name");
            object.lappend(
                mcx,
                Node::mk(mcx, types_nodes::String { sval: str_arena(mcx, &nspname)? })?,
            )?;
            object.lappend(
                mcx,
                Node::mk(mcx, types_nodes::String { sval: str_arena(mcx, rel.name())? })?,
            )?;
        }
        None => {
            cmd.objtype = ObjectType::OBJECT_DOMCONSTRAINT;
            let domname = domname.expect("domain constraint carries its domain name");
            let mut names = NodeList::nil();
            for n in domname.iter() {
                names.lappend(mcx, n)?;
            }
            let tn = TypeName { names, typemod: -1, location: -1, ..Default::default() };
            object.lappend(mcx, Node::mk(mcx, tn)?)?;
        }
    }
    object.lappend(
        mcx,
        Node::mk(mcx, types_nodes::String { sval: str_arena(mcx, conname)? })?,
    )?;
    cmd.object = Some(Node::mk(mcx, object)?);
    cmd.comment = Some(str_arena(mcx, comment_str.as_str())?);
    let subnode = cmd.seal();
    let mut newcmd = Node::build::<AlterTableCmd>(mcx)?;
    newcmd.subtype = AlterTableType::AT_ReAddComment;
    newcmd.def = Some(subnode);
    tab.subcmds[pass].lappend(mcx, newcmd.seal())
}

// TryReuseIndex (tablecmds.c:15886).
fn TryReuseIndex<'mcx>(
    mcx: Mcx<'mcx>,
    old_id: Oid,
    stmt_node: Node<'mcx>,
) -> PgResult<()> {
    let stmt = stmt_node
        .as_variant::<types_nodes::rawnodes::IndexStmt>()
        .expect("IndexStmt");
    if !indexcmds_seams::check_index_compatible::call(
        mcx,
        old_id,
        stmt.accessMethod.expect("transformed IndexStmt has an AM"),
        &stmt.indexParams,
        &stmt.excludeOpNames,
        stmt.iswithoutoverlaps,
    )? {
        return Ok(());
    }
    let irel = indexam::index_open(mcx, old_id, NoLock)?;
    if irel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_INDEX {
        let old_number = irel.rd_rel.relfilenode;
        // SAFETY: parse tree is arena-owned; no derived refs live.
        unsafe {
            stmt_node
                .with_mut::<types_nodes::rawnodes::IndexStmt, _>(|s| {
                    s.oldNumber = old_number;
                    s.oldCreateSubid = 0;
                })
                .expect("IndexStmt");
        }
    }
    irel.close(NoLock)
}

// TryReuseForeignKey (tablecmds.c:15915): stash the old P-F equality
// operators for the revalidation-skip test in ATAddForeignKeyConstraint.
fn TryReuseForeignKey<'mcx>(
    mcx: Mcx<'mcx>,
    old_id: Oid,
) -> PgResult<types_nodes::list::OidList<'mcx>> {
    let con_rel = table::table_open(mcx, ConstraintRelationId, types_rel::AccessShareLock)?;
    let keys = [oid_scankey(1, old_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, ConstraintOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {old_id}"));
    let arrays = pg_constraint::DeconstructFkConstraintRow(mcx, tup, con_rel.descr())?;
    let list =
        types_nodes::list::OidList::from_slice(mcx, &arrays.pf_eq_oprs[..arrays.numfks])?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok(list)
}

fn readd_index_fixups<'mcx>(mcx: Mcx<'mcx>, old_id: Oid, stmt_node: Node<'mcx>) -> PgResult<()> {
    let idxcomment = match commands_comment::GetComment(mcx, old_id, RELATION_RELATION_ID, 0)? {
        Some(c) => Some(str_arena(mcx, c.as_str())?),
        None => None,
    };
    // SAFETY: parse tree is arena-owned; no derived refs live.
    unsafe {
        stmt_node
            .with_mut::<types_nodes::rawnodes::IndexStmt, _>(|s| {
                s.idxcomment = idxcomment;
                s.reset_default_tblspc = true;
            })
            .expect("IndexStmt");
    }
    Ok(())
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

const Anum_pg_attribute_attmissingval: usize = 25;

// Unwraps the 1-element attmissingval array under the OLD column type's
// metadata; the element is arena-copied (the scan tuple dies here).
fn fetch_missing_element<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    att: &types_tuple::FormData_pg_attribute,
) -> PgResult<Option<Datum>> {
    let attrrel =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, types_rel::AccessShareLock)?;
    let keys = [oid_scankey(1, relid), int2_key(5, attnum)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &attrrel,
        AttributeRelidNumIndexId,
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for attribute {attnum} of relation {relid}"));
    let desc = attrrel.descr();
    let mut isnull = false;
    // SAFETY: attmissingval under pg_attribute's descriptor.
    let d = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_attribute_attmissingval as i32, desc, &mut isnull)
    };
    let result = if isnull {
        None
    } else {
        let p = d.as_usize() as *const u8;
        // SAFETY: live anyarray varlena image through its extent.
        let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let payload = varlena::open_image(mcx, image)?;
        let body = payload.as_bytes();
        let total = body.len() + 4;
        let mut full: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total)?;
        mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
        mcx::vec_append_bytes(&mut full, body)?;
        let elems = datum::array_build::deconstruct_array_image(
            mcx,
            &full,
            att.attlen,
            att.attbyval,
            att.attalign as u8,
        )?;
        assert!(elems.len() == 1, "attmissingval with {} entries", elems.len());
        let v = elems[0];
        if att.attbyval {
            Some(v)
        } else {
            let src = v.as_usize() as *const u8;
            let len = if att.attlen > 0 {
                att.attlen as usize
            } else {
                debug_assert!(att.attlen == -1);
                // SAFETY: element datum points into `full`, a live varlena image.
                unsafe { types_tuple::varatt::varsize_any(src) }
            };
            // SAFETY: `len` bytes readable at src per the array image layout.
            let bytes = unsafe { core::slice::from_raw_parts(src, len) };
            let copy = mcx::slice_borrow_in(mcx, bytes)?;
            Some(Datum::from_usize(copy.as_ptr() as usize))
        }
    };
    genam::systable_endscan(mcx, scan)?;
    attrrel.close(types_rel::AccessShareLock)?;
    Ok(result)
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

#[track_caller]
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

pub(crate) const Anum_pg_class_reloftype: usize = 5;
const Anum_pg_class_relreplident: usize = 27;
const TableSpaceRelationId: Oid = 1213;
const GLOBALTABLESPACE_OID: Oid = 1664;

pub(crate) fn pg_class_read_attr(mcx: Mcx<'_>, relid: Oid, attnum: usize) -> PgResult<Datum> {
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
    // rd_indam->amcanunique: only the btree handler sets it; from_relam
    // covers non-builtin AMs over builtin handlers.
    let amcanunique = types_relscan::IndexAmKind::from_relam(index_rel.rd_rel.relam)
        == types_relscan::IndexAmKind::Btree;
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
pub(crate) fn check_of_type(mcx: Mcx<'_>, typeid: Oid) -> PgResult<()> {
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
    let (typeid, _typmod) =
        parse_utilcmd::typenameTypeIdAndModAllowComposite(mcx, None, of_typename)?;
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

    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        return Err(PgError::error(
            "ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into());
    }

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
    // RelationIsMapped: storage-bearing relations with relfilenode 0.
    if types_rel::RELKIND_HAS_STORAGE(rel.rd_rel.relkind)
        && rel.rd_rel.relfilenode == InvalidOid
    {
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
    // tablecmds.c:3723 — their local buffer manager cannot cope with a move.
    if rel.is_other_temp() {
        return Err(Box::new(
            PgError::error("cannot move temporary tables of other sessions")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(true)
}

// ATExecSetTableSpace (tablecmds.c:16853).
fn ATExecSetTableSpace<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
    new_tablespace: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let is_index = lsyscache::get_rel_relkind(table_oid)? as u8 == types_rel::RELKIND_INDEX;
    let rel = if is_index {
        indexam::index_open(mcx, table_oid, lockmode)?
    } else {
        table::table_open(mcx, table_oid, lockmode)?
    };
    if !CheckRelationTableSpaceMove(&rel, new_tablespace)? {
        return rel.close(NoLock);
    }
    let reltoastrelid = rel.rd_rel.reltoastrelid;
    let mut reltoastidxids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    if reltoastrelid != InvalidOid {
        let toast_rel = table::table_open(mcx, reltoastrelid, lockmode)?;
        reltoastidxids = relcache::indexlist::RelationGetIndexList(mcx, reltoastrelid)?;
        toast_rel.close(lockmode)?;
    }
    // Relfilenumbers are not unique across tablespaces within a database.
    let newrelfilenumber = catalog::GetNewRelFileNumber(
        mcx,
        new_tablespace,
        None,
        rel.rd_rel.relpersistence,
    )?;
    let mut newrlocator = rel.rd_locator.get();
    newrlocator.relNumber = newrelfilenumber;
    newrlocator.spcOid = new_tablespace;

    if is_index {
        index_copy_data(&rel, newrlocator)?;
    } else {
        tableam::table_relation_copy_data(&rel, &newrlocator)?;
    }

    SetRelationTableSpace(mcx, &rel, new_tablespace, newrelfilenumber)?;
    rel.rd_locator.set(newrlocator);
    relcache::invalidate::RelationAssumeNewRelfilelocator(&rel);
    rel.close(NoLock)?;
    xact::CommandCounterIncrement()?;

    if reltoastrelid != InvalidOid {
        ATExecSetTableSpace(mcx, reltoastrelid, new_tablespace, lockmode)?;
    }
    for &idx in reltoastidxids.iter() {
        ATExecSetTableSpace(mcx, idx, new_tablespace, lockmode)?;
    }
    Ok(())
}

const Anum_pg_class_oid_mv: usize = 1;
const Anum_pg_class_relnamespace_mv: usize = 3;
const Anum_pg_class_relowner_mv: usize = 6;
const Anum_pg_class_reltablespace_mv: usize = 9;
const Anum_pg_class_relisshared_mv: usize = 16;
const Anum_pg_class_relkind_mv: usize = 18;

// AlterObjectTypeCommandTag(objtype) restricted to the MoveAll object types
// (commandtag.rs consts CMDTAG_ALTER_{TABLE,INDEX,MATERIALIZED_VIEW}); the
// full mapping lives in the tcop::utility crate, which depends on this one.
fn move_all_command_tag(objtype: ObjectType) -> types_core::CommandTag {
    match objtype {
        ObjectType::OBJECT_INDEX => types_core::CommandTag(15),
        ObjectType::OBJECT_MATVIEW => types_core::CommandTag(18),
        _ => types_core::CommandTag(34),
    }
}

// AlterTableMoveAll (tablecmds.c:16984).
pub fn AlterTableMoveAll<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::AlterTableMoveAllStmt<'mcx>,
) -> PgResult<Oid> {
    if stmt.objtype != ObjectType::OBJECT_TABLE
        && stmt.objtype != ObjectType::OBJECT_INDEX
        && stmt.objtype != ObjectType::OBJECT_MATVIEW
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "only tables, indexes, and materialized views exist in tablespaces".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    let role_oids = user::roleSpecsToIds(mcx, &stmt.roles)?;

    let mut orig_tablespaceoid = commands_tablespace::get_tablespace_oid(
        mcx,
        stmt.orig_tablespacename.expect("AlterTableMoveAllStmt.orig_tablespacename"),
        false,
    )?;
    let mut new_tablespaceoid = commands_tablespace::get_tablespace_oid(
        mcx,
        stmt.new_tablespacename.expect("AlterTableMoveAllStmt.new_tablespacename"),
        false,
    )?;

    if orig_tablespaceoid == GLOBALTABLESPACE_OID || new_tablespaceoid == GLOBALTABLESPACE_OID {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot move relations in to or out of pg_global tablespace".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    if new_tablespaceoid != InvalidOid
        && new_tablespaceoid != init_small::globals::MyDatabaseTableSpace()
    {
        let aclresult = aclchk::object_aclcheck(
            TableSpaceRelationId,
            new_tablespaceoid,
            miscinit::GetUserId(),
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk::aclcheck_error(
                aclresult,
                ObjectType::OBJECT_TABLESPACE,
                stmt.new_tablespacename.unwrap_or(""),
            )?;
        }
    }

    if orig_tablespaceoid == init_small::globals::MyDatabaseTableSpace() {
        orig_tablespaceoid = InvalidOid;
    }
    if new_tablespaceoid == init_small::globals::MyDatabaseTableSpace() {
        new_tablespaceoid = InvalidOid;
    }
    if orig_tablespaceoid == new_tablespaceoid {
        return Ok(new_tablespaceoid);
    }

    let mut relations: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    {
        let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
        let key = oid_scankey(Anum_pg_class_reltablespace_mv, orig_tablespaceoid);
        let mut scan = genam::systable_beginscan(mcx, &pg_class, InvalidOid, false, None, &[key])?;
        while let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? {
            let desc = pg_class.descr();
            let mut isnull = false;
            let relOid = unsafe {
                types_tuple::heap_getattr(tuple, Anum_pg_class_oid_mv as i32, desc, &mut isnull)
            }
            .as_oid();
            let relnamespace = unsafe {
                types_tuple::heap_getattr(
                    tuple,
                    Anum_pg_class_relnamespace_mv as i32,
                    desc,
                    &mut isnull,
                )
            }
            .as_oid();
            let relisshared = unsafe {
                types_tuple::heap_getattr(
                    tuple,
                    Anum_pg_class_relisshared_mv as i32,
                    desc,
                    &mut isnull,
                )
            }
            .as_bool();
            let relkind = unsafe {
                types_tuple::heap_getattr(tuple, Anum_pg_class_relkind_mv as i32, desc, &mut isnull)
            }
            .as_char() as u8;
            let relowner = unsafe {
                types_tuple::heap_getattr(tuple, Anum_pg_class_relowner_mv as i32, desc, &mut isnull)
            }
            .as_oid();

            if catalog::IsCatalogNamespace(relnamespace)
                || relisshared
                || catalog_namespace::isAnyTempNamespace(relnamespace)?
                || catalog::IsToastNamespace(relnamespace)
            {
                continue;
            }

            let matches = match stmt.objtype {
                ObjectType::OBJECT_TABLE => {
                    relkind == RELKIND_RELATION || relkind == types_rel::RELKIND_PARTITIONED_TABLE
                }
                ObjectType::OBJECT_INDEX => {
                    relkind == types_rel::RELKIND_INDEX
                        || relkind == types_rel::RELKIND_PARTITIONED_INDEX
                }
                ObjectType::OBJECT_MATVIEW => relkind == types_rel::RELKIND_MATVIEW,
                _ => false,
            };
            if !matches {
                continue;
            }

            if !role_oids.is_empty() && !role_oids.iter().any(|&r| r == relowner) {
                continue;
            }

            if !aclchk::object_ownercheck(RELATION_RELATION_ID, relOid, miscinit::GetUserId())? {
                let relname = lsyscache::get_rel_name(mcx, relOid)?;
                aclchk::aclcheck_error(
                    aclchk::ACLCHECK_NOT_OWNER,
                    crate::get_relkind_objtype(relkind),
                    relname.as_deref().unwrap_or(""),
                )?;
            }

            if stmt.nowait {
                if !lmgr::ConditionalLockRelationOid(relOid, types_rel::AccessExclusiveLock)? {
                    let nspname = lsyscache::get_namespace_name(mcx, relnamespace)?;
                    let relname = lsyscache::get_rel_name(mcx, relOid)?;
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "aborting because lock on relation \"{}.{}\" is not available",
                                nspname.as_deref().unwrap_or(""),
                                relname.as_deref().unwrap_or("")
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_OBJECT_IN_USE),
                    ));
                }
            } else {
                lmgr::LockRelationOid(relOid, types_rel::AccessExclusiveLock)?;
            }

            relations.push(relOid);
        }
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
    }

    if relations.is_empty() {
        let tsname = if orig_tablespaceoid == InvalidOid {
            "(database default)"
        } else {
            stmt.orig_tablespacename.unwrap_or("")
        };
        elog_seams::ereport_msg::call(
            NOTICE,
            format!("no matching relations in tablespace \"{tsname}\" found"),
            None,
        )?;
    }

    let tag = move_all_command_tag(stmt.objtype);
    for &relid in relations.iter() {
        let mut cmd = Node::build::<AlterTableCmd>(mcx)?;
        cmd.subtype = AlterTableType::AT_SetTableSpace;
        cmd.name = stmt.new_tablespacename;
        let cmds = NodeList::make1(mcx, cmd.seal())?;

        event_trigger::EventTriggerAlterTableStart(tag);
        let res = AlterTableInternal(mcx, relid, &cmds, false);
        event_trigger::EventTriggerAlterTableEnd();
        res?;
    }

    Ok(new_tablespaceoid)
}

// index_copy_data (tablecmds.c:17103).
fn index_copy_data(
    rel: &Relation<'_>,
    newrlocator: types_storage::RelFileLocator,
) -> PgResult<()> {
    let src = types_storage::RelFileLocatorBackend {
        locator: rel.rd_locator.get(),
        backend: rel.rd_backend,
    };
    smgr::smgropen(src.locator, src.backend)?;
    bufmgr_seams::flush_relation_buffers::call(src)?;
    let persistence = rel.rd_rel.relpersistence;
    let dstrel = catalog_storage::RelationCreateStorage(newrlocator, persistence, true)?;
    use types_core::primitive::ForkNumber;
    catalog_storage::RelationCopyStorage(src, dstrel, ForkNumber::MAIN_FORKNUM, persistence)?;
    for fork_i in ForkNumber::MAIN_FORKNUM as i32 + 1..=types_core::MAX_FORKNUM as i32 {
        let fork = ForkNumber::from_i32(fork_i).expect("valid fork number");
        if smgr::smgrexists(src, fork)? {
            smgr::smgrcreate(dstrel, fork, false)?;
            if rel.is_permanent()
                || (persistence == types_core::catalog::RELPERSISTENCE_UNLOGGED
                    && fork == ForkNumber::INIT_FORKNUM)
            {
                catalog_storage::log_smgrcreate(&newrlocator, fork)?;
            }
            catalog_storage::RelationCopyStorage(src, dstrel, fork, persistence)?;
        }
    }
    catalog_storage::RelationDropStorage(rel)?;
    smgr::smgrclose(dstrel)
}

// SetRelationTableSpace (tablecmds.c:3750).
fn SetRelationTableSpace<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_tablespace: Oid,
    new_relfilenumber: Oid,
) -> PgResult<()> {
    const Anum_pg_class_relfilenode: usize = 8;
    const Anum_pg_class_reltablespace: usize = 9;
    let reloid = rel.rd_id;
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(1, reloid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {reloid}"));
    // C: SearchSysCacheLockedCopy1 (tablecmds.c:3765) / UnlockTuple (:3777).
    // Taken before the content read that feeds the replacement image.
    let otid = tup.t_self;
    lmgr::LockTuple(&pg_class, &otid, InplaceUpdateTupleLock)?;
    let desc = pg_class.descr();
    let natts = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    let stored_spc = if new_tablespace == init_small::globals::MyDatabaseTableSpace() {
        InvalidOid
    } else {
        new_tablespace
    };
    values[Anum_pg_class_reltablespace - 1] = Datum::from_oid(stored_spc);
    replace[Anum_pg_class_reltablespace - 1] = true;
    if new_relfilenumber != InvalidOid {
        values[Anum_pg_class_relfilenode - 1] = Datum::from_oid(new_relfilenumber);
        replace[Anum_pg_class_relfilenode - 1] = true;
    }
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    lmgr::UnlockTuple(&pg_class, &otid, InplaceUpdateTupleLock)?;
    // Tablespace dependency is only recorded for storage-less relations
    // (tablecmds.c:3782).
    if !types_rel::RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        pg_shdepend::changeDependencyOnTablespace(mcx, RELATION_RELATION_ID, reloid, stored_spc)?;
    }
    pg_class.close(RowExclusiveLock)
}

// ATExecSetTableSpaceNoStorage (tablecmds.c:16997): catalog-only
// reltablespace change for partitioned tables and indexes.
fn ATExecSetTableSpaceNoStorage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_tablespace: Oid,
) -> PgResult<()> {
    debug_assert!(!types_rel::RELKIND_HAS_STORAGE(rel.rd_rel.relkind));
    if !CheckRelationTableSpaceMove(rel, new_tablespace)? {
        return Ok(());
    }
    SetRelationTableSpace(mcx, rel, new_tablespace, InvalidOid)?;
    xact::CommandCounterIncrement()
}

// ATPrepSetAccessMethod (tablecmds.c:16491).
fn ATPrepSetAccessMethod<'mcx>(
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    amname: Option<&str>,
) -> PgResult<()> {
    // DEFAULT on a partitioned table resets the catalogued AM to InvalidOid.
    let amoid = match amname {
        Some(name) => commands_amcmds::get_table_am_oid(name, false)?,
        None if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE => InvalidOid,
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

// ATExecSetAccessMethodNoStorage (tablecmds.c:16525): catalog-only relam
// change for partitioned tables, with the pg_am dependency kept in step.
fn ATExecSetAccessMethodNoStorage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_access_method: Oid,
) -> PgResult<()> {
    const Anum_pg_class_relam: usize = 7;
    debug_assert!(!types_rel::RELKIND_HAS_STORAGE(rel.rd_rel.relkind));
    let old_access_method = rel.rd_rel.relam;
    if old_access_method == new_access_method {
        return Ok(());
    }
    set_pg_class_datum(mcx, rel.rd_id, Anum_pg_class_relam, Datum::from_oid(new_access_method))?;
    if old_access_method == InvalidOid {
        pg_depend::recordDependencyOn(
            mcx,
            &pg_depend::ObjectAddress::set(RELATION_RELATION_ID, rel.rd_id),
            &pg_depend::ObjectAddress::set(
                commands_amcmds::AccessMethodRelationId,
                new_access_method,
            ),
            pg_depend::DependencyType::Normal,
        )?;
    } else if new_access_method == InvalidOid {
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            RELATION_RELATION_ID,
            rel.rd_id,
            commands_amcmds::AccessMethodRelationId,
            pg_depend::DependencyType::Normal,
        )?;
    } else {
        pg_depend::changeDependencyFor(
            mcx,
            RELATION_RELATION_ID,
            rel.rd_id,
            commands_amcmds::AccessMethodRelationId,
            old_access_method,
            new_access_method,
        )?;
    }
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
// find_composite_type_dependencies (tablecmds.c).
pub(crate) enum CompositeDepOrigin {
    TypeName(String),
    Relation { relname: String, relkind: u8 },
}

pub fn find_composite_type_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    orig_type_name: &str,
) -> PgResult<()> {
    find_composite_type_dependencies_impl(
        mcx,
        type_oid,
        &CompositeDepOrigin::TypeName(orig_type_name.to_string()),
    )
}

pub(crate) fn find_composite_type_dependencies_rel<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    orig_rel: &Relation<'mcx>,
) -> PgResult<()> {
    find_composite_type_dependencies_impl(
        mcx,
        type_oid,
        &CompositeDepOrigin::Relation {
            relname: orig_rel.name().to_string(),
            relkind: orig_rel.rd_rel.relkind,
        },
    )
}

fn find_composite_type_dependencies_impl<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    origin: &CompositeDepOrigin,
) -> PgResult<()> {
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, types_rel::AccessShareLock)?;
    const Anum_pg_depend_classid: usize = 1;
    const Anum_pg_depend_objid: usize = 2;
    const Anum_pg_depend_objsubid: usize = 3;
    const Anum_pg_depend_refclassid: usize = 4;
    const Anum_pg_depend_refobjid: usize = 5;
    let keys = [
        oid_scankey(Anum_pg_depend_refclassid, TYPE_RELATION_ID),
        oid_scankey(Anum_pg_depend_refobjid, type_oid),
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
        let get = |anum: usize| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_depend columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        let classid = get(Anum_pg_depend_classid).as_oid();
        let objid = get(Anum_pg_depend_objid).as_oid();
        let objsubid = get(Anum_pg_depend_objsubid).as_i32();
        if classid == TYPE_RELATION_ID {
            find_composite_type_dependencies_impl(mcx, objid, origin)?;
            continue;
        }
        if classid != RELATION_RELATION_ID {
            continue;
        }
        let rel = relation_seams::relation_open::call(mcx, objid, types_rel::AccessShareLock)?;
        let natts = rel.rd_att.natts as i32;
        let mut attname: Option<String> = None;
        if objsubid > 0 && objsubid <= natts {
            let att = rel.rd_att.attr(objsubid as usize - 1);
            attname =
                Some(core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8").into());
        } else {
            for attno in 1..=natts {
                let att = rel.rd_att.attr(attno as usize - 1);
                if att.atttypid == type_oid && !att.attisdropped {
                    attname = Some(
                        core::str::from_utf8(att.attname.name_str())
                            .expect("attname UTF-8")
                            .into(),
                    );
                    break;
                }
            }
            if attname.is_none() {
                rel.close(types_rel::AccessShareLock)?;
                continue;
            }
        }
        if types_rel::RELKIND_HAS_STORAGE(rel.rd_rel.relkind)
            || matches!(rel.rd_rel.relkind, b'p' | b'I')
        {
            let relname = rel.name().to_string();
            let colname = attname.expect("column resolved above");
            let msg = match origin {
                CompositeDepOrigin::TypeName(name) => format!(
                    "cannot alter type \"{name}\" because column \
                     \"{relname}.{colname}\" uses it"
                ),
                CompositeDepOrigin::Relation { relname: origname, relkind } => match *relkind {
                    types_rel::RELKIND_COMPOSITE_TYPE => format!(
                        "cannot alter type \"{origname}\" because column \
                         \"{relname}.{colname}\" uses it"
                    ),
                    types_rel::RELKIND_FOREIGN_TABLE => format!(
                        "cannot alter foreign table \"{origname}\" because column \
                         \"{relname}.{colname}\" uses its row type"
                    ),
                    _ => format!(
                        "cannot alter table \"{origname}\" because column \
                         \"{relname}.{colname}\" uses its row type"
                    ),
                },
            };
            return Err(Box::new(
                PgError::new(ERROR, msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        } else if rel.rd_rel.reltype != InvalidOid {
            find_composite_type_dependencies_impl(mcx, rel.rd_rel.reltype, origin)?;
        }
        rel.close(types_rel::AccessShareLock)?;
    }
    genam::systable_endscan(mcx, scan)?;
    dep_rel.close(types_rel::AccessShareLock)
}

// ATExecGenericOptions (tablecmds.c): ALTER FOREIGN TABLE ... OPTIONS (...).
fn ATExecGenericOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    options: &NodeList<'mcx>,
) -> PgResult<()> {
    use cache_syscache::cacheinfo::FOREIGNTABLEREL;
    use foreigncmds::foreign::{
        Anum_pg_foreign_table_ftoptions, Anum_pg_foreign_table_ftserver, Natts_pg_foreign_table,
        GetForeignDataWrapper, GetForeignServer,
    };

    if options.is_nil() {
        return Ok(());
    }
    let ftrel =
        table::table_open(mcx, types_core::FOREIGN_TABLE_RELATION_ID, RowExclusiveLock)?;
    let Some(tp) = cache_syscache::SearchSysCacheCopy(
        mcx,
        FOREIGNTABLEREL,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(rel.rd_id)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?
    else {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("foreign table \"{}\" does not exist", rel.name()),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    let getattr = |attnum: i32| -> (Datum, bool) {
        let mut isnull = false;
        // SAFETY: pg_foreign_table column of the copied tuple.
        let d = unsafe { types_tuple::heap_getattr(&tp, attnum, ftrel.descr(), &mut isnull) };
        (d, isnull)
    };
    let server = GetForeignServer(mcx, getattr(Anum_pg_foreign_table_ftserver).0.as_oid())?;
    let fdw = GetForeignDataWrapper(mcx, server.fdwid)?;

    let mut repl_val = [Datum::null(); Natts_pg_foreign_table];
    let mut repl_null = [false; Natts_pg_foreign_table];
    let mut repl_repl = [false; Natts_pg_foreign_table];

    let (datum, isnull) = getattr(Anum_pg_foreign_table_ftoptions);
    let old = if isnull { None } else { Some(datum) };
    let new_options = foreigncmds::options::transformGenericOptions(
        mcx,
        types_core::FOREIGN_TABLE_RELATION_ID,
        old,
        options,
        fdw.fdwvalidator,
    )?;
    match &new_options {
        Some(image) => {
            repl_val[Anum_pg_foreign_table_ftoptions as usize - 1] =
                Datum::from_usize(image.as_ptr() as usize)
        }
        None => repl_null[Anum_pg_foreign_table_ftoptions as usize - 1] = true,
    }
    repl_repl[Anum_pg_foreign_table_ftoptions as usize - 1] = true;

    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, &tp, ftrel.descr(), &repl_val, &repl_null, &repl_repl)?;
    let otid = tp.t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &ftrel, &otid, &mut newtup)?;

    ftrel.close(RowExclusiveLock)
}

// ATExecAlterColumnGenericOptions (tablecmds.c): ALTER FOREIGN TABLE ...
// ALTER COLUMN ... OPTIONS (...) over pg_attribute.attfdwoptions.
fn ATExecAlterColumnGenericOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colname: &str,
    options: &NodeList<'mcx>,
) -> PgResult<()> {
    use cache_syscache::cacheinfo::FOREIGNTABLEREL;
    use foreigncmds::foreign::{
        Anum_pg_foreign_table_ftserver, GetForeignDataWrapper, GetForeignServer,
    };

    const Anum_pg_attribute_attnum: i32 = 5;
    const Anum_pg_attribute_attfdwoptions: i32 = 24;
    const Natts_pg_attribute: usize = 25;

    if options.is_nil() {
        return Ok(());
    }
    let Some(fttp) = cache_syscache::SearchSysCache1(
        FOREIGNTABLEREL,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(rel.rd_id)),
    )?
    else {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("foreign table \"{}\" does not exist", rel.name()),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    let ftserver =
        cache_syscache::SysCacheGetAttrNotNull(FOREIGNTABLEREL, &fttp, Anum_pg_foreign_table_ftserver)?
            .as_oid();
    cache_syscache::ReleaseSysCache(fttp);
    let server = GetForeignServer(mcx, ftserver)?;
    let fdw = GetForeignDataWrapper(mcx, server.fdwid)?;

    let attrel =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let key1 = oid_scankey(1, rel.rd_id);
    let mut scan = genam::systable_beginscan(
        mcx,
        &attrel,
        AttributeRelidNumIndexId,
        true,
        None,
        &[key1],
    )?;
    let desc = attrel.descr();
    let mut found = false;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed pg_attribute columns under its own descriptor.
        let name = unsafe { types_tuple::heap_getattr(tup, 2, desc, &mut isnull) };
        let name = unsafe { core::slice::from_raw_parts(name.as_usize() as *const u8, 64) };
        let len = name.iter().position(|&b| b == 0).unwrap_or(64);
        if &name[..len] != colname.as_bytes() {
            continue;
        }
        let dropped = unsafe { types_tuple::heap_getattr(tup, 17, desc, &mut isnull) }.as_bool();
        if dropped {
            continue;
        }
        let attnum =
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_attribute_attnum, desc, &mut isnull) }
                .as_i16();
        if attnum <= 0 {
            genam::systable_endscan(mcx, scan)?;
            attrel.close(RowExclusiveLock)?;
            return Err(Box::new(
                PgError::new(ERROR, format!("cannot alter system column \"{colname}\""))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        let old_datum = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_attribute_attfdwoptions, desc, &mut isnull)
        };
        let old = if isnull { None } else { Some(old_datum) };
        let new_options = foreigncmds::options::transformGenericOptions(
            mcx,
            types_core::ATTRIBUTE_RELATION_ID,
            old,
            options,
            fdw.fdwvalidator,
        )?;
        let mut repl_val = [Datum::null(); Natts_pg_attribute];
        let mut repl_null = [false; Natts_pg_attribute];
        let mut repl_repl = [false; Natts_pg_attribute];
        match &new_options {
            Some(image) => {
                repl_val[Anum_pg_attribute_attfdwoptions as usize - 1] =
                    Datum::from_usize(image.as_ptr() as usize)
            }
            None => repl_null[Anum_pg_attribute_attfdwoptions as usize - 1] = true,
        }
        repl_repl[Anum_pg_attribute_attfdwoptions as usize - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_val, &repl_null, &repl_repl)?;
        let otid = tup.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &attrel, &otid, &mut newtup)?;
        found = true;
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    attrel.close(RowExclusiveLock)?;
    if !found {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "column \"{colname}\" of relation \"{}\" does not exist",
                    rel.name()
                ),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
        ));
    }
    Ok(())
}
