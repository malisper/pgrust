// CreateTriggerFiringOn (trigger.c): user CREATE [CONSTRAINT] TRIGGER (incl.
// OR REPLACE, UPDATE OF columns, WHEN, trigger args, transition-table names,
// partitioned-table recursion with tgparentid clones) and the internal RI
// constraint-trigger callers. LOUD: non-superuser ACL walks, non-identity
// partition attribute maps.
use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::{DependencyType, ObjectAddress};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::primnodes::{Alias, Var};
use types_nodes::rawnodes::CreateTrigStmt;
use types_nodes::{Node, NodeList};
use types_rel::{
    AccessShareLock, NoLock, Relation, RowExclusiveLock, ShareRowExclusiveLock,
    RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_trigger::{
    TRIGGER_FIRES_ON_ORIGIN, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE,
    TRIGGER_TYPE_EVENT_MASK, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_ROW,
    TRIGGER_TYPE_TIMING_MASK, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_UPDATE,
};

pub const TRIGGER_RELATION_ID: Oid = 2620;
pub const TRIGGER_OID_INDEX_ID: Oid = 2702;
pub const TRIGGER_RELID_NAME_INDEX_ID: Oid = 2701;

const PROCEDURE_RELATION_ID: Oid = 1255;
const CLASS_OID_INDEX_ID: Oid = 2662;
const TRIGGEROID: Oid = 2279;
const CONSTRAINT_TRIGGER: u8 = b't';
const PRS2_OLD_VARNO: i32 = 1;
const PRS2_NEW_VARNO: i32 = 2;

const Anum_pg_trigger_oid: AttrNumber = 1;
const Anum_pg_trigger_tgparentid: i32 = 3;
const Anum_pg_trigger_tgisinternal: i32 = 8;
const Anum_pg_trigger_tgconstraint: i32 = 11;
const Natts_pg_trigger: usize = 19;
const Anum_pg_class_relhastriggers: usize = 22;

pub struct InternalTriggerArgs<'a> {
    pub trigname_base: &'a str,
    pub relid: Oid,
    pub constrrelid: Oid,
    pub constraint_oid: Oid,
    pub index_oid: Oid,
    pub funcoid: Oid,
    pub tgtype: i16,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub parent_trigger_oid: Oid,
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: CreateTriggerFiringOn {what}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub fn CreateTriggerInternal<'mcx>(
    mcx: Mcx<'mcx>,
    args: &InternalTriggerArgs<'_>,
) -> PgResult<Oid> {
    let mut name_copy: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, args.trigname_base.len())?;
    mcx::vec_append_bytes(&mut name_copy, args.trigname_base.as_bytes())?;
    let trigname: &'mcx str = core::str::from_utf8(name_copy.leak()).expect("was UTF-8");
    let stmt = CreateTrigStmt {
        replace: false,
        isconstraint: true,
        trigname: Some(trigname),
        relation: None,
        funcname: NodeList::nil(),
        args: NodeList::nil(),
        row: args.tgtype & TRIGGER_TYPE_ROW != 0,
        timing: args.tgtype & TRIGGER_TYPE_TIMING_MASK,
        events: args.tgtype & TRIGGER_TYPE_EVENT_MASK,
        columns: NodeList::nil(),
        whenClause: None,
        transitionRels: NodeList::nil(),
        deferrable: args.deferrable,
        initdeferred: args.initdeferred,
        constrrel: None,
    };
    CreateTriggerFiringOn(
        mcx,
        &stmt,
        None,
        args.relid,
        args.constrrelid,
        args.constraint_oid,
        args.index_oid,
        args.funcoid,
        args.parent_trigger_oid,
        None,
        true,
        false,
        TRIGGER_FIRES_ON_ORIGIN,
    )
}

// get_relkind_objtype (objectaddress.c), the kinds reachable from trigger DDL.
fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    use types_nodes::parsenodes::ObjectType;
    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => ObjectType::OBJECT_TABLE,
        types_rel::RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}

// errdetail_relkind_not_supported (pg_class.c), triggerable-rel error slice.
pub(crate) fn relkind_not_supported_detail(relkind: u8) -> &'static str {
    match relkind {
        b'S' => "This operation is not supported for sequences.",
        b't' => "This operation is not supported for TOAST tables.",
        b'i' | b'I' => "This operation is not supported for indexes.",
        b'c' => "This operation is not supported for composite types.",
        b'm' => "This operation is not supported for materialized views.",
        other => unported(&format!("errdetail_relkind_not_supported '{}'", other as char)),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn CreateTriggerFiringOn<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    query_string: Option<&str>,
    rel_oid: Oid,
    ref_rel_oid: Oid,
    mut constraint_oid: Oid,
    index_oid: Oid,
    mut funcoid: Oid,
    parent_trigger_oid: Oid,
    when_clause: Option<Node<'mcx>>,
    is_internal: bool,
    in_partition: bool,
    fires_when: i8,
) -> PgResult<Oid> {
    let trigname_given = stmt.trigname.expect("CreateTrigStmt.trigname");
    let rel = if rel_oid != InvalidOid {
        table::table_open(mcx, rel_oid, ShareRowExclusiveLock)?
    } else {
        let rv = to_rangevar(stmt.relation.expect("CreateTrigStmt.relation"));
        table::table_openrv(mcx, &rv, ShareRowExclusiveLock)?
    };
    let relname = rel.name();

    match rel.rd_rel.relkind {
        RELKIND_RELATION => {
            if stmt.timing != TRIGGER_TYPE_BEFORE && stmt.timing != TRIGGER_TYPE_AFTER {
                return Err(Box::new(
                    (*err(format!("\"{relname}\" is a table"), ERRCODE_WRONG_OBJECT_TYPE))
                        .with_detail("Tables cannot have INSTEAD OF triggers.".to_string()),
                ));
            }
        }
        RELKIND_PARTITIONED_TABLE => {
            if stmt.timing != TRIGGER_TYPE_BEFORE && stmt.timing != TRIGGER_TYPE_AFTER {
                return Err(Box::new(
                    (*err(format!("\"{relname}\" is a table"), ERRCODE_WRONG_OBJECT_TYPE))
                        .with_detail("Tables cannot have INSTEAD OF triggers.".to_string()),
                ));
            }
            if stmt.row && !stmt.transitionRels.is_nil() {
                return Err(Box::new(
                    (*err(
                        format!("\"{relname}\" is a partitioned table"),
                        ERRCODE_FEATURE_NOT_SUPPORTED,
                    ))
                    .with_detail(
                        "ROW triggers with transition tables are not supported on partitioned \
                         tables."
                            .to_string(),
                    ),
                ));
            }
        }
        b'v' => {
            if stmt.timing != TRIGGER_TYPE_INSTEAD && stmt.row {
                return Err(Box::new(
                    (*err(format!("\"{relname}\" is a view"), ERRCODE_WRONG_OBJECT_TYPE))
                        .with_detail(
                            "Views cannot have row-level BEFORE or AFTER triggers."
                                .to_string(),
                        ),
                ));
            }
            if stmt.events & TRIGGER_TYPE_TRUNCATE != 0 {
                return Err(Box::new(
                    (*err(format!("\"{relname}\" is a view"), ERRCODE_WRONG_OBJECT_TYPE))
                        .with_detail("Views cannot have TRUNCATE triggers.".to_string()),
                ));
            }
        }
        RELKIND_FOREIGN_TABLE => {
            if stmt.timing != TRIGGER_TYPE_BEFORE && stmt.timing != TRIGGER_TYPE_AFTER {
                return Err(Box::new(
                    (*err(
                        format!("\"{relname}\" is a foreign table"),
                        ERRCODE_WRONG_OBJECT_TYPE,
                    ))
                    .with_detail("Foreign tables cannot have INSTEAD OF triggers.".to_string()),
                ));
            }
            if stmt.isconstraint {
                return Err(Box::new(
                    (*err(
                        format!("\"{relname}\" is a foreign table"),
                        ERRCODE_WRONG_OBJECT_TYPE,
                    ))
                    .with_detail("Foreign tables cannot have constraint triggers.".to_string()),
                ));
            }
        }
        other => {
            return Err(Box::new(
                (*err(
                    format!("relation \"{relname}\" cannot have triggers"),
                    ERRCODE_WRONG_OBJECT_TYPE,
                ))
                .with_detail(relkind_not_supported_detail(other as u8).to_string()),
            ));
        }
    }

    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&rel) {
        return Err(err(
            format!("permission denied: \"{relname}\" is a system catalog"),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let mut constrrelid = InvalidOid;
    if stmt.isconstraint {
        if ref_rel_oid != InvalidOid {
            lmgr::LockRelationOid(ref_rel_oid, AccessShareLock)?;
            constrrelid = ref_rel_oid;
        } else if let Some(crv) = stmt.constrrel {
            let rv = to_rangevar(crv);
            constrrelid =
                catalog_namespace::RangeVarGetRelidExtended(&rv, AccessShareLock, 0, None)?;
        }
    }

    // C: permission checks — ACL_TRIGGER on the relation (and the constraint
    // referenced relation); the function's ACL_EXECUTE check sits at the
    // funcoid lookup below, as in C.
    if !is_internal {
        let aclresult =
            aclchk::pg_class_aclcheck(rel.rd_id, miscinit::GetUserId(), adt_acl::ACL_TRIGGER)?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk::aclcheck_error(
                aclresult,
                get_relkind_objtype(rel.rd_rel.relkind as u8),
                relname,
            )?;
        }
        if constrrelid != InvalidOid {
            let aclresult =
                aclchk::pg_class_aclcheck(constrrelid, miscinit::GetUserId(), adt_acl::ACL_TRIGGER)?;
            if aclresult != aclchk::ACLCHECK_OK {
                let constrrelname = lsyscache::relation::get_rel_name(mcx, constrrelid)?
                    .map(|n| n.as_str().to_string())
                    .unwrap_or_default();
                aclchk::aclcheck_error(
                    aclresult,
                    get_relkind_objtype(lsyscache::relation::get_rel_relkind(constrrelid)? as u8),
                    &constrrelname,
                )?;
            }
        }
    }

    let partition_recurse =
        !is_internal && stmt.row && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    if partition_recurse {
        pg_inherits::find_all_inheritors(mcx, rel.rd_id, ShareRowExclusiveLock)?;
    }

    let mut tgtype: i16 = 0;
    if stmt.row {
        tgtype |= TRIGGER_TYPE_ROW;
    }
    tgtype |= stmt.timing;
    tgtype |= stmt.events;

    if tgtype & TRIGGER_TYPE_ROW != 0 && tgtype & TRIGGER_TYPE_TRUNCATE != 0 {
        return Err(err(
            "TRUNCATE FOR EACH ROW triggers are not supported".to_string(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    if tgtype & TRIGGER_TYPE_INSTEAD != 0 {
        if tgtype & TRIGGER_TYPE_ROW == 0 {
            return Err(err(
                "INSTEAD OF triggers must be FOR EACH ROW".to_string(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        if stmt.whenClause.is_some() {
            return Err(err(
                "INSTEAD OF triggers cannot have WHEN conditions".to_string(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        if !stmt.columns.is_nil() {
            return Err(err(
                "INSTEAD OF triggers cannot have column lists".to_string(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
    }

    let (oldtablename, newtablename) = validate_transition_rels(mcx, stmt, &rel, tgtype)?;

    let (when_node, qual, when_rtable) = match when_clause {
        Some(w) => (Some(w), Some(outfuncs::nodeToString(mcx, w)?), NodeList::nil()),
        None => match stmt.whenClause {
            Some(when) => {
                let (node, qual, rtable) =
                    transform_when_clause(mcx, &rel, when, tgtype, query_string)?;
                (Some(node), Some(qual), rtable)
            }
            None => (None, None, NodeList::nil()),
        },
    };

    if funcoid == InvalidOid {
        funcoid = lookup_trigger_func(mcx, &stmt.funcname)?;
    }
    if !is_internal {
        let aclresult = aclchk::object_aclcheck(
            PROCEDURE_RELATION_ID,
            funcoid,
            miscinit::GetUserId(),
            adt_acl::ACL_EXECUTE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk::aclcheck_error(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION,
                &name_list_to_string(&stmt.funcname),
            )?;
        }
    }
    let funcrettype = lsyscache::function::get_func_rettype(funcoid)?;
    if funcrettype != TRIGGEROID {
        return Err(err(
            format!(
                "function {} must return type {}",
                name_list_to_string(&stmt.funcname),
                "trigger"
            ),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }

    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;

    let mut trigoid = InvalidOid;
    let mut existing: Option<(types_tuple::ItemPointerData, Oid, bool, bool)> = None;
    if !is_internal {
        let cname = name_arg(mcx, trigname_given)?;
        let keys = [
            scan_key(2, F_OIDEQ, Datum::from_oid(rel.rd_id)),
            scan_key(4, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
        ];
        let mut scan = genam::systable_beginscan(
            mcx,
            &tgrel,
            TRIGGER_RELID_NAME_INDEX_ID,
            true,
            None,
            &keys,
        )?;
        if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let td = tgrel.descr();
            let mut isnull = false;
            // SAFETY (each): fixed NOT NULL pg_trigger columns under its
            // own descriptor.
            unsafe {
                trigoid =
                    types_tuple::heap_getattr(tup, Anum_pg_trigger_oid as i32, td, &mut isnull)
                        .as_oid();
                let excon =
                    types_tuple::heap_getattr(tup, Anum_pg_trigger_tgconstraint, td, &mut isnull)
                        .as_oid();
                let exint =
                    types_tuple::heap_getattr(tup, Anum_pg_trigger_tgisinternal, td, &mut isnull)
                        .as_bool();
                let exparent =
                    types_tuple::heap_getattr(tup, Anum_pg_trigger_tgparentid, td, &mut isnull)
                        .as_oid();
                existing = Some((tup.t_self, excon, exint, exparent != InvalidOid));
            }
        }
        genam::systable_endscan(mcx, scan)?;
    }

    match existing {
        None => {
            trigoid = catalog::GetNewOidWithIndex(
                mcx,
                &tgrel,
                TRIGGER_OID_INDEX_ID,
                Anum_pg_trigger_oid,
            )?;
        }
        Some((_, existing_con, existing_int, existing_clone)) => {
            if !stmt.replace {
                return Err(err(
                    format!(
                        "trigger \"{trigname_given}\" for relation \"{relname}\" already exists"
                    ),
                    ERRCODE_DUPLICATE_OBJECT,
                ));
            }
            if (existing_int || existing_clone) && !is_internal && !in_partition {
                return Err(err(
                    format!(
                        "trigger \"{trigname_given}\" for relation \"{relname}\" is an internal \
                         or a child trigger"
                    ),
                    ERRCODE_DUPLICATE_OBJECT,
                ));
            }
            debug_assert!(!stmt.isconstraint);
            if existing_con != InvalidOid {
                return Err(err(
                    format!(
                        "trigger \"{trigname_given}\" for relation \"{relname}\" is a constraint \
                         trigger"
                    ),
                    ERRCODE_DUPLICATE_OBJECT,
                ));
            }
        }
    }

    if stmt.isconstraint && constraint_oid == InvalidOid {
        debug_assert!(!is_internal);
        let mut entry = pg_constraint::ConstraintEntry::base(
            trigname_given,
            rel.rd_rel.relnamespace,
            CONSTRAINT_TRIGGER,
            rel.rd_id,
        );
        entry.deferrable = stmt.deferrable;
        entry.deferred = stmt.initdeferred;
        entry.is_no_inherit = true;
        constraint_oid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    }

    let mut trigname_storage;
    let trigname: &str = if is_internal {
        trigname_storage = mcx::PgString::from_str_in(trigname_given, mcx)?;
        use core::fmt::Write;
        write!(trigname_storage, "_{trigoid}").expect("tgname suffix");
        trigname_storage.as_str()
    } else {
        trigname_given
    };

    let mut values = [Datum::null(); Natts_pg_trigger];
    let mut nulls = [false; Natts_pg_trigger];
    let cname = name_arg(mcx, trigname)?;
    let (columns, tgattr) = build_columns(mcx, &rel, &stmt.columns)?;
    let (tgnargs, tgargs) = build_args(mcx, &stmt.args)?;
    values[0] = Datum::from_oid(trigoid);
    values[1] = Datum::from_oid(rel.rd_id);
    values[2] = Datum::from_oid(parent_trigger_oid);
    values[3] = Datum::from_usize(cname.as_ptr() as usize);
    values[4] = Datum::from_oid(funcoid);
    values[5] = Datum::from_i16(tgtype);
    values[6] = Datum::from_i8(fires_when);
    values[7] = Datum::from_bool(is_internal);
    values[8] = Datum::from_oid(constrrelid);
    values[9] = Datum::from_oid(index_oid);
    values[10] = Datum::from_oid(constraint_oid);
    values[11] = Datum::from_bool(stmt.deferrable);
    values[12] = Datum::from_bool(stmt.initdeferred);
    values[13] = Datum::from_i16(tgnargs);
    values[14] = Datum::from_usize(tgattr.as_ptr() as usize);
    values[15] = Datum::from_usize(tgargs.as_ptr() as usize);
    let qual_storage;
    match &qual {
        Some(q) => {
            qual_storage = varlena_image(mcx, q.as_str().as_bytes())?;
            values[16] = Datum::from_usize(qual_storage.as_ptr() as usize);
        }
        None => nulls[16] = true,
    }
    let old_storage;
    match oldtablename {
        Some(n) => {
            old_storage = name_arg(mcx, n)?;
            values[17] = Datum::from_usize(old_storage.as_ptr() as usize);
        }
        None => nulls[17] = true,
    }
    let new_storage;
    match newtablename {
        Some(n) => {
            new_storage = name_arg(mcx, n)?;
            values[18] = Datum::from_usize(new_storage.as_ptr() as usize);
        }
        None => nulls[18] = true,
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, tgrel.descr(), &values, &nulls)?;
    match existing {
        None => catalog_indexing::CatalogTupleInsert(mcx, &tgrel, &mut tuple)?,
        Some((otid, ..)) => catalog_indexing::CatalogTupleUpdate(mcx, &tgrel, &otid, &mut tuple)?,
    }
    tgrel.close(RowExclusiveLock)?;

    set_relation_has_triggers(mcx, rel.rd_id)?;

    if existing.is_some() {
        pg_depend::deleteDependencyRecordsFor(mcx, TRIGGER_RELATION_ID, trigoid, true)?;
    }

    let myself = ObjectAddress::set(TRIGGER_RELATION_ID, trigoid);
    pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(PROCEDURE_RELATION_ID, funcoid),
        DependencyType::Normal,
    )?;
    if is_internal && constraint_oid != InvalidOid {
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, constraint_oid),
            DependencyType::Internal,
        )?;
    } else {
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(RELATION_RELATION_ID, rel.rd_id),
            DependencyType::Auto,
        )?;
        if constrrelid != InvalidOid {
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(RELATION_RELATION_ID, constrrelid),
                DependencyType::Auto,
            )?;
        }
        debug_assert!(index_oid == InvalidOid);
        if constraint_oid != InvalidOid {
            pg_depend::recordDependencyOn(
                mcx,
                &ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, constraint_oid),
                &myself,
                DependencyType::Internal,
            )?;
        }
        if parent_trigger_oid != InvalidOid {
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(TRIGGER_RELATION_ID, parent_trigger_oid),
                DependencyType::PartitionPri,
            )?;
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(RELATION_RELATION_ID, rel.rd_id),
                DependencyType::PartitionSec,
            )?;
        }
    }

    for &attnum in columns.iter() {
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::sub_set(RELATION_RELATION_ID, rel.rd_id, attnum as i32),
            DependencyType::Normal,
        )?;
    }

    if !when_rtable.is_nil() {
        dependency_seams::record_dependency_on_expr::call(
            mcx,
            &myself,
            when_node.expect("WHEN clause parsed here"),
            &when_rtable,
            DependencyType::Normal,
        )?;
    }

    if partition_recurse {
        let partdesc = partdesc::RelationGetPartitionDesc(&rel, true)?;
        debug_assert!(index_oid == InvalidOid);
        for i in 0..partdesc.nparts {
            let child_oid = partdesc.oids[i];
            let child = table::table_open(mcx, child_oid, ShareRowExclusiveLock)?;
            let child_qual = match when_node {
                Some(q) => Some(map_partition_qual(mcx, q, &child, &rel)?),
                None => None,
            };
            let child_stmt = CreateTrigStmt {
                replace: stmt.replace,
                isconstraint: stmt.isconstraint,
                trigname: stmt.trigname,
                relation: None,
                funcname: NodeList::nil(),
                args: stmt.args.clone_in(mcx)?,
                row: stmt.row,
                timing: stmt.timing,
                events: stmt.events,
                columns: stmt.columns.clone_in(mcx)?,
                whenClause: None,
                transitionRels: NodeList::nil(),
                deferrable: stmt.deferrable,
                initdeferred: stmt.initdeferred,
                constrrel: stmt.constrrel,
            };
            CreateTriggerFiringOn(
                mcx,
                &child_stmt,
                query_string,
                child_oid,
                ref_rel_oid,
                InvalidOid,
                InvalidOid,
                funcoid,
                trigoid,
                child_qual,
                is_internal,
                true,
                fires_when,
            )?;
            child.close(NoLock)?;
        }
    }

    rel.close(NoLock)?;
    Ok(trigoid)
}

// map_partition_varattnos (catalog/partition.c) over a trigger WHEN qual:
// both OLD (varno 1) and NEW (varno 2) references translate.
pub fn map_partition_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual: Node<'mcx>,
    child: &Relation<'mcx>,
    parent: &Relation<'mcx>,
) -> PgResult<Node<'mcx>> {
    let attmap = tupdesc::build_attrmap_by_name(mcx, child.descr(), parent.descr())?;
    let to_rowtype = child.rd_rel.reltype;
    let (q, _) = rewrite_manip::map_variable_attnos(mcx, qual, 1, 0, &attmap, to_rowtype)?;
    let (q, _) = rewrite_manip::map_variable_attnos(mcx, q, 2, 0, &attmap, to_rowtype)?;
    Ok(q)
}

fn to_rangevar<'a>(rv: &'a types_nodes::primnodes::RangeVar<'a>) -> rel_vocab::RangeVar<'a> {
    rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("RangeVar.relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    }
}

fn validate_transition_rels<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    rel: &Relation<'mcx>,
    tgtype: i16,
) -> PgResult<(Option<&'mcx str>, Option<&'mcx str>)> {
    let mut oldtablename: Option<&'mcx str> = None;
    let mut newtablename: Option<&'mcx str> = None;
    for n in stmt.transitionRels.iter() {
        let tt = n
            .as_variant::<types_nodes::rawnodes::TriggerTransition>()
            .expect("TriggerTransition");
        if !tt.isTable {
            return Err(Box::new(
                (*err(
                    "ROW variable naming in the REFERENCING clause is not supported".to_string(),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ))
                .with_hint("Use OLD TABLE or NEW TABLE for naming transition tables.".to_string()),
            ));
        }
        if rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
            return Err(Box::new(
                (*err(
                    format!("\"{}\" is a foreign table", rel.name()),
                    ERRCODE_WRONG_OBJECT_TYPE,
                ))
                .with_detail(
                    "Triggers on foreign tables cannot have transition tables.".to_string(),
                ),
            ));
        }
        if rel.rd_rel.relkind == b'v' {
            return Err(Box::new(
                (*err(format!("\"{}\" is a view", rel.name()), ERRCODE_WRONG_OBJECT_TYPE))
                    .with_detail("Triggers on views cannot have transition tables.".to_string()),
            ));
        }
        if tgtype & TRIGGER_TYPE_ROW != 0 && pg_inherits::has_superclass(mcx, rel.rd_id)? {
            let msg = if rel.rd_rel.relispartition {
                "ROW triggers with transition tables are not supported on partitions"
            } else {
                "ROW triggers with transition tables are not supported on inheritance children"
            };
            return Err(err(msg.to_string(), ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        if stmt.timing != TRIGGER_TYPE_AFTER {
            return Err(err(
                "transition table name can only be specified for an AFTER trigger".to_string(),
                ERRCODE_INVALID_OBJECT_DEFINITION,
            ));
        }
        if tgtype & TRIGGER_TYPE_TRUNCATE != 0 {
            return Err(err(
                "TRUNCATE triggers with transition tables are not supported".to_string(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        let nevents = (tgtype & TRIGGER_TYPE_INSERT != 0) as i32
            + (tgtype & TRIGGER_TYPE_UPDATE != 0) as i32
            + (tgtype & TRIGGER_TYPE_DELETE != 0) as i32;
        if nevents != 1 {
            return Err(err(
                "transition tables cannot be specified for triggers with more than one event"
                    .to_string(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        if !stmt.columns.is_nil() {
            return Err(err(
                "transition tables cannot be specified for triggers with column lists".to_string(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        debug_assert!(!stmt.isconstraint);
        let name = tt.name.expect("TriggerTransition.name");
        if tt.isNew {
            if tgtype & (TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE) == 0 {
                return Err(err(
                    "NEW TABLE can only be specified for an INSERT or UPDATE trigger".to_string(),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            if newtablename.is_some() {
                return Err(err(
                    "NEW TABLE cannot be specified multiple times".to_string(),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            newtablename = Some(name);
        } else {
            if tgtype & (TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE) == 0 {
                return Err(err(
                    "OLD TABLE can only be specified for a DELETE or UPDATE trigger".to_string(),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            if oldtablename.is_some() {
                return Err(err(
                    "OLD TABLE cannot be specified multiple times".to_string(),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            oldtablename = Some(name);
        }
    }
    if let (Some(new), Some(old)) = (newtablename, oldtablename) {
        if new == old {
            return Err(err(
                "OLD TABLE name and NEW TABLE name cannot be the same".to_string(),
                ERRCODE_INVALID_OBJECT_DEFINITION,
            ));
        }
    }
    Ok((oldtablename, newtablename))
}

fn transform_when_clause<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    when: Node<'mcx>,
    tgtype: i16,
    query_string: Option<&str>,
) -> PgResult<(Node<'mcx>, mcx::PgString<'mcx>, NodeList<'mcx>)> {
    let mut pstate = parser_small1::make_parsestate(mcx, None);
    if let Some(s) = query_string {
        let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
        mcx::vec_append_bytes(&mut buf, s.as_bytes())?;
        pstate.p_sourcetext = Some(buf.leak());
    }

    let old_alias = mcx::leak_in(mcx::alloc_in(
        mcx,
        Alias { aliasname: Some("old"), colnames: NodeList::nil() },
    )?);
    let nsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        AccessShareLock,
        Some(&*old_alias),
        false,
        false,
    )?;
    parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;
    let new_alias = mcx::leak_in(mcx::alloc_in(
        mcx,
        Alias { aliasname: Some("new"), colnames: NodeList::nil() },
    )?);
    let nsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        AccessShareLock,
        Some(&*new_alias),
        false,
        false,
    )?;
    parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

    let when_clause = parse_clause::transformWhereClause(
        mcx,
        &mut pstate,
        Some(when),
        parser_small1::ParseExprKind::EXPR_KIND_TRIGGER_WHEN,
        "WHEN",
    )?
    .expect("WHEN clause present");
    parse_collate::assign_expr_collations(mcx, &pstate, when_clause)?;

    let vars = vars::pull_var_clause(mcx, when_clause, 0)?;
    for v in vars.iter() {
        let var = v.as_variant::<Var>().expect("pull_var_clause Var");
        check_when_var(&pstate, rel, var, tgtype)?;
    }

    let qual = outfuncs::nodeToString(mcx, when_clause)?;
    // C keeps pstate->p_rtable for recordDependencyOnExpr.
    Ok((when_clause, qual, pstate.p_rtable))
}

#[track_caller]
#[cold]
#[inline(never)]
fn when_err(
    pstate: &parser_small1::ParseState<'_, '_>,
    location: i32,
    msg: &str,
    sqlstate: types_error::SqlState,
    detail: Option<String>,
) -> Box<PgError> {
    let mut e = PgError::new(ERROR, msg.to_string()).with_sqlstate(sqlstate);
    if let Some(d) = detail {
        e = e.with_detail(d);
    }
    let pos =
        parser_small1::parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
    if pos > 0 {
        e = e.with_cursor_position(pos);
    }
    Box::new(e)
}

fn check_when_var(
    pstate: &parser_small1::ParseState<'_, '_>,
    rel: &Relation<'_>,
    var: &Var,
    tgtype: i16,
) -> PgResult<()> {
    match var.varno as i32 {
        PRS2_OLD_VARNO => {
            if tgtype & TRIGGER_TYPE_ROW == 0 {
                return Err(when_err(
                    pstate,
                    var.location,
                    "statement trigger's WHEN condition cannot reference column values",
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                    None,
                ));
            }
            if tgtype & TRIGGER_TYPE_INSERT != 0 {
                return Err(when_err(
                    pstate,
                    var.location,
                    "INSERT trigger's WHEN condition cannot reference OLD values",
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                    None,
                ));
            }
        }
        PRS2_NEW_VARNO => {
            if tgtype & TRIGGER_TYPE_ROW == 0 {
                return Err(when_err(
                    pstate,
                    var.location,
                    "statement trigger's WHEN condition cannot reference column values",
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                    None,
                ));
            }
            if tgtype & TRIGGER_TYPE_DELETE != 0 {
                return Err(when_err(
                    pstate,
                    var.location,
                    "DELETE trigger's WHEN condition cannot reference NEW values",
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                    None,
                ));
            }
            if var.varattno < 0 && tgtype & TRIGGER_TYPE_BEFORE != 0 {
                return Err(when_err(
                    pstate,
                    var.location,
                    "BEFORE trigger's WHEN condition cannot reference NEW system columns",
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    None,
                ));
            }
            if tgtype & TRIGGER_TYPE_BEFORE != 0 && var.varattno == 0 {
                let has_generated = (0..rel.rd_att.natts as usize)
                    .any(|i| rel.rd_att.attr(i).attgenerated != 0);
                if has_generated {
                    return Err(when_err(
                        pstate,
                        var.location,
                        "BEFORE trigger's WHEN condition cannot reference NEW generated columns",
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        Some(
                            "A whole-row reference is used and the table contains generated \
                             columns."
                                .to_string(),
                        ),
                    ));
                }
            }
            if tgtype & TRIGGER_TYPE_BEFORE != 0 && var.varattno > 0 {
                let att = rel.rd_att.attr(var.varattno as usize - 1);
                if att.attgenerated != 0 {
                    let colname =
                        core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
                    return Err(when_err(
                        pstate,
                        var.location,
                        "BEFORE trigger's WHEN condition cannot reference NEW generated columns",
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        Some(format!("Column \"{colname}\" is a generated column.")),
                    ));
                }
            }
        }
        _ => panic!("trigger WHEN condition cannot contain references to other relations"),
    }
    Ok(())
}

// LookupFuncName(funcname, 0, NULL, false) (parse_func.c).
fn lookup_trigger_func<'mcx>(mcx: Mcx<'mcx>, funcname: &NodeList<'mcx>) -> PgResult<Oid> {
    let mut buf = [""; 4];
    let mut n = 0;
    for part in funcname.iter() {
        buf[n] = part.as_string().expect("funcname String").sval;
        n += 1;
    }
    let clist = catalog_namespace::FuncnameGetCandidates(mcx, &buf[..n], 0, &[], false, false)?;
    match clist.len() {
        0 => Err(err(
            format!("function {}() does not exist", name_list_to_string(funcname)),
            ERRCODE_UNDEFINED_FUNCTION,
        )),
        1 => Ok(clist[0].oid),
        _ => panic!("multiple zero-argument candidates for {}", name_list_to_string(funcname)),
    }
}

fn name_list_to_string(names: &NodeList<'_>) -> String {
    let mut out = String::new();
    for (i, part) in names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(part.as_string().expect("name String").sval);
    }
    out
}

fn build_columns<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    columns: &NodeList<'_>,
) -> PgResult<(PgVec<'mcx, i16>, PgVec<'mcx, u32>)> {
    let mut attnums: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    for c in columns.iter() {
        let name = c.as_string().expect("column String").sval;
        let mut attnum: i16 = 0;
        for i in 0..rel.rd_att.natts as usize {
            let att = rel.rd_att.attr(i);
            if !att.attisdropped && att.attname.name_str() == name.as_bytes() {
                attnum = att.attnum;
                break;
            }
        }
        if attnum == 0 {
            return Err(err(
                format!("column \"{name}\" of relation \"{}\" does not exist", rel.name()),
                ERRCODE_UNDEFINED_COLUMN,
            ));
        }
        if attnums.iter().any(|&a| a == attnum) {
            return Err(err(
                format!("column \"{name}\" specified more than once"),
                ERRCODE_DUPLICATE_COLUMN,
            ));
        }
        attnums.push(attnum);
    }
    let vec = int2vector(mcx, &attnums)?;
    Ok((attnums, vec))
}

// CreateTriggerFiringOn's tgargs packing: each arg's bytes NUL-terminated,
// concatenated into one bytea (C's backslash-escape round trip through
// byteain nets out to raw bytes plus separators).
fn build_args<'mcx>(mcx: Mcx<'mcx>, args: &NodeList<'_>) -> PgResult<(i16, PgVec<'mcx, u32>)> {
    let mut body: Vec<u8> = Vec::new();
    let mut nargs: i16 = 0;
    for a in args.iter() {
        let s = a.as_string().expect("trigger arg String").sval;
        body.extend_from_slice(s.as_bytes());
        body.push(0);
        nargs += 1;
    }
    let image = varlena_image(mcx, &body)?;
    Ok((nargs, image))
}

// buildint2vector: 24-byte 1-D array header + n int2s, 4-aligned storage.
fn int2vector<'mcx>(mcx: Mcx<'mcx>, items: &[i16]) -> PgResult<PgVec<'mcx, u32>> {
    let n = items.len();
    let size = 24 + 2 * n;
    let words = 6 + n.div_ceil(2);
    let mut buf: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, words)?;
    buf.resize(words, 0);
    buf[0] = types_tuple::varatt::set_varsize_4b_word(size as u32);
    buf[1] = 1;
    buf[2] = 0;
    buf[3] = types_core::INT2OID;
    buf[4] = n as u32;
    buf[5] = 0;
    // SAFETY: words 6.. reserve ceil(n/2) u32s >= n i16 slots.
    unsafe {
        core::ptr::copy_nonoverlapping(items.as_ptr(), buf.as_mut_ptr().add(6) as *mut i16, n);
    }
    Ok(buf)
}

// A 4-byte-header varlena image (bytea/text), 4-aligned storage.
fn varlena_image<'mcx>(mcx: Mcx<'mcx>, body: &[u8]) -> PgResult<PgVec<'mcx, u32>> {
    let size = 4 + body.len();
    let words = size.div_ceil(4);
    let mut buf: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, words)?;
    buf.resize(words, 0);
    buf[0] = types_tuple::varatt::set_varsize_4b_word(size as u32);
    // SAFETY: words 1.. hold size-4 = body.len() in-bounds bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(
            body.as_ptr(),
            (buf.as_mut_ptr() as *mut u8).add(4),
            body.len(),
        );
    }
    Ok(buf)
}

// TriggerSetParentTrigger (trigger.c): set or clear tgparentid on a child
// trigger, with the matching partition dependency records.
pub fn TriggerSetParentTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    child_trig_id: Oid,
    parent_trig_id: Oid,
    child_table_id: Oid,
) -> PgResult<()> {
    const TriggerOidIndexId: Oid = 2702;
    let trig_rel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let keys = [scan_key(Anum_pg_trigger_oid, F_OIDEQ, Datum::from_oid(child_trig_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &trig_rel, TriggerOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for trigger {child_trig_id}"));
    let desc = trig_rel.descr();
    let mut isnull = false;
    // SAFETY: tgparentid is a fixed NOT NULL pg_trigger column.
    let tgparentid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_trigger_tgparentid, desc, &mut isnull)
    }
    .as_oid();
    let natts = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    nulls.resize(natts, false);
    replace.resize(natts, false);
    if parent_trig_id != InvalidOid && tgparentid != InvalidOid {
        panic!("trigger {child_trig_id} already has a parent trigger");
    }
    values[Anum_pg_trigger_tgparentid as usize - 1] = Datum::from_oid(parent_trig_id);
    replace[Anum_pg_trigger_tgparentid as usize - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &trig_rel, &otid, &mut newtup)?;

    let depender = ObjectAddress::set(TRIGGER_RELATION_ID, child_trig_id);
    if parent_trig_id != InvalidOid {
        let parent = ObjectAddress::set(TRIGGER_RELATION_ID, parent_trig_id);
        pg_depend::recordDependencyOn(mcx, &depender, &parent, DependencyType::PartitionPri)?;
        let tbl = ObjectAddress::set(RELATION_RELATION_ID, child_table_id);
        pg_depend::recordDependencyOn(mcx, &depender, &tbl, DependencyType::PartitionSec)?;
    } else {
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            TRIGGER_RELATION_ID,
            child_trig_id,
            TRIGGER_RELATION_ID,
            DependencyType::PartitionPri,
        )?;
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            TRIGGER_RELATION_ID,
            child_trig_id,
            RELATION_RELATION_ID,
            DependencyType::PartitionSec,
        )?;
    }
    trig_rel.close(RowExclusiveLock)
}

pub(crate) fn scan_key(
    attno: AttrNumber,
    func: types_core::RegProcedure,
    arg: Datum,
) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

// The pg_class.relhastriggers update half of CreateTriggerFiringOn; the
// already-true arm's CacheInvalidateRelcacheByTuple is covered by
// CatalogTupleUpdate's inval on the first set.
fn set_relation_has_triggers<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let relrel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = scan_key(1, F_OIDEQ, Datum::from_oid(relid));
    let mut scan = genam::systable_beginscan(
        mcx,
        &relrel,
        CLASS_OID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let td = relrel.descr();
    let mut isnull = false;
    // SAFETY: pg_class row under its own descriptor; relhastriggers declared.
    let has = unsafe {
        types_tuple::heap_getattr(reltup, Anum_pg_class_relhastriggers as i32, td, &mut isnull)
    }
    .as_bool();
    if !has {
        let natts = td.natts as usize;
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        repl_values[Anum_pg_class_relhastriggers - 1] = Datum::from_bool(true);
        repl[Anum_pg_class_relhastriggers - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, reltup, td, &repl_values, &repl_isnull, &repl)?;
        let otid = reltup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &relrel, &otid, &mut newtup)?;
        xact::CommandCounterIncrement()?;
    } else {
        genam::systable_endscan(mcx, scan)?;
        inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
    }
    relrel.close(RowExclusiveLock)
}

pub(crate) fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "trigger name overflows NAMEDATALEN: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}
