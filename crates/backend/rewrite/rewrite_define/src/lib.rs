//! rewriteDefine.c (DefineRule/DefineQueryRewrite/EnableDisableRule),
//! rewriteRemove.c, rewriteSupport.c.

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use relcache::schemapg::REWRITE_RELATION_ID;
use types_core::catalog::RELATION_RELATION_ID;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, NAMEDATALEN};
use types_core::{AttrNumber, Oid, RegProcedure};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::Node;
use types_rel::{
    AccessExclusiveLock, Relation, RowExclusiveLock, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

// get_relkind_objtype (tablecmds.c), rule-relevant subset only.
fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    match relkind {
        RELKIND_VIEW => types_nodes::parsenodes::ObjectType::OBJECT_VIEW,
        _ => types_nodes::parsenodes::ObjectType::OBJECT_TABLE,
    }
}

pub const ViewSelectRuleName: &str = "_RETURN";

pub fn init_seams() {
    rewrite_define_seams::remove_rewrite_rule_by_id::set(RemoveRewriteRuleById);
    rewrite_define_seams::get_rewrite_oid::set(get_rewrite_oid);
}

const RULE_FIRES_ON_ORIGIN: u8 = b'O';
const REWRITE_OID_INDEX_ID: Oid = 2692;
const REWRITE_REL_RULENAME_INDEX_ID: Oid = 2693;

const Anum_pg_rewrite_oid: AttrNumber = 1;
const Anum_pg_rewrite_rulename: AttrNumber = 2;
const Anum_pg_rewrite_ev_class: AttrNumber = 3;
const Anum_pg_rewrite_ev_type: AttrNumber = 4;
const Anum_pg_rewrite_is_instead: AttrNumber = 6;
const Anum_pg_rewrite_ev_qual: AttrNumber = 7;
const Anum_pg_rewrite_ev_action: AttrNumber = 8;
const Anum_pg_class_relhasrules: usize = 21;
const Anum_pg_class_oid: AttrNumber = 1;
const CLASS_OID_INDEX_ID: Oid = 2662;

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_image<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "namestrcpy truncation unported: {name:?}");
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

// InsertRule (rewriteDefine.c).
fn InsertRule<'mcx>(
    mcx: Mcx<'mcx>,
    rulname: &str,
    evtype: CmdType,
    eventrel_oid: Oid,
    evinstead: bool,
    event_qual: Option<Node<'mcx>>,
    action: &NodeList<'mcx>,
    replace: bool,
) -> PgResult<Oid> {
    let evqual = match event_qual {
        Some(q) => outfuncs::nodeToString(mcx, q)?,
        None => mcx::PgString::from_str_in("<>", mcx)?,
    };
    let action_node = Node::mk_list(mcx, action.clone_in(mcx)?)?;
    let actiontree = outfuncs::nodeToString(mcx, action_node)?;

    let rel = table::table_open(mcx, REWRITE_RELATION_ID, RowExclusiveLock)?;

    let rname = name_image(mcx, rulname)?;
    let keys = [
        eq_key(Anum_pg_rewrite_ev_class, F_OIDEQ, Datum::from_oid(eventrel_oid)),
        eq_key(
            Anum_pg_rewrite_rulename,
            F_NAMEEQ,
            Datum::from_usize(rname.as_ptr() as usize),
        ),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, REWRITE_REL_RULENAME_INDEX_ID, true, None, &keys)?;
    let oldtup = genam::systable_getnext(mcx, &mut scan)?;

    let evqual_text = varlena::cstring_to_text(mcx, evqual.as_bytes())?;
    let action_text = varlena::cstring_to_text(mcx, actiontree.as_bytes())?;
    let mut values = [
        Datum::null(),
        Datum::from_usize(rname.as_ptr() as usize),
        Datum::from_oid(eventrel_oid),
        Datum::from_i8((evtype as u8 + b'0') as i8),
        Datum::from_i8(RULE_FIRES_ON_ORIGIN as i8),
        Datum::from_bool(evinstead),
        Datum::from_usize(evqual_text.as_bytes().as_ptr() as usize),
        Datum::from_usize(action_text.as_bytes().as_ptr() as usize),
    ];
    let nulls = [false; 8];

    let mut is_update = false;
    let rule_oid;
    if let Some(oldtup) = oldtup {
        if !replace {
            let relname = lsyscache::get_rel_name(mcx, eventrel_oid)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!(
                    "rule \"{rulname}\" for relation \"{relname}\" already exists"
                ))
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
            ));
        }
        let mut replaces = [false; 8];
        replaces[Anum_pg_rewrite_ev_type as usize - 1] = true;
        replaces[Anum_pg_rewrite_is_instead as usize - 1] = true;
        replaces[Anum_pg_rewrite_ev_qual as usize - 1] = true;
        replaces[Anum_pg_rewrite_ev_action as usize - 1] = true;
        let mut tuple =
            heaptuple::heap_modify_tuple(mcx, oldtup, rel.descr(), &values, &nulls, &replaces)?;
        let mut isnull = false;
        // SAFETY: fixed NOT NULL oid column under pg_rewrite's descriptor.
        rule_oid = unsafe {
            types_tuple::heap_getattr(oldtup, Anum_pg_rewrite_oid as i32, rel.descr(), &mut isnull)
        }
        .as_oid();
        let otid = oldtup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut tuple)?;
        is_update = true;
    } else {
        genam::systable_endscan(mcx, scan)?;
        rule_oid =
            catalog::GetNewOidWithIndex(mcx, &rel, REWRITE_OID_INDEX_ID, Anum_pg_rewrite_oid)?;
        values[0] = Datum::from_oid(rule_oid);
        let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;
    }

    if is_update {
        pg_depend::deleteDependencyRecordsFor(mcx, REWRITE_RELATION_ID, rule_oid, false)?;
    }

    let myself = ObjectAddress::set(REWRITE_RELATION_ID, rule_oid);
    let referenced = ObjectAddress::set(RELATION_RELATION_ID, eventrel_oid);
    let behavior = if evtype == CmdType::CMD_SELECT {
        DependencyType::Internal
    } else {
        DependencyType::Auto
    };
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, behavior)?;

    catalog_dependency::recordDependencyOnExpr(
        mcx,
        &myself,
        action_node,
        &NodeList::nil(),
        DependencyType::Normal,
    )?;

    if let Some(qual) = event_qual {
        let qry = action.nth(0).as_query().expect("rule action is a Query");
        let qry = rewrite_manip::getInsertSelectQuery_ref(qry)?;
        catalog_dependency::recordDependencyOnExpr(
            mcx,
            &myself,
            qual,
            &qry.rtable,
            DependencyType::Normal,
        )?;
    }

    rel.close(RowExclusiveLock)?;
    Ok(rule_oid)
}

// DefineQueryRewrite (rewriteDefine.c), ON SELECT lane.
pub fn DefineQueryRewrite<'mcx>(
    mcx: Mcx<'mcx>,
    rulename: &str,
    event_relid: Oid,
    event_qual: Option<Node<'mcx>>,
    event_type: CmdType,
    is_instead: bool,
    replace: bool,
    action: NodeList<'mcx>,
) -> PgResult<ObjectAddress> {
    let event_relation = table::table_open(mcx, event_relid, AccessExclusiveLock)?;

    let relkind = event_relation.rd_rel.relkind;
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_MATVIEW
        && relkind != RELKIND_VIEW
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        return Err(Box::new(
            PgError::error(format!(
                "relation \"{}\" cannot have rules",
                event_relation.name()
            ))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(relkind_not_supported_detail(relkind)),
        ));
    }
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&event_relation) {
        return Err(Box::new(
            PgError::error(format!(
                "permission denied: \"{}\" is a system catalog",
                event_relation.name()
            ))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    for item in action.iter() {
        let query = item.as_query().expect("rule action is a Query");
        if query.resultRelation == 0 {
            continue;
        }
        // Don't be fooled by INSERT/SELECT.
        if !core::ptr::eq(query, rewrite_manip::getInsertSelectQuery_ref(query)?) {
            continue;
        }
        if query.resultRelation == rewrite_manip::PRS2_OLD_VARNO {
            return Err(Box::new(
                PgError::error("rule actions on OLD are not implemented")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint("Use views or triggers instead."),
            ));
        }
        if query.resultRelation == rewrite_manip::PRS2_NEW_VARNO {
            return Err(Box::new(
                PgError::error("rule actions on NEW are not implemented")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint("Use triggers instead."),
            ));
        }
    }

    if event_type != CmdType::CMD_SELECT {
        return define_non_select_rewrite(
            mcx,
            rulename,
            event_relid,
            event_qual,
            event_type,
            is_instead,
            replace,
            action,
            event_relation,
        );
    }

    if relkind != RELKIND_VIEW && relkind != RELKIND_MATVIEW {
        return Err(Box::new(
            PgError::error(format!(
                "relation \"{}\" cannot have ON SELECT rules",
                event_relation.name()
            ))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(relkind_not_supported_detail(relkind)),
        ));
    }
    if action.is_nil() {
        return Err(Box::new(
            PgError::error("INSTEAD NOTHING rules on SELECT are not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint("Use views instead."),
        ));
    }
    if action.len() > 1 {
        return Err(feature_not_supported(
            "multiple actions for rules on SELECT are not implemented",
        ));
    }
    let query = action.nth(0).as_query().expect("rule action is a Query");
    if !is_instead || query.commandType != CmdType::CMD_SELECT {
        return Err(feature_not_supported("rules on SELECT must have action INSTEAD SELECT"));
    }
    if query.hasModifyingCTE {
        return Err(feature_not_supported(
            "rules on SELECT must not contain data-modifying statements in WITH",
        ));
    }
    if event_qual.is_some() {
        return Err(feature_not_supported(
            "event qualifications are not implemented for rules on SELECT",
        ));
    }
    checkRuleResultList(&query.targetList, &event_relation, true, relkind != RELKIND_MATVIEW)?;
    if !replace {
        if let Some(rules) = relcache::rules::RelationGetRules(mcx, event_relid)? {
            if rules.rules.iter().any(|r| r.event == CmdType::CMD_SELECT as i32) {
                return Err(Box::new(
                    PgError::error(format!("\"{}\" is already a view", event_relation.name()))
                        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                ));
            }
        }
    }
    if rulename != ViewSelectRuleName {
        return Err(Box::new(
            PgError::error(format!(
                "view rule for \"{}\" must be named \"{}\"",
                event_relation.name(),
                ViewSelectRuleName
            ))
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }

    let rule_id = InsertRule(
        mcx,
        rulename,
        event_type,
        event_relid,
        is_instead,
        event_qual,
        &action,
        replace,
    )?;
    SetRelationRuleStatus(mcx, event_relid, true)?;

    // Close rel, but keep lock till commit (table_close(rel, NoLock)).
    event_relation.close(types_rel::NoLock)?;
    Ok(ObjectAddress::set(REWRITE_RELATION_ID, rule_id))
}

// DefineQueryRewrite's non-SELECT arm.
#[allow(clippy::too_many_arguments)]
fn define_non_select_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    rulename: &str,
    event_relid: Oid,
    event_qual: Option<Node<'mcx>>,
    event_type: CmdType,
    is_instead: bool,
    replace: bool,
    action: NodeList<'mcx>,
    event_relation: Relation<'mcx>,
) -> PgResult<ObjectAddress> {
    let mut have_returning = false;
    for item in action.iter() {
        let query = item.as_query().expect("rule action is a Query");
        if query.returningList.is_nil() {
            continue;
        }
        if have_returning {
            return Err(feature_not_supported(
                "cannot have multiple RETURNING lists in a rule",
            ));
        }
        have_returning = true;
        if event_qual.is_some() {
            return Err(feature_not_supported(
                "RETURNING lists are not supported in conditional rules",
            ));
        }
        if !is_instead {
            return Err(feature_not_supported(
                "RETURNING lists are not supported in non-INSTEAD rules",
            ));
        }
        checkRuleResultList(&query.returningList, &event_relation, false, false)?;
    }

    if rulename == ViewSelectRuleName {
        return Err(Box::new(
            PgError::error(format!(
                "non-view rule for \"{}\" must not be named \"{}\"",
                event_relation.name(),
                ViewSelectRuleName
            ))
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }

    // A nil-action non-INSTEAD rule is a no-op; discard it.
    let mut rule_id = types_core::InvalidOid;
    if !action.is_nil() || is_instead {
        rule_id = InsertRule(
            mcx,
            rulename,
            event_type,
            event_relid,
            is_instead,
            event_qual,
            &action,
            replace,
        )?;
        SetRelationRuleStatus(mcx, event_relid, true)?;
    }

    event_relation.close(types_rel::NoLock)?;
    Ok(ObjectAddress::set(REWRITE_RELATION_ID, rule_id))
}

// DefineRule (rewriteDefine.c): CREATE RULE.
pub fn DefineRule<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::rawnodes::RuleStmt<'mcx>,
    query_string: &str,
) -> PgResult<ObjectAddress> {
    let (actions, where_clause) = parser_analyze::transformRuleStmt(mcx, stmt, query_string)?;
    let rvn = stmt.relation.expect("CREATE RULE has a relation");
    let rv = rel_vocab::RangeVar {
        catalogname: rvn.catalogname,
        schemaname: rvn.schemaname,
        relname: rvn.relname.expect("grammar always sets relname"),
        inh: rvn.inh,
        relpersistence: rvn.relpersistence,
        location: rvn.location,
    };
    let rel_id = catalog_namespace::RangeVarGetRelid(&rv, AccessExclusiveLock, false)?;
    DefineQueryRewrite(
        mcx,
        stmt.rulename,
        rel_id,
        where_clause,
        stmt.event,
        stmt.instead,
        stmt.replace,
        actions,
    )
}

// checkRuleResultList (rewriteDefine.c).
fn checkRuleResultList<'mcx>(
    targetList: &NodeList<'mcx>,
    event_relation: &Relation<'mcx>,
    isSelect: bool,
    requireColumnNameMatch: bool,
) -> PgResult<()> {
    debug_assert!(isSelect || !requireColumnNameMatch);
    let desc = event_relation.descr();
    let mut i: i32 = 0;
    for item in targetList.iter() {
        let tle = item.as_target_entry().expect("targetList entry");
        if tle.resjunk {
            continue;
        }
        i += 1;
        if i > desc.natts {
            return Err(invalid_object(if isSelect {
                "SELECT rule's target list has too many entries"
            } else {
                "RETURNING list has too many entries"
            }));
        }
        let attr = desc.attr(i as usize - 1);
        let attname = core::str::from_utf8(attr.attname.name_str()).expect("attname utf8");
        if attr.attisdropped {
            return Err(feature_not_supported(if isSelect {
                "cannot convert relation containing dropped columns to view"
            } else {
                "cannot create a RETURNING list for a relation containing dropped columns"
            }));
        }
        if requireColumnNameMatch && tle.resname != Some(attname) {
            return Err(Box::new(
                PgError::error(format!(
                    "SELECT rule's target entry {i} has different column name from column \
                     \"{attname}\""
                ))
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                .with_detail(format!(
                    "SELECT target entry is named \"{}\".",
                    tle.resname.unwrap_or("")
                )),
            ));
        }
        let tletypid = parse_expr::expr_type(tle.expr);
        if attr.atttypid != tletypid {
            let msg = if isSelect {
                format!(
                    "SELECT rule's target entry {i} has different type from column \"{attname}\""
                )
            } else {
                format!(
                    "RETURNING list's entry {i} has different type from column \"{attname}\""
                )
            };
            let entry = if isSelect { "SELECT target entry" } else { "RETURNING list entry" };
            return Err(Box::new(
                PgError::error(msg)
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .with_detail(format!(
                        "{entry} has type {}, but column has type {}.",
                        format_type::format_type_be(tletypid).unwrap_or_else(|_| "???".into()),
                        format_type::format_type_be(attr.atttypid)
                            .unwrap_or_else(|_| "???".into()),
                    )),
            ));
        }
        let tletypmod = parse_expr::expr_typmod(tle.expr);
        if attr.atttypmod != tletypmod && attr.atttypmod != -1 && tletypmod != -1 {
            let msg = if isSelect {
                format!(
                    "SELECT rule's target entry {i} has different size from column \"{attname}\""
                )
            } else {
                format!(
                    "RETURNING list's entry {i} has different size from column \"{attname}\""
                )
            };
            let entry = if isSelect { "SELECT target entry" } else { "RETURNING list entry" };
            return Err(Box::new(
                PgError::error(msg)
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .with_detail(format!(
                        "{entry} has type {}, but column has type {}.",
                        format_type::format_type_with_typemod(tletypid, tletypmod)
                            .unwrap_or_else(|_| "???".into()),
                        format_type::format_type_with_typemod(attr.atttypid, attr.atttypmod)
                            .unwrap_or_else(|_| "???".into()),
                    )),
            ));
        }
    }
    if i != desc.natts {
        return Err(invalid_object(if isSelect {
            "SELECT rule's target list has too few entries"
        } else {
            "RETURNING list has too few entries"
        }));
    }
    Ok(())
}

// SetRelationRuleStatus (rewriteSupport.c). The catalog update queues the
// relcache inval; the no-change branch stays loud.
pub fn SetRelationRuleStatus<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    relHasRules: bool,
) -> PgResult<()> {
    let rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_class_oid, F_OIDEQ, Datum::from_oid(relationId))];
    let mut scan = genam::systable_beginscan(mcx, &rel, CLASS_OID_INDEX_ID, true, None, &keys)?;
    let tup = match genam::systable_getnext(mcx, &mut scan)? {
        Some(t) => t,
        None => {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for relation {relationId}"
            ))))
        }
    };
    let mut isnull = false;
    // SAFETY: pg_class row read under pg_class's descriptor; relhasrules is
    // a declared column.
    let cur = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_class_relhasrules as i32, rel.descr(), &mut isnull)
    };
    if !isnull && cur.as_bool() == relHasRules {
        // No change: still broadcast the SI notice so every backend reloads
        // the relation's rules.
        inval::invalidate::CacheInvalidateRelcacheByTuple(tup)?;
        genam::systable_endscan(mcx, scan)?;
        rel.close(RowExclusiveLock)?;
        return Ok(());
    }
    let natts = rel.descr().natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_class_relhasrules - 1] = Datum::from_bool(relHasRules);
    repl[Anum_pg_class_relhasrules - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, rel.descr(), &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
    rel.close(RowExclusiveLock)?;
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_object(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION))
}

// errdetail_relkind_not_supported (pg_class.c).
fn relkind_not_supported_detail(relkind: u8) -> &'static str {
    match relkind {
        b'r' => "This operation is not supported for tables.",
        b'i' => "This operation is not supported for indexes.",
        b'S' => "This operation is not supported for sequences.",
        b't' => "This operation is not supported for TOAST tables.",
        b'v' => "This operation is not supported for views.",
        b'm' => "This operation is not supported for materialized views.",
        b'c' => "This operation is not supported for composite types.",
        b'f' => "This operation is not supported for foreign tables.",
        b'p' => "This operation is not supported for partitioned tables.",
        b'I' => "This operation is not supported for partitioned indexes.",
        other => panic!("unrecognized relkind: {other}"),
    }
}

const Anum_pg_rewrite_ev_enabled: AttrNumber = 5;

// EnableDisableRule (rewriteDefine.c); ownership check rides the single-user
// boot identity (DefineQueryRewrite precedent).
pub fn EnableDisableRule<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    rulename: &str,
    fires_when: u8,
) -> PgResult<()> {
    let owning_rel = rel.rd_id;
    let pg_rewrite = table::table_open(mcx, REWRITE_RELATION_ID, RowExclusiveLock)?;
    let rname = name_image(mcx, rulename)?;
    let keys = [
        eq_key(Anum_pg_rewrite_ev_class, F_OIDEQ, Datum::from_oid(owning_rel)),
        eq_key(
            Anum_pg_rewrite_rulename,
            F_NAMEEQ,
            Datum::from_usize(rname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_rewrite,
        REWRITE_REL_RULENAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let tup = match genam::systable_getnext(mcx, &mut scan)? {
        Some(t) => t,
        None => {
            let relname = lsyscache::get_rel_name(mcx, owning_rel)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!(
                    "rule \"{rulename}\" for relation \"{relname}\" does not exist"
                ))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
            ));
        }
    };
    let td = pg_rewrite.descr();
    let mut isnull = false;
    // SAFETY: pg_rewrite row under its own descriptor; declared columns.
    let cur_enabled =
        unsafe { types_tuple::heap_getattr(tup, Anum_pg_rewrite_ev_enabled as i32, td, &mut isnull) }
            .as_u8();
    debug_assert!(!isnull);
    let mut changed = false;
    if cur_enabled != fires_when {
        let natts = td.natts as usize;
        let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        repl_values[Anum_pg_rewrite_ev_enabled as usize - 1] =
            Datum::from_i8(fires_when as i8);
        repl[Anum_pg_rewrite_ev_enabled as usize - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, td, &repl_values, &repl_isnull, &repl)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &pg_rewrite, &otid, &mut newtup)?;
        changed = true;
    } else {
        genam::systable_endscan(mcx, scan)?;
    }
    pg_rewrite.close(RowExclusiveLock)?;
    if changed {
        inval::invalidate::CacheInvalidateRelcache(rel)?;
    }
    Ok(())
}

// RemoveRewriteRuleById (rewriteRemove.c).
pub fn RemoveRewriteRuleById<'mcx>(mcx: Mcx<'mcx>, rule_oid: Oid) -> PgResult<()> {
    let pg_rewrite = table::table_open(mcx, REWRITE_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_rewrite_oid, F_OIDEQ, Datum::from_oid(rule_oid))];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_rewrite, REWRITE_OID_INDEX_ID, true, None, &keys)?;
    let tup = match genam::systable_getnext(mcx, &mut scan)? {
        Some(t) => t,
        None => {
            return Err(Box::new(PgError::error(format!(
                "could not find tuple for rule {rule_oid}"
            ))))
        }
    };
    let td = pg_rewrite.descr();
    let mut isnull = false;
    // SAFETY: pg_rewrite row under its own descriptor; ev_class declared.
    let event_relation_oid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_rewrite_ev_class as i32, td, &mut isnull)
    }
    .as_oid();
    let event_relation = table::table_open(mcx, event_relation_oid, AccessExclusiveLock)?;
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&event_relation)
    {
        return Err(Box::new(
            PgError::error(format!(
                "permission denied: \"{}\" is a system catalog",
                event_relation.name()
            ))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&pg_rewrite, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    pg_rewrite.close(RowExclusiveLock)?;
    inval::invalidate::CacheInvalidateRelcache(&event_relation)?;
    // Close rel, but keep lock till commit.
    event_relation.close(types_rel::NoLock)?;
    Ok(())
}

// RenameRewriteRule (rewriteDefine.c). Permission/relkind checks happen
// after table_openrv, unlike C's pre-lock RangeVarCallbackForRenameRule.
pub fn RenameRewriteRule<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &rel_vocab::RangeVar<'mcx>,
    old_name: &str,
    new_name: &str,
) -> PgResult<ObjectAddress> {
    let targetrel = table::table_openrv(mcx, relation, AccessExclusiveLock)?;
    let relkind = targetrel.rd_rel.relkind;
    if relkind != RELKIND_RELATION && relkind != RELKIND_VIEW && relkind != RELKIND_PARTITIONED_TABLE
    {
        return Err(Box::new(
            PgError::error(format!("relation \"{}\" cannot have rules", targetrel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(relkind_not_supported_detail(relkind)),
        ));
    }
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&targetrel) {
        return Err(Box::new(
            PgError::error(format!(
                "permission denied: \"{}\" is a system catalog",
                targetrel.name()
            ))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    if !aclchk::object_ownercheck(RELATION_RELATION_ID, targetrel.rd_id, miscinit::GetUserId())? {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, get_relkind_objtype(relkind), targetrel.name())?;
    }

    let pg_rewrite = table::table_open(mcx, REWRITE_RELATION_ID, RowExclusiveLock)?;
    let oldrname = name_image(mcx, old_name)?;
    let keys = [
        eq_key(Anum_pg_rewrite_ev_class, F_OIDEQ, Datum::from_oid(targetrel.rd_id)),
        eq_key(
            Anum_pg_rewrite_rulename,
            F_NAMEEQ,
            Datum::from_usize(oldrname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_rewrite,
        REWRITE_REL_RULENAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let Some(ruletup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(
            PgError::error(format!(
                "rule \"{old_name}\" for relation \"{}\" does not exist",
                targetrel.name()
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    let td = pg_rewrite.descr();
    let mut isnull = false;
    // SAFETY: pg_rewrite row under its own descriptor; declared columns.
    let rule_oid =
        unsafe { types_tuple::heap_getattr(ruletup, Anum_pg_rewrite_oid as i32, td, &mut isnull) }
            .as_oid();
    let ev_type =
        unsafe { types_tuple::heap_getattr(ruletup, Anum_pg_rewrite_ev_type as i32, td, &mut isnull) }
            .as_u8();

    if get_rewrite_oid(mcx, targetrel.rd_id, new_name, true)? != types_core::InvalidOid {
        return Err(Box::new(
            PgError::error(format!(
                "rule \"{new_name}\" for relation \"{}\" already exists",
                targetrel.name()
            ))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    if ev_type == CmdType::CMD_SELECT as u8 + b'0' {
        return Err(Box::new(
            PgError::error("renaming an ON SELECT rule is not allowed")
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }

    let newrname = name_image(mcx, new_name)?;
    let natts = td.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_rewrite_rulename as usize - 1] =
        Datum::from_usize(newrname.as_ptr() as usize);
    repl[Anum_pg_rewrite_rulename as usize - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, ruletup, td, &repl_values, &repl_isnull, &repl)?;
    let otid = ruletup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_rewrite, &otid, &mut newtup)?;
    pg_rewrite.close(RowExclusiveLock)?;

    inval::invalidate::CacheInvalidateRelcache(&targetrel)?;
    let address = ObjectAddress::set(REWRITE_RELATION_ID, rule_oid);
    targetrel.close(types_rel::NoLock)?;
    Ok(address)
}

// get_rewrite_oid (rewriteSupport.c).
pub fn get_rewrite_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    rulename: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let pg_rewrite = table::table_open(mcx, REWRITE_RELATION_ID, types_rel::AccessShareLock)?;
    let rname = name_image(mcx, rulename)?;
    let keys = [
        eq_key(Anum_pg_rewrite_ev_class, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(
            Anum_pg_rewrite_rulename,
            F_NAMEEQ,
            Datum::from_usize(rname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_rewrite,
        REWRITE_REL_RULENAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut ruleoid = types_core::InvalidOid;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let td = pg_rewrite.descr();
        let mut isnull = false;
        // SAFETY: pg_rewrite row under its own descriptor; oid declared.
        ruleoid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_rewrite_oid as i32, td, &mut isnull)
        }
        .as_oid();
    }
    genam::systable_endscan(mcx, scan)?;
    pg_rewrite.close(types_rel::AccessShareLock)?;
    if ruleoid == types_core::InvalidOid && !missing_ok {
        let relname =
            lsyscache::get_rel_name(mcx, relid)?.map(|s| s.to_string()).unwrap_or_default();
        return Err(Box::new(
            PgError::error(format!(
                "rule \"{rulename}\" for relation \"{relname}\" does not exist"
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(ruleoid)
}
