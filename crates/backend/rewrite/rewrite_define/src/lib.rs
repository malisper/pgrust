//! rewriteDefine.c ON SELECT (view) lane + rewriteSupport.c
//! SetRelationRuleStatus. CREATE RULE (DefineRule), non-SELECT rules, and
//! rule replace are loud panics.

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
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_WRONG_OBJECT_TYPE,
};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::Node;
use types_rel::{
    AccessExclusiveLock, Relation, RowExclusiveLock, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub const ViewSelectRuleName: &str = "_RETURN";

const RULE_FIRES_ON_ORIGIN: u8 = b'O';
const REWRITE_OID_INDEX_ID: Oid = 2692;
const REWRITE_REL_RULENAME_INDEX_ID: Oid = 2693;

const Anum_pg_rewrite_oid: AttrNumber = 1;
const Anum_pg_rewrite_rulename: AttrNumber = 2;
const Anum_pg_rewrite_ev_class: AttrNumber = 3;
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

// InsertRule (rewriteDefine.c), insert lane; replacing an existing rule is
// loud (CREATE OR REPLACE VIEW unported).
fn InsertRule<'mcx>(
    mcx: Mcx<'mcx>,
    rulname: &str,
    evtype: CmdType,
    eventrel_oid: Oid,
    evinstead: bool,
    event_qual: Option<Node<'mcx>>,
    action: NodeList<'mcx>,
    replace: bool,
) -> PgResult<Oid> {
    let evqual = match event_qual {
        Some(q) => outfuncs::nodeToString(mcx, q)?,
        None => mcx::PgString::from_str_in("<>", mcx)?,
    };
    let action_node = Node::mk_list(mcx, action)?;
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
    let oldtup = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    if oldtup {
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
        panic!("InsertRule (rewriteDefine.c): rule replace lane unported (CREATE OR REPLACE)");
    }

    let rule_oid =
        catalog::GetNewOidWithIndex(mcx, &rel, REWRITE_OID_INDEX_ID, Anum_pg_rewrite_oid)?;
    let evqual_text = varlena::cstring_to_text(mcx, evqual.as_bytes())?;
    let action_text = varlena::cstring_to_text(mcx, actiontree.as_bytes())?;
    let values = [
        Datum::from_oid(rule_oid),
        Datum::from_usize(rname.as_ptr() as usize),
        Datum::from_oid(eventrel_oid),
        Datum::from_i8((evtype as u8 + b'0') as i8),
        Datum::from_i8(RULE_FIRES_ON_ORIGIN as i8),
        Datum::from_bool(evinstead),
        Datum::from_usize(evqual_text.as_bytes().as_ptr() as usize),
        Datum::from_usize(action_text.as_bytes().as_ptr() as usize),
    ];
    let nulls = [false; 8];
    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;

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
    assert!(event_qual.is_none(), "InsertRule: qualified rule dependency lane unported");

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
        return Err(wrong_object(format!(
            "relation \"{}\" cannot have rules",
            event_relation.name()
        )));
    }
    if catalog::IsSystemRelation(&event_relation) {
        panic!("DefineQueryRewrite: allowSystemTableMods refusal lane unported (system catalog)");
    }

    if event_type != CmdType::CMD_SELECT {
        panic!("DefineQueryRewrite (rewriteDefine.c): non-SELECT rule lane unported (CREATE RULE)");
    }

    if relkind != RELKIND_VIEW && relkind != RELKIND_MATVIEW {
        return Err(wrong_object(format!(
            "relation \"{}\" cannot have ON SELECT rules",
            event_relation.name()
        )));
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
    checkRuleResultList(&query.targetList, &event_relation, relkind != RELKIND_MATVIEW)?;
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
        action,
        replace,
    )?;
    SetRelationRuleStatus(mcx, event_relid, true)?;

    // Close rel, but keep lock till commit (table_close(rel, NoLock)).
    event_relation.close(types_rel::NoLock)?;
    Ok(ObjectAddress::set(REWRITE_RELATION_ID, rule_id))
}

// checkRuleResultList (rewriteDefine.c), SELECT arm only.
fn checkRuleResultList<'mcx>(
    targetList: &NodeList<'mcx>,
    event_relation: &Relation<'mcx>,
    requireColumnNameMatch: bool,
) -> PgResult<()> {
    let desc = event_relation.descr();
    let mut i: i32 = 0;
    for item in targetList.iter() {
        let tle = item.as_target_entry().expect("targetList entry");
        if tle.resjunk {
            continue;
        }
        i += 1;
        if i > desc.natts {
            return Err(invalid_object("SELECT rule's target list has too many entries"));
        }
        let attr = desc.attr(i as usize - 1);
        let attname = core::str::from_utf8(attr.attname.name_str()).expect("attname utf8");
        if attr.attisdropped {
            return Err(feature_not_supported(
                "cannot convert relation containing dropped columns to view",
            ));
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
            panic!(
                "checkRuleResultList: type mismatch error lane unported \
                 (needs format_type_be; col {attname} {} vs tle {tletypid})",
                attr.atttypid
            );
        }
        let tletypmod = parse_expr::expr_typmod(tle.expr);
        if attr.atttypmod != tletypmod && attr.atttypmod != -1 && tletypmod != -1 {
            panic!(
                "checkRuleResultList: typmod mismatch error lane unported \
                 (needs format_type_with_typemod; col {attname})"
            );
        }
    }
    if i != desc.natts {
        return Err(invalid_object("SELECT rule's target list has too few entries"));
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
        panic!(
            "SetRelationRuleStatus: no-change relcache-inval branch unported \
             (CacheInvalidateRelcacheByTuple)"
        );
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

#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

#[cold]
#[inline(never)]
fn invalid_object(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION))
}

#[cold]
#[inline(never)]
fn wrong_object(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE))
}
