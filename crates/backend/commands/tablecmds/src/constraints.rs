// heap.c constraint-cooking slice (AddRelationNewConstraints /
// AddRelationNotNullConstraints / cookDefault / cookConstraint / StoreRelCheck
// / StoreRelNotNull / SetRelationNumChecks), hosted here because catalog_heap
// -> parse_expr would cycle (parse_relation already depends on catalog_heap).

use datum::Datum;
use mcx::{Mcx, PgVec};
use parser_small1::{make_parsestate, ParseExprKind, ParseState};
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_INVALID_COLUMN_REFERENCE,
};
use types_nodes::rawnodes::{Constraint, ConstrType};
use types_nodes::{Node, NodeList, NodeTag};
use types_rel::{AccessShareLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const Anum_pg_class_relchecks: AttrNumber = 20;

pub(crate) fn add_relation_new_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_col_defaults: &[(AttrNumber, Node<'mcx>)],
    new_constraints: &NodeList<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let numoldchecks = match rel.rd_att.constr.as_deref() {
        Some(c) => c.num_check as i16,
        None => 0,
    };

    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(bytes_in(mcx, query_string.as_bytes())?);
    let nsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        AccessShareLock,
        None,
        false,
        true,
    )?;
    parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, true, true, true)?;

    for &(attnum, raw_default) in new_col_defaults {
        let att = rel.rd_att.attr(attnum as usize - 1);
        let attname = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
        let expr = cook_default(
            mcx,
            &mut pstate,
            raw_default,
            att.atttypid,
            att.atttypmod,
            attname,
        )?;
        // C skips the pg_attrdef entry for a bare NULL Const default.
        if let Some(c) = expr.as_variant::<types_nodes::primnodes::Const>() {
            if c.constisnull {
                continue;
            }
        }
        pg_attrdef::StoreAttrDefault(mcx, rel, attnum, expr)?;
    }

    let mut numchecks = numoldchecks;
    let mut checknames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    for cnode in new_constraints.iter() {
        let cdef = cnode.as_variant::<Constraint>().expect("Constraint");
        if cdef.contype != ConstrType::CONSTR_CHECK {
            panic!(
                "AddRelationNewConstraints (heap.c): {:?} arm unported (CHECK only)",
                cdef.contype
            );
        }
        let raw_expr = match cdef.raw_expr {
            Some(e) => {
                debug_assert!(cdef.cooked_expr.is_none());
                e
            }
            None => panic!(
                "AddRelationNewConstraints (heap.c): cooked_expr (inheritance/ALTER \
                 lane) unported"
            ),
        };
        let relname = core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname");
        let expr = cook_constraint(mcx, &mut pstate, raw_expr, relname)?;

        let ccname = match cdef.conname {
            Some(name) => {
                if checknames.iter().any(|&n| n == name) {
                    return Err(check_constraint_exists(name));
                }
                checknames.push(name);
                // New relation: MergeWithExistingConstraint's probe cannot
                // match (no pre-existing constraints); ALTER lane unported.
                mcx::PgString::from_str_in(name, mcx)?
            }
            None => {
                let vars = vars::pull_var_clause(mcx, expr, 0)?;
                let mut colname: Option<&str> = None;
                let mut unique_attno: Option<i16> = None;
                let mut single = true;
                for v in vars.iter() {
                    let attno = v.as_var().expect("pull_var_clause").varattno;
                    match unique_attno {
                        None => unique_attno = Some(attno),
                        Some(a) if a == attno => {}
                        Some(_) => single = false,
                    }
                }
                if single {
                    if let Some(attno) = unique_attno {
                        let att = rel.rd_att.attr(attno as usize - 1);
                        colname =
                            Some(core::str::from_utf8(att.attname.name_str()).expect("attname"));
                    }
                }
                let name = pg_constraint::ChooseConstraintName(
                    mcx,
                    relname,
                    colname,
                    "check",
                    rel.rd_rel.relnamespace,
                    &checknames,
                )?;
                checknames.push(str_in(mcx, name.as_str())?);
                name
            }
        };

        store_rel_check(
            mcx,
            rel,
            ccname.as_str(),
            expr,
            cdef.is_enforced,
            cdef.initially_valid,
        )?;
        numchecks += 1;
    }

    if numchecks != numoldchecks || !new_col_defaults.is_empty() {
        set_relation_num_checks(mcx, rel, numchecks)?;
    }
    parser_small1::free_parsestate(pstate)?;
    Ok(())
}

// AddRelationNotNullConstraints, CREATE TABLE column-constraint arm (no
// inheritance sources; attnotnull was already set by BuildDescForRelation).
pub(crate) fn add_relation_not_null_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nnconstraints: &NodeList<'mcx>,
) -> PgResult<()> {
    let relname = core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname");
    let mut nnnames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    for cnode in nnconstraints.iter() {
        let cdef = cnode.as_variant::<Constraint>().expect("Constraint");
        debug_assert!(cdef.contype == ConstrType::CONSTR_NOTNULL);
        let colname = cdef
            .keys
            .nth(0)
            .as_string()
            .expect("not-null constraint keys")
            .sval;
        let attnum = (0..rel.rd_att.natts as usize)
            .find(|&i| rel.rd_att.attr(i).attname.name_str() == colname.as_bytes())
            .map(|i| (i + 1) as AttrNumber)
            .unwrap_or_else(|| {
                panic!("AddRelationNotNullConstraints (heap.c): column {colname:?} not found")
            });
        if cdef.conname.is_some() {
            panic!(
                "AddRelationNotNullConstraints (heap.c): named not-null constraints \
                 (ConstraintNameIsUsed) unported"
            );
        }
        let name = pg_constraint::ChooseConstraintName(
            mcx,
            relname,
            Some(colname),
            "not_null",
            rel.rd_rel.relnamespace,
            &nnnames,
        )?;
        nnnames.push(str_in(mcx, name.as_str())?);
        pg_constraint::CreateConstraintEntry(
            mcx,
            &pg_constraint::CheckOrNotNullEntry {
                name: name.as_str(),
                namespace_id: rel.rd_rel.relnamespace,
                contype: pg_constraint::CONSTRAINT_NOTNULL,
                is_enforced: true,
                is_validated: true,
                relid: rel.rd_id,
                conkey: &[attnum],
                conbin: None,
                is_local: true,
                inhcount: 0,
                is_no_inherit: cdef.is_no_inherit,
            },
        )?;
    }
    Ok(())
}

fn cook_default<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    raw_default: Node<'mcx>,
    atttypid: Oid,
    atttypmod: i32,
    attname: &str,
) -> PgResult<Node<'mcx>> {
    let expr = parse_expr::transformExpr(
        mcx,
        pstate,
        raw_default,
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT,
    )?;
    debug_assert!(!vars::contain_var_clause(expr)?);
    let type_id = parse_expr::expr_type(expr);
    let expr = match coerce::coerce_to_target_type(
        mcx,
        pstate,
        expr,
        type_id,
        atttypid,
        atttypmod,
        coerce::CoercionContext::COERCION_ASSIGNMENT,
        types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )? {
        Some(e) => e,
        None => return Err(default_type_mismatch(attname, atttypid, type_id)),
    };
    parse_collate::assign_expr_collations(mcx, pstate, expr)?;
    Ok(expr)
}

fn cook_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    raw_constraint: Node<'mcx>,
    relname: &str,
) -> PgResult<Node<'mcx>> {
    let expr = parse_expr::transformExpr(
        mcx,
        pstate,
        raw_constraint,
        ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT,
    )?;
    let expr = coerce::coerce_to_boolean(
        mcx,
        pstate,
        expr,
        parse_expr::expr_type(expr),
        parse_expr::expr_location(expr),
        "CHECK",
    )?;
    parse_collate::assign_expr_collations(mcx, pstate, expr)?;
    if pstate.p_rtable.len() != 1 {
        return Err(check_references_other_table(relname));
    }
    Ok(expr)
}

// StoreRelCheck (heap.c); partitioned-table NO INHERIT check unreachable
// (relkind gated to 'r' in DefineRelation).
fn store_rel_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ccname: &str,
    expr: Node<'mcx>,
    is_enforced: bool,
    is_validated: bool,
) -> PgResult<Oid> {
    let ccbin = outfuncs::nodeToString(mcx, expr)?;
    let var_list = vars::pull_var_clause(mcx, expr, 0)?;
    let mut att_nos: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    for v in var_list.iter() {
        let attno = v.as_var().expect("pull_var_clause").varattno;
        if !att_nos.iter().any(|&a| a == attno) {
            att_nos.push(attno);
        }
    }
    pg_constraint::CreateConstraintEntry(
        mcx,
        &pg_constraint::CheckOrNotNullEntry {
            name: ccname,
            namespace_id: rel.rd_rel.relnamespace,
            contype: pg_constraint::CONSTRAINT_CHECK,
            is_enforced,
            is_validated,
            relid: rel.rd_id,
            conkey: &att_nos,
            conbin: Some(ccbin.as_str()),
            is_local: true,
            inhcount: 0,
            is_no_inherit: false,
        },
    )
}

// SetRelationNumChecks (heap.c): update pg_class.relchecks (also fires the
// SI message C relies on to rebuild peers' relcache entries).
fn set_relation_num_checks<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    numchecks: i16,
) -> PgResult<()> {
    let relrel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(rel.rd_id);
    let mut scan = genam::systable_beginscan(
        mcx,
        &relrel,
        catalog::ClassOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {}", rel.rd_id));
    let natts = relrel.descr().natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[(Anum_pg_class_relchecks - 1) as usize] = Datum::from_i16(numchecks);
    repl[(Anum_pg_class_relchecks - 1) as usize] = true;
    let mut newtup = heaptuple::heap_modify_tuple(
        mcx,
        reltup,
        relrel.descr(),
        &repl_values,
        &repl_isnull,
        &repl,
    )?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &relrel, &otid, &mut newtup)?;
    relrel.close(RowExclusiveLock)?;
    Ok(())
}

pub(crate) fn collect_raw_defaults<'mcx>(
    mcx: Mcx<'mcx>,
    table_elts: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, (AttrNumber, Node<'mcx>)>> {
    let mut out: PgVec<'mcx, (AttrNumber, Node<'mcx>)> = PgVec::new_in(mcx);
    for (i, elt) in table_elts.iter().enumerate() {
        if elt.node_tag() != NodeTag::T_ColumnDef {
            continue;
        }
        let cd = elt
            .as_variant::<types_nodes::rawnodes::ColumnDef>()
            .expect("ColumnDef");
        if cd.cooked_default.is_some() {
            panic!("DefineRelation (tablecmds.c): cooked_default (inheritance) unported");
        }
        if let Some(raw) = cd.raw_default {
            out.push(((i + 1) as AttrNumber, raw));
        }
    }
    Ok(out)
}

fn bytes_in<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<&'mcx [u8]> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, b.len())?;
    mcx::vec_append_bytes(&mut v, b)?;
    Ok(v.leak())
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    Ok(core::str::from_utf8(bytes_in(mcx, s.as_bytes())?).expect("was UTF-8"))
}

#[cold]
#[inline(never)]
fn default_type_mismatch(attname: &str, atttypid: Oid, exprtype: Oid) -> Box<PgError> {
    let want = format_type::format_type_be(atttypid).unwrap_or_else(|_| "???".into());
    let got = format_type::format_type_be(exprtype).unwrap_or_else(|_| "???".into());
    Box::new(
        PgError::error(format!(
            "column \"{attname}\" is of type {want} but default expression is of type {got}"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        .with_hint("You will need to rewrite or cast the expression."),
    )
}

#[cold]
#[inline(never)]
fn check_references_other_table(relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "only table \"{relname}\" can be referenced in check constraint"
        ))
        .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE),
    )
}

#[cold]
#[inline(never)]
fn check_constraint_exists(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("check constraint \"{name}\" already exists"))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}
