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

pub(crate) fn eq_key(
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

pub(crate) struct CookedCon<'mcx> {
    pub contype: ConstrType,
    pub conoid: Oid,
    pub name: &'mcx str,
    pub attnum: AttrNumber,
    pub expr: Option<Node<'mcx>>,
    pub skip_validation: bool,
}

pub(crate) fn add_relation_new_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_col_defaults: &[(AttrNumber, Node<'mcx>, u8)],
    new_constraints: &NodeList<'mcx>,
    query_string: Option<&str>,
) -> PgResult<PgVec<'mcx, CookedCon<'mcx>>> {
    add_relation_new_constraints_ext(
        mcx,
        rel,
        new_col_defaults,
        new_constraints,
        false,
        true,
        query_string,
    )
}

pub(crate) fn add_relation_new_constraints_ext<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_col_defaults: &[(AttrNumber, Node<'mcx>, u8)],
    new_constraints: &NodeList<'mcx>,
    allow_merge: bool,
    is_local: bool,
    query_string: Option<&str>,
) -> PgResult<PgVec<'mcx, CookedCon<'mcx>>> {
    let numoldchecks = match rel.rd_att.constr.as_deref() {
        Some(c) => c.num_check as i16,
        None => 0,
    };

    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = match query_string {
        Some(q) => Some(bytes_in(mcx, q.as_bytes())?),
        None => None,
    };
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
    let mut cooked: PgVec<'mcx, CookedCon<'mcx>> = PgVec::new_in(mcx);

    for &(attnum, raw_default, generated) in new_col_defaults {
        let att = rel.rd_att.attr(attnum as usize - 1);
        let attname = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
        let expr = cook_default(
            mcx,
            &mut pstate,
            raw_default,
            att.atttypid,
            att.atttypmod,
            attname,
            generated,
            Some(rel),
        )?;
        // C skips the pg_attrdef entry for a bare NULL Const default (never
        // for generated: cookDefault's coercion keeps the expression form).
        if generated == 0 {
            if let Some(c) = expr.as_variant::<types_nodes::primnodes::Const>() {
                if c.constisnull {
                    continue;
                }
            }
        }
        let def_oid = pg_attrdef::StoreAttrDefault(mcx, rel, attnum, expr)?;
        cooked.push(CookedCon {
            contype: ConstrType::CONSTR_DEFAULT,
            conoid: def_oid,
            name: "",
            attnum,
            expr: Some(expr),
            skip_validation: false,
        });
    }

    let mut numchecks = numoldchecks;
    let mut checknames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    let mut nnnames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    for cnode in new_constraints.iter() {
        let cdef = cnode.as_variant::<Constraint>().expect("Constraint");
        let relname = core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname");
        if cdef.contype == ConstrType::CONSTR_NOTNULL {
            let colname = cdef.keys.nth(0).as_string().expect("not-null keys").sval;
            let Some((colnum, _)) =
                crate::alter::attname_lookup(mcx, rel.rd_id, colname, false)?
            else {
                return Err(Box::new(
                    PgError::error(format!(
                        "column \"{colname}\" of relation \"{relname}\" does not exist"
                    ))
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
                ));
            };
            if colnum < 0 {
                return Err(Box::new(
                    PgError::error(format!(
                        "cannot add not-null constraint on system column \"{colname}\""
                    ))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            debug_assert!(cdef.initially_valid != cdef.skip_validation);
            if pg_constraint::AdjustNotNullInheritance(
                mcx,
                rel.rd_id,
                colnum,
                cdef.conname,
                is_local,
                cdef.is_no_inherit,
                cdef.skip_validation,
                relname,
                colname,
            )? {
                continue;
            }
            let nnname = match cdef.conname {
                Some(name) => {
                    if pg_constraint::ConstraintNameIsUsed(
                        mcx,
                        pg_constraint::ConstraintCategory::Relation,
                        rel.rd_id,
                        name,
                    )? {
                        return Err(Box::new(
                            PgError::error(format!(
                                "constraint \"{name}\" for relation \"{relname}\" already exists"
                            ))
                            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
                        ));
                    }
                    mcx::PgString::from_str_in(name, mcx)?
                }
                None => pg_constraint::ChooseConstraintName(
                    mcx,
                    relname,
                    Some(colname),
                    "not_null",
                    rel.rd_rel.relnamespace,
                    &nnnames,
                )?,
            };
            nnnames.push(str_in(mcx, nnname.as_str())?);
            let conkey = [colnum];
            let mut entry = pg_constraint::ConstraintEntry::base(
                nnname.as_str(),
                rel.rd_rel.relnamespace,
                pg_constraint::CONSTRAINT_NOTNULL,
                rel.rd_id,
            );
            entry.conkey = &conkey;
            entry.n_keys = 1;
            entry.is_validated = cdef.initially_valid;
            entry.is_local = is_local;
            entry.inhcount = if is_local { 0 } else { 1 };
            entry.is_no_inherit = cdef.is_no_inherit;
            let con_oid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;
            cooked.push(CookedCon {
                contype: ConstrType::CONSTR_NOTNULL,
                conoid: con_oid,
                name: str_in(mcx, nnname.as_str())?,
                attnum: colnum,
                expr: None,
                skip_validation: cdef.skip_validation,
            });
            continue;
        }
        if cdef.contype != ConstrType::CONSTR_CHECK {
            panic!(
                "AddRelationNewConstraints (heap.c): {:?} arm unported (CHECK only)",
                cdef.contype
            );
        }
        let expr = match cdef.raw_expr {
            Some(e) => {
                debug_assert!(cdef.cooked_expr.is_none());
                cook_constraint(mcx, &mut pstate, e, relname)?
            }
            None => {
                let cooked = cdef
                    .cooked_expr
                    .expect("Constraint without raw_expr or cooked_expr");
                readfuncs::stringToNode(mcx, cooked)?
            }
        };

        let ccname = match cdef.conname {
            Some(name) => {
                if checknames.iter().any(|&n| n == name) {
                    return Err(check_constraint_exists(name));
                }
                checknames.push(name);
                if merge_with_existing_constraint(
                    mcx,
                    rel,
                    name,
                    expr,
                    allow_merge,
                    is_local,
                    cdef.is_enforced,
                    cdef.initially_valid,
                    cdef.is_no_inherit,
                )? {
                    continue;
                }
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
                    // C get_attname(..., missing_ok=true): a whole-row Var's
                    // attno 0 yields no column name.
                    if let Some(attno) = unique_attno.filter(|&a| a >= 1) {
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

        let con_oid = store_rel_check(
            mcx,
            rel,
            ccname.as_str(),
            expr,
            cdef.is_enforced,
            cdef.initially_valid,
            is_local,
            if is_local { 0 } else { 1 },
            cdef.is_no_inherit,
        )?;
        numchecks += 1;
        cooked.push(CookedCon {
            contype: ConstrType::CONSTR_CHECK,
            conoid: con_oid,
            name: str_in(mcx, ccname.as_str())?,
            attnum: 0,
            expr: Some(expr),
            skip_validation: cdef.skip_validation,
        });
    }

    // C updates pg_class.relchecks even when unchanged — the SI message forces
    // peers to rebuild relcache entries.
    set_relation_num_checks(mcx, rel, numchecks)?;
    parser_small1::free_parsestate(pstate)?;
    Ok(cooked)
}

// AddRelationNotNullConstraints (heap.c): local column constraints first
// (inhcount = matching parents), then leftover inherited ones with
// conislocal=false. Returns nncols; the caller must set_attnotnull each
// (table-level NOT NULL on an inherited column is not covered by
// BuildDescForRelation).
pub(crate) fn add_relation_not_null_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nnconstraints: &NodeList<'mcx>,
    old_notnulls: &[crate::inheritance::InheritedNotNull<'mcx>],
    existing_constraints: &[&str],
) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let relname = core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname");
    let mut nncols: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);
    let mut nnnames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    for &n in existing_constraints {
        nnnames.push(str_in(mcx, n)?);
    }
    let mut givennames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    let mut old_pending: PgVec<'mcx, bool> = mcx::vec_from_elem_in(mcx, true, old_notnulls.len());
    let cons: PgVec<'mcx, Node<'mcx>> = {
        let mut v = PgVec::new_in(mcx);
        for cnode in nnconstraints.iter() {
            v.push(cnode);
        }
        v
    };
    let mut merged: PgVec<'mcx, bool> = mcx::vec_from_elem_in(mcx, false, cons.len());
    for outerpos in 0..cons.len() {
        if merged[outerpos] {
            continue;
        }
        let cdef = cons[outerpos].as_variant::<Constraint>().expect("Constraint");
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
        // A column can only have one not-null constraint: merge later
        // duplicates into this one, checking NO INHERIT and name conflicts.
        let mut given_name = cdef.conname;
        for restpos in outerpos + 1..cons.len() {
            let other = cons[restpos].as_variant::<Constraint>().expect("Constraint");
            let other_col = other
                .keys
                .nth(0)
                .as_string()
                .expect("not-null constraint keys")
                .sval;
            if other_col != colname {
                continue;
            }
            if other.is_no_inherit != cdef.is_no_inherit {
                return Err(Box::new(
                    PgError::new(
                        types_error::ERROR,
                        format!(
                            "conflicting NO INHERIT declaration for not-null constraint on column \"{colname}\""
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
                ));
            }
            if let Some(othername) = other.conname {
                match given_name {
                    None => given_name = Some(othername),
                    Some(n) if n != othername => {
                        return Err(Box::new(
                            PgError::new(
                                types_error::ERROR,
                                format!(
                                    "conflicting not-null constraint names \"{n}\" and \"{othername}\""
                                ),
                            )
                            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
                        ));
                    }
                    Some(_) => {}
                }
            }
            merged[restpos] = true;
        }
        let mut inhcount: i16 = 0;
        for (i, old) in old_notnulls.iter().enumerate() {
            if old_pending[i] && old.attnum == attnum {
                if cdef.is_no_inherit {
                    return Err(Box::new(
                        PgError::new(
                            types_error::ERROR,
                            format!(
                                "cannot define not-null constraint with NO INHERIT on column \"{colname}\""
                            ),
                        )
                        .with_detail("The column has an inherited not-null constraint.")
                        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                    ));
                }
                inhcount += 1;
                old_pending[i] = false;
            }
        }
        // C checks user-specified names only against other user-specified
        // names (givennames); system-chosen collisions just pick another.
        let name = match given_name {
            Some(given) => {
                if givennames.iter().any(|&n| n == given) {
                    return Err(Box::new(
                        PgError::new(
                            types_error::ERROR,
                            format!(
                                "constraint \"{given}\" for relation \"{relname}\" already exists"
                            ),
                        )
                        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
                    ));
                }
                givennames.push(str_in(mcx, given)?);
                mcx::PgString::from_str_in(given, mcx)?
            }
            None => pg_constraint::ChooseConstraintName(
                mcx,
                relname,
                Some(colname),
                "not_null",
                rel.rd_rel.relnamespace,
                &nnnames,
            )?,
        };
        nnnames.push(str_in(mcx, name.as_str())?);
        let conkey = [attnum];
        let mut entry = pg_constraint::ConstraintEntry::base(
            name.as_str(),
            rel.rd_rel.relnamespace,
            pg_constraint::CONSTRAINT_NOTNULL,
            rel.rd_id,
        );
        entry.conkey = &conkey;
        entry.n_keys = 1;
        entry.inhcount = inhcount;
        entry.is_no_inherit = cdef.is_no_inherit;
        pg_constraint::CreateConstraintEntry(mcx, &entry)?;
        nncols.push(attnum);
    }

    for outer in 0..old_notnulls.len() {
        if !old_pending[outer] {
            continue;
        }
        let cooked = &old_notnulls[outer];
        let mut conname: Option<&str> = Some(cooked.name);
        let mut inhcount: i16 = 1;
        for rest in outer + 1..old_notnulls.len() {
            if old_pending[rest] && old_notnulls[rest].attnum == cooked.attnum {
                inhcount += 1;
                old_pending[rest] = false;
            }
        }
        if let Some(n) = conname {
            if nnnames.iter().any(|&t| t == n) {
                conname = None;
            }
        }
        let name = match conname {
            Some(n) => mcx::PgString::from_str_in(n, mcx)?,
            None => {
                let colname = {
                    let att = rel.rd_att.attr(cooked.attnum as usize - 1);
                    str_in(
                        mcx,
                        core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8"),
                    )?
                };
                pg_constraint::ChooseConstraintName(
                    mcx,
                    relname,
                    Some(colname),
                    "not_null",
                    rel.rd_rel.relnamespace,
                    &nnnames,
                )?
            }
        };
        nnnames.push(str_in(mcx, name.as_str())?);
        let conkey = [cooked.attnum];
        let mut entry = pg_constraint::ConstraintEntry::base(
            name.as_str(),
            rel.rd_rel.relnamespace,
            pg_constraint::CONSTRAINT_NOTNULL,
            rel.rd_id,
        );
        entry.conkey = &conkey;
        entry.n_keys = 1;
        entry.is_local = false;
        entry.inhcount = inhcount;
        pg_constraint::CreateConstraintEntry(mcx, &entry)?;
        nncols.push(cooked.attnum);
    }
    Ok(nncols)
}

pub fn cook_default<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    raw_default: Node<'mcx>,
    atttypid: Oid,
    atttypmod: i32,
    attname: &str,
    attgenerated: u8,
    rel: Option<&Relation<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let expr = parse_expr::transformExpr(
        mcx,
        pstate,
        raw_default,
        if attgenerated != 0 {
            ParseExprKind::EXPR_KIND_GENERATED_COLUMN
        } else {
            ParseExprKind::EXPR_KIND_COLUMN_DEFAULT
        },
    )?;
    if attgenerated != 0 {
        check_nested_generated(pstate, rel.expect("generated default cooks with its relation"), expr)?;
        if clauses::contain_mutable_functions_after_planning(mcx, expr)? {
            return Err(Box::new(
                PgError::new(
                    types_error::ERROR,
                    "generation expression is not immutable".to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
        if attgenerated == b'v' {
            check_virtual_generated_security(pstate, expr)?;
        }
    } else {
        debug_assert!(!vars::contain_var_clause(expr)?);
    }
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

// check_nested_generated (heap.c): generation expressions may not reference
// generated columns or the whole row. DIVERGENCE: C names the first offender
// in expression order with a cursor; this walk reports in Var-visit order
// without a cursor.
fn check_nested_generated<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    rel: &Relation<'mcx>,
    expr: Node<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'p, 'mcx> {
        pstate: &'a ParseState<'p, 'mcx>,
        rel: &'a Relation<'mcx>,
    }
    impl<'a, 'p, 'mcx> nodes_core::NodeWalker<'mcx> for W<'a, 'p, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                debug_assert!(v.varno == 1 && v.varlevelsup == 0);
                let cursor = parser_small1::parser_errposition(
                    self.pstate,
                    v.location,
                    mbutils::GetDatabaseEncoding(),
                );
                if v.varattno == 0 {
                    return Err(Box::new(
                        PgError::new(
                            types_error::ERROR,
                            "cannot use whole-row variable in column generation expression"
                                .to_string(),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                        .with_detail(
                            "This would cause the generated column to depend on its own value.",
                        )
                        .with_cursor_position(cursor),
                    ));
                }
                if v.varattno > 0
                    && self.rel.rd_att.attr(v.varattno as usize - 1).attgenerated != 0
                {
                    let att = self.rel.rd_att.attr(v.varattno as usize - 1);
                    let name =
                        core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
                    return Err(Box::new(
                        PgError::new(
                            types_error::ERROR,
                            format!(
                                "cannot use generated column \"{name}\" in column generation \
                                 expression"
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                        .with_detail(
                            "A generated column cannot reference another generated column.",
                        )
                        .with_cursor_position(cursor),
                    ));
                }
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    nodes_core::NodeWalker::visit(&mut W { pstate, rel }, expr)?;
    Ok(())
}

// check_virtual_generated_security (heap.c): virtual generation expressions
// are restricted to pinned (built-in) functions and types — selecting from a
// table with virtual generated columns is otherwise exploitable like a view
// (CVE-2024-7348).
fn check_virtual_generated_security<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'p, 'mcx> {
        pstate: &'a ParseState<'p, 'mcx>,
    }
    impl<'a, 'p, 'mcx> nodes_core::NodeWalker<'mcx> for W<'a, 'p, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            let cursor = || {
                parser_small1::parser_errposition(
                    self.pstate,
                    parse_expr::expr_location(node),
                    mbutils::GetDatabaseEncoding(),
                )
            };
            if nodes_core::check_functions_in_node(node, &mut |func_id| {
                Ok(func_id >= types_core::FirstUnpinnedObjectId)
            })? {
                return Err(Box::new(
                    PgError::new(
                        types_error::ERROR,
                        "generation expression uses user-defined function".to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_detail(
                        "Virtual generated columns that make use of user-defined functions \
                         are not yet supported.",
                    )
                    .with_cursor_position(cursor()),
                ));
            }
            if parse_expr::expr_type(node) >= types_core::FirstUnpinnedObjectId {
                return Err(Box::new(
                    PgError::new(
                        types_error::ERROR,
                        "generation expression uses user-defined type".to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_detail(
                        "Virtual generated columns that make use of user-defined types \
                         are not yet supported.",
                    )
                    .with_cursor_position(cursor()),
                ));
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    nodes_core::NodeWalker::visit(&mut W { pstate }, expr)?;
    Ok(())
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

// StoreRelCheck (heap.c).
fn store_rel_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ccname: &str,
    expr: Node<'mcx>,
    is_enforced: bool,
    is_validated: bool,
    is_local: bool,
    inhcount: i16,
    is_no_inherit: bool,
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
    // Partitioned tables hold no rows themselves, so a NO INHERIT
    // constraint makes no sense (heap.c:2195-2203).
    if is_no_inherit && rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        let relname = core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname");
        return Err(Box::new(
            types_error::PgError::new(
                types_error::ERROR,
                format!("cannot add NO INHERIT constraint to partitioned table \"{relname}\""),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let mut entry = pg_constraint::ConstraintEntry::base(
        ccname,
        rel.rd_rel.relnamespace,
        pg_constraint::CONSTRAINT_CHECK,
        rel.rd_id,
    );
    entry.conkey = &att_nos;
    entry.n_keys = att_nos.len();
    entry.is_enforced = is_enforced;
    entry.is_validated = is_validated;
    entry.is_local = is_local;
    entry.inhcount = inhcount;
    entry.is_no_inherit = is_no_inherit;
    entry.conbin = Some(ccbin.as_str());
    entry.con_expr = Some(expr);
    pg_constraint::CreateConstraintEntry(mcx, &entry)
}

// MergeWithExistingConstraint (heap.c): returns true when the new CHECK
// constraint merged into an identical pre-existing one.
fn merge_with_existing_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ccname: &str,
    expr: Node<'mcx>,
    mut allow_merge: bool,
    is_local: bool,
    is_enforced: bool,
    is_initially_valid: bool,
    is_no_inherit: bool,
) -> PgResult<bool> {
    let relname = core::str::from_utf8(rel.rd_rel.relname.name_str()).expect("relname");
    let con_rel = table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let namebuf = {
        let ccname = crate::truncate_name(mcx, ccname)?;
        let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 64)?;
        mcx::vec_append_bytes(&mut buf, ccname.as_bytes())?;
        mcx::vec_append_bytes(&mut buf, &[0u8; 64][..64 - ccname.len()])?;
        buf
    };
    let keys = [
        eq_key(pg_constraint::Anum_pg_constraint_conrelid, types_core::fmgr::F_OIDEQ, Datum::from_oid(rel.rd_id)),
        eq_key(pg_constraint::Anum_pg_constraint_contypid, types_core::fmgr::F_OIDEQ, Datum::from_oid(types_core::InvalidOid)),
        eq_key(
            pg_constraint::Anum_pg_constraint_conname,
            types_core::fmgr::F_NAMEEQ,
            Datum::from_usize(namebuf.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        genam::systable_endscan(mcx, scan)?;
        con_rel.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let get = |anum: AttrNumber| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
        unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
    };
    let contype = get(pg_constraint::Anum_pg_constraint_contype).as_i8() as u8;
    let mut found = false;
    if contype == pg_constraint::CONSTRAINT_CHECK {
        let mut isnull = false;
        // SAFETY: conbin under pg_constraint's descriptor; null-checked below.
        let val = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_constraint::Anum_pg_constraint_conbin as i32,
                desc,
                &mut isnull,
            )
        };
        if isnull {
            panic!("null conbin for rel {relname}");
        }
        let p = val.as_usize() as *const u8;
        // SAFETY: live varlena text image through its extent.
        let image =
            unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let payload = varlena::open_image(mcx, image)?;
        let conbin =
            core::str::from_utf8(payload.as_bytes()).expect("conbin UTF-8").to_string();
        let existing = readfuncs::stringToNode(mcx, &conbin)?;
        if types_nodes::equal::equal(expr, existing) {
            found = true;
        }
    }
    let conislocal = get(pg_constraint::Anum_pg_constraint_conislocal).as_bool();
    let connoinherit = get(pg_constraint::Anum_pg_constraint_connoinherit).as_bool();
    let coninhcount = get(pg_constraint::Anum_pg_constraint_coninhcount).as_i16();
    let conenforced = get(pg_constraint::Anum_pg_constraint_conenforced).as_bool();
    let convalidated = get(pg_constraint::Anum_pg_constraint_convalidated).as_bool();
    let con_oid = get(pg_constraint::Anum_pg_constraint_oid).as_oid();
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)?;

    if is_local && !conislocal && !rel.rd_rel.relispartition {
        allow_merge = true;
    }
    if !found || !allow_merge {
        return Err(Box::new(
            PgError::error(format!(
                "constraint \"{ccname}\" for relation \"{relname}\" already exists"
            ))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    if connoinherit {
        return Err(merge_conflict(ccname, relname, "non-inherited"));
    }
    if coninhcount > 0 && is_no_inherit {
        return Err(merge_conflict(ccname, relname, "inherited"));
    }
    if is_initially_valid && conenforced && !convalidated {
        return Err(merge_conflict(ccname, relname, "NOT VALID"));
    }
    if (!is_local && is_enforced && !conenforced) || (is_local && !is_enforced && conenforced) {
        return Err(merge_conflict(ccname, relname, "NOT ENFORCED"));
    }

    elog_seams::ereport::call(PgError::new(
        types_error::NOTICE,
        format!("merging constraint \"{ccname}\" with inherited definition"),
    ))?;

    let mut fields: PgVec<'mcx, (AttrNumber, Datum)> = PgVec::new_in(mcx);
    if rel.rd_rel.relispartition {
        fields.push((pg_constraint::Anum_pg_constraint_coninhcount, Datum::from_i16(1)));
        fields.push((pg_constraint::Anum_pg_constraint_conislocal, Datum::from_bool(false)));
    } else if is_local {
        fields.push((pg_constraint::Anum_pg_constraint_conislocal, Datum::from_bool(true)));
    } else {
        if coninhcount == i16::MAX {
            return Err(Box::new(
                PgError::error("too many inheritance parents".to_string())
                    .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ));
        }
        fields.push((
            pg_constraint::Anum_pg_constraint_coninhcount,
            Datum::from_i16(coninhcount + 1),
        ));
    }
    if is_no_inherit {
        debug_assert!(is_local);
        fields.push((pg_constraint::Anum_pg_constraint_connoinherit, Datum::from_bool(true)));
    }
    if is_enforced && !conenforced {
        debug_assert!(is_local);
        fields.push((pg_constraint::Anum_pg_constraint_conenforced, Datum::from_bool(true)));
        fields.push((pg_constraint::Anum_pg_constraint_convalidated, Datum::from_bool(true)));
    }
    pg_constraint::update_constraint_fields(mcx, con_oid, &fields)?;
    Ok(true)
}

#[track_caller]
#[cold]
#[inline(never)]
fn merge_conflict(ccname: &str, relname: &str, kind: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "constraint \"{ccname}\" conflicts with {kind} constraint on relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

// SetRelationNumChecks (heap.c): update pg_class.relchecks (also fires the
// SI message C relies on to rebuild peers' relcache entries).
pub(crate) fn set_relation_num_checks<'mcx>(
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
) -> PgResult<PgVec<'mcx, (AttrNumber, Node<'mcx>, u8)>> {
    let mut out: PgVec<'mcx, (AttrNumber, Node<'mcx>, u8)> = PgVec::new_in(mcx);
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
            out.push(((i + 1) as AttrNumber, raw, cd.generated));
        }
    }
    Ok(out)
}

fn bytes_in<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<&'mcx [u8]> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, b.len())?;
    mcx::vec_append_bytes(&mut v, b)?;
    Ok(v.leak())
}

pub(crate) fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    Ok(core::str::from_utf8(bytes_in(mcx, s.as_bytes())?).expect("was UTF-8"))
}

#[track_caller]
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

#[track_caller]
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

#[track_caller]
#[cold]
#[inline(never)]
fn check_constraint_exists(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("check constraint \"{name}\" already exists"))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}
