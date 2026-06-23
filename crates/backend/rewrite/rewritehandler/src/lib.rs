#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! `backend/rewrite/rewriteHandler.c` — the query-rewriter engine
//! (PostgreSQL 18.3), STEP 2 first slice.
//!
//! The previous keystone block (no value-typed `RuleLock`/`RewriteRule` reader
//! off the relcache) is resolved by the Oid-keyed
//! [`relcache_seams::relation_rules`] reader seam. This
//! slice lands the rewriteHandler.c functions whose dependency surface is
//! fully present *and acyclic now* and that have real waiting consumers:
//!
//!  * [`build_column_default`] — per-column default expression builder
//!    (consumer: nodeModifyTable). Installs the `build_column_default` seam.
//!  * the view-updatability analysis family (`view_col_is_auto_updatable`,
//!    [`view_query_is_auto_updatable`], `view_cols_are_auto_updatable`,
//!    `adjust_view_column_set`, [`view_has_instead_trigger`],
//!    [`error_view_not_updatable`]) — operate on a passed-in view `Query`
//!    + `TriggerDesc`, no rule reads. Installs the `view_query_is_auto_updatable`
//!    seam (consumer: view.c `DefineView`).
//!  * [`expand_generated_columns_in_expr`] / `expand_generated_columns_internal`
//!    / [`build_generation_expression`] — virtual generated-column expansion
//!    (consumers: publicationcmds, plancat). Installs the
//!    `expand_generated_columns_in_expr` seam.
//!
//! The rule-firing engine (`RewriteQuery`/`fireRules`/`rewriteRuleAction`), the
//! auto-updatable-view rewrite (`rewriteTargetView`, in `engine.rs`), and the
//! RIR engine (`fireRIRrules`/`ApplyRetrieveRule`/`get_view_query`/
//! `relation_is_updatable`) are ported. `rewriteTargetView` covers the
//! auto-updatable INSERT/UPDATE/DELETE-on-view rewrite; the WITH CHECK OPTION /
//! security-barrier enforcement legs are gated on the *view* `ViewOptions`
//! carrier (the trimmed `rd_options` carries only the heap `StdRdOptions`, so a
//! view's `check_option`/`security_barrier`/`security_invoker` flags read
//! `false`), and the INSERT .. ON CONFLICT EXCLUDED-RTE rebuild / sublink-view
//! locking legs remain precise mirror-and-panic boundaries. The `query_rewrite`
//! contract collapse is a separate hard STOP (see crate notes / DESIGN_DEBT
//! TD-REWRITEHANDLER-RULELOCK).

use mcx::{Mcx, PgBox, PgString};

use types_core::InvalidOid;
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};

use nodes::copy_query::Query;
use nodes::ddlnodes::CoercionContext;
use nodes::nodes::{CmdType, Node};
use nodes::parsenodes::{RTEKind, RangeTblEntry};
use nodes::primnodes::{CoercionForm, CollateExpr, Expr, NextValueExpr, TargetEntry};
use nodes::rawnodes::RangeTblRef;

use nodes_core::bitmapset::{bms_add_member, bms_is_member, bms_next_member};
use nodes_core::makefuncs::make_target_entry;
use nodes_core::nodefuncs::{expr_collation, expr_type};
use rewrite_core::change::ChangeVarNodes;
use rewrite_core::replace::{ReplaceVarsFromTargetList, ReplaceVarsNoMatchOption};

mod engine;
mod seams;
pub use engine::{
    fireRIRrules, fireRules, matchLocks, rewriteRuleAction, rewriteTargetListIU, rewriteValuesRTE,
    rewriteValuesRTEToNulls, AcquireRewriteLocks, QueryRewrite,
};
pub use seams::init_seams;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h). Consumed by the
/// view-column-set family (`view_cols_are_auto_updatable` /
/// `adjust_view_column_set`), used by `relation_is_updatable`.
pub(crate) const FirstLowInvalidHeapAttributeNumber: i32 = -7;

/// `ATTRIBUTE_GENERATED_VIRTUAL` (catalog/pg_attribute.h).
pub(crate) const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

/// `RELKIND_*` as the `i8` stored in `RangeTblEntry.relkind` / `rd_rel.relkind`.
pub(crate) const RELKIND_RELATION: i8 = b'r' as i8;
pub(crate) const RELKIND_VIEW: i8 = b'v' as i8;
pub(crate) const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
pub(crate) const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

type Relation<'mcx> = rel::Relation<'mcx>;

// ===========================================================================
// build_column_default (rewriteHandler.c:1230)
// ===========================================================================

/// `build_column_default(rel, attrno)` — build the default-value expression
/// tree for 1-based column `attrno` of `rel`, or `None` (C `NULL`) when the
/// column has no default. For an identity column this is a `NextValueExpr`;
/// for a generated column the GENERATED-AS expression. Result allocated in
/// `mcx`; reading the catalog default can `ereport(ERROR)`.
pub fn build_column_default<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attrno: i32,
) -> PgResult<Option<PgBox<'mcx, Expr<'mcx>>>> {
    let rd_att = &rel.rd_att;
    let att_tup = rd_att.attr((attrno - 1) as usize);
    let atttype = att_tup.atttypid;
    let atttypmod = att_tup.atttypmod;
    let attgenerated = att_tup.attgenerated;
    let attidentity = att_tup.attidentity;
    let atthasdef = att_tup.atthasdef;
    let attname = String::from_utf8_lossy(att_tup.attname.name_str()).into_owned();

    // if (att_tup->attidentity) { NextValueExpr ... return; }
    if attidentity != 0 {
        let seqid =
            pg_depend::getIdentitySequence(mcx, rel, attrno as i16, false)?;
        let nve = Expr::NextValueExpr(NextValueExpr {
            seqid,
            typeId: atttype,
        });
        return Ok(Some(mcx::alloc_in(mcx, nve)?));
    }

    // If relation has a default for this column, fetch that expression.
    let mut expr: Option<Expr> = None;
    if atthasdef {
        let d = tupdesc::TupleDescGetDefault(mcx, rd_att, attrno as i16)?;
        match d {
            Some(node) => expr = Some(node_into_expr(node.clone_in(mcx)?)?),
            None => {
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "default expression not found for attribute {} of relation \"{}\"",
                        attrno,
                        rel.name()
                    ),
                ));
            }
        }
    }

    // No per-column default, so look for a default for the type itself. But not
    // for generated columns.
    if expr.is_none() && attgenerated == 0 {
        if let Some(node) =
            lsyscache_seams::get_typdefault::call(mcx, atttype)?
        {
            expr = Some(node_into_expr(node.clone_in(mcx)?)?);
        }
    }

    let Some(expr) = expr else {
        return Ok(None); // No default anywhere
    };

    // Make sure the value is coerced to the target column type.
    let exprtype = expr_type(Some(&expr))?;

    let coerced = coerce::coerce_to_target_type(
        mcx,
        None, // no UNKNOWN params here
        // The coerce entry operates in the parser-arena notional `'static`; the
        // `'mcx`-built default is erased in and the result re-interned into `mcx`
        // below (`Expr` is invariant, so these are the sanctioned boundary moves).
        expr.erase_lifetime(),
        exprtype,
        atttype,
        atttypmod,
        CoercionContext::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;

    let Some(coerced) = coerced else {
        return Err(PgError::new(
            ERROR,
            format!(
                "column \"{}\" is of type {} but default expression is of type {}",
                attname,
                adt_format_type::format_type_be_str(atttype)?,
                adt_format_type::format_type_be_str(exprtype)?,
            ),
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    };

    Ok(Some(mcx::alloc_in(mcx, coerced.clone_in(mcx)?)?))
}

/// The catalog default / type default seams return a `Node`; in our split model
/// a default value is always an `Expr`. Unwrap `Node::Expr`, else it's a model
/// violation (the parser stores defaults as expressions).
fn node_into_expr(node: Node<'_>) -> PgResult<Expr> {
    let tag = node.tag();
    if let Some(e) = node.into_expr() {
        Ok(e)
    } else {
        Err(PgError::new(
            ERROR,
            format!(
                "build_column_default: default expression is not an Expr node (tag {:?})",
                tag
            ),
        ))
    }
}

// ===========================================================================
// View-updatability analysis family (rewriteHandler.c:2522-3104)
// ===========================================================================

/// `view_has_instead_trigger(view, event, mergeActionList)` — does the view
/// have an INSTEAD OF trigger for `event`?  For MERGE, true iff every action in
/// `mergeActionList` has a corresponding INSTEAD OF trigger.
pub fn view_has_instead_trigger(
    view: &Relation<'_>,
    event: CmdType,
    merge_action_list: &[nodes::nodes::NodePtr<'_>],
) -> PgResult<bool> {
    let trig = view.rd_trigdesc.as_deref();
    match event {
        CmdType::CMD_INSERT => Ok(trig.is_some_and(|t| t.trig_insert_instead_row)),
        CmdType::CMD_UPDATE => Ok(trig.is_some_and(|t| t.trig_update_instead_row)),
        CmdType::CMD_DELETE => Ok(trig.is_some_and(|t| t.trig_delete_instead_row)),
        CmdType::CMD_MERGE => {
            for node in merge_action_list {
                let Some(action) = (**node).as_mergeaction() else {
                    return Err(PgError::new(
                        ERROR,
                        format!("unrecognized node type: {:?}", (**node).tag()),
                    ));
                };
                match action.commandType {
                    CmdType::CMD_INSERT => {
                        if !trig.is_some_and(|t| t.trig_insert_instead_row) {
                            return Ok(false);
                        }
                    }
                    CmdType::CMD_UPDATE => {
                        if !trig.is_some_and(|t| t.trig_update_instead_row) {
                            return Ok(false);
                        }
                    }
                    CmdType::CMD_DELETE => {
                        if !trig.is_some_and(|t| t.trig_delete_instead_row) {
                            return Ok(false);
                        }
                    }
                    CmdType::CMD_NOTHING => { /* No trigger required */ }
                    other => {
                        return Err(PgError::new(
                            ERROR,
                            format!("unrecognized commandType: {}", other as i32),
                        ))
                    }
                }
            }
            Ok(true) // no actions without an INSTEAD OF trigger
        }
        other => Err(PgError::new(
            ERROR,
            format!("unrecognized CmdType: {}", other as i32),
        )),
    }
}

/// `view_col_is_auto_updatable(rtr, tle)` — `None` if the view column is
/// updatable, else the (untranslated) reason it is not.
fn view_col_is_auto_updatable(rtr: &RangeTblRef, tle: &TargetEntry<'_>) -> Option<&'static str> {
    // The view targetlist may contain resjunk columns which are not updatable.
    if tle.resjunk {
        return Some("Junk view columns are not updatable.");
    }

    let var = tle.expr.as_deref().and_then(Expr::as_var);
    let Some(var) = var else {
        return Some(
            "View columns that are not columns of their base relation are not updatable.",
        );
    };
    if var.varno != rtr.rtindex || var.varlevelsup != 0 {
        return Some(
            "View columns that are not columns of their base relation are not updatable.",
        );
    }

    if var.varattno < 0 {
        return Some("View columns that refer to system columns are not updatable.");
    }
    if var.varattno == 0 {
        return Some("View columns that return whole-row references are not updatable.");
    }

    None // the view column is updatable
}

/// `view_query_is_auto_updatable(viewquery, check_cols)` — `None` if the view
/// definition is automatically updatable, else the (untranslated) reason. If
/// `check_cols`, the view must have at least one updatable column.
pub fn view_query_is_auto_updatable(
    viewquery: &Query<'_>,
    check_cols: bool,
) -> PgResult<Option<&'static str>> {
    if !viewquery.distinctClause.is_empty() {
        return Ok(Some(
            "Views containing DISTINCT are not automatically updatable.",
        ));
    }
    if !viewquery.groupClause.is_empty() || !viewquery.groupingSets.is_empty() {
        return Ok(Some(
            "Views containing GROUP BY are not automatically updatable.",
        ));
    }
    if viewquery.havingQual.is_some() {
        return Ok(Some(
            "Views containing HAVING are not automatically updatable.",
        ));
    }
    if viewquery.setOperations.is_some() {
        return Ok(Some(
            "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.",
        ));
    }
    if !viewquery.cteList.is_empty() {
        return Ok(Some(
            "Views containing WITH are not automatically updatable.",
        ));
    }
    if viewquery.limitOffset.is_some() || viewquery.limitCount.is_some() {
        return Ok(Some(
            "Views containing LIMIT or OFFSET are not automatically updatable.",
        ));
    }

    if viewquery.hasAggs {
        return Ok(Some(
            "Views that return aggregate functions are not automatically updatable.",
        ));
    }
    if viewquery.hasWindowFuncs {
        return Ok(Some(
            "Views that return window functions are not automatically updatable.",
        ));
    }
    if viewquery.hasTargetSRFs {
        return Ok(Some(
            "Views that return set-returning functions are not automatically updatable.",
        ));
    }

    // The view query should select from a single base relation.
    let fromlist = viewquery
        .jointree
        .as_ref()
        .map(|jt| &jt.fromlist)
        .filter(|fl| fl.len() == 1);
    let Some(fromlist) = fromlist else {
        return Ok(Some(
            "Views that do not select from a single table or view are not automatically updatable.",
        ));
    };

    let Some(rtr) = (*fromlist[0]).as_rangetblref() else {
        return Ok(Some(
            "Views that do not select from a single table or view are not automatically updatable.",
        ));
    };

    let base_rte = rt_fetch(&viewquery.rtable, rtr.rtindex);
    if base_rte.rtekind != RTEKind::RTE_RELATION
        || (base_rte.relkind != RELKIND_RELATION
            && base_rte.relkind != RELKIND_FOREIGN_TABLE
            && base_rte.relkind != RELKIND_VIEW
            && base_rte.relkind != RELKIND_PARTITIONED_TABLE)
    {
        return Ok(Some(
            "Views that do not select from a single table or view are not automatically updatable.",
        ));
    }

    if base_rte.tablesample.is_some() {
        return Ok(Some(
            "Views containing TABLESAMPLE are not automatically updatable.",
        ));
    }

    if check_cols {
        let mut found = false;
        for tle in viewquery.targetList.iter() {
            if view_col_is_auto_updatable(rtr, tle).is_none() {
                found = true;
                break;
            }
        }
        if !found {
            return Ok(Some(
                "Views that have no updatable columns are not automatically updatable.",
            ));
        }
    }

    Ok(None) // the view is updatable
}

/// `view_cols_are_auto_updatable(viewquery, required_cols, updatable_cols,
/// non_updatable_col)` — `None` if all required columns are updatable, else the
/// reason. Optionally fills the set of updatable columns and the name of the
/// first offending non-updatable required column.
///
/// Caller `relation_is_updatable` (and the next-slice `rewriteTargetView`).
pub(crate) fn view_cols_are_auto_updatable<'mcx>(
    mcx: Mcx<'mcx>,
    viewquery: &Query<'mcx>,
    required_cols: Option<&nodes::bitmapset::Bitmapset<'_>>,
    mut updatable_cols: Option<&mut Option<PgBox<'mcx, nodes::bitmapset::Bitmapset<'mcx>>>>,
    non_updatable_col: &mut Option<String>,
) -> PgResult<Option<&'static str>> {
    // The caller verified this view is auto-updatable -> single base relation.
    let jt = viewquery.jointree.as_ref().expect("auto-updatable view has a jointree");
    let rtr = (*jt.fromlist[0])
        .as_rangetblref()
        .unwrap_or_else(|| panic!("auto-updatable view fromlist[0] is not a RangeTblRef"));

    if let Some(slot) = updatable_cols.as_deref_mut() {
        *slot = None;
    }
    *non_updatable_col = None;

    let mut col = -FirstLowInvalidHeapAttributeNumber;
    for tle in viewquery.targetList.iter() {
        col += 1;
        let detail = view_col_is_auto_updatable(rtr, tle);
        match detail {
            None => {
                // The column is updatable.
                if let Some(slot) = updatable_cols.as_deref_mut() {
                    let prev = slot.take();
                    *slot = Some(bms_add_member(mcx, prev, col)?);
                }
            }
            Some(detail) => {
                if bms_is_member(col, required_cols) {
                    *non_updatable_col = tle.resname.as_ref().map(|s| s.to_string());
                    return Ok(Some(detail));
                }
            }
        }
    }

    Ok(None) // all required view columns are updatable
}

/// `adjust_view_column_set(cols, targetlist)` — map a set of view column
/// numbers onto the matching base-relation columns (the tlist entries are plain
/// Vars of the base relation, as verified by view_query_is_auto_updatable).
///
/// Caller `relation_is_updatable` (and the next-slice `rewriteTargetView`).
pub(crate) fn adjust_view_column_set<'mcx>(
    mcx: Mcx<'mcx>,
    cols: Option<&nodes::bitmapset::Bitmapset<'_>>,
    targetlist: &[TargetEntry<'mcx>],
) -> PgResult<Option<PgBox<'mcx, nodes::bitmapset::Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, nodes::bitmapset::Bitmapset<'mcx>>> = None;

    let mut col = -1;
    loop {
        col = bms_next_member(cols, col);
        if col < 0 {
            break;
        }
        // bit numbers are offset by FirstLowInvalidHeapAttributeNumber
        let attno = col + FirstLowInvalidHeapAttributeNumber;

        if attno == 0 {
            // Whole-row reference to the view: treat as a reference to each
            // column available from the view.
            for tle in targetlist {
                if tle.resjunk {
                    continue;
                }
                let var = tle
                    .expr
                    .as_deref()
                    .and_then(Expr::as_var)
                    .expect("adjust_view_column_set: tlist entry must be a Var (castNode)");
                let prev = result.take();
                result = Some(bms_add_member(
                    mcx,
                    prev,
                    var.varattno as i32 - FirstLowInvalidHeapAttributeNumber,
                )?);
            }
        } else {
            // Views have no system columns, so any other system attno errors.
            let tle = parser_relation::get_tle_by_resno(targetlist, attno as i16);
            match tle {
                Some(tle) if !tle.resjunk && tle.expr.as_deref().is_some_and(Expr::is_var) => {
                    let var = tle.expr.as_deref().and_then(Expr::as_var).unwrap();
                    let prev = result.take();
                    result = Some(bms_add_member(
                        mcx,
                        prev,
                        var.varattno as i32 - FirstLowInvalidHeapAttributeNumber,
                    )?);
                }
                _ => {
                    return Err(PgError::new(
                        ERROR,
                        format!("attribute number {} not found in view targetlist", attno),
                    ));
                }
            }
        }
    }

    Ok(result)
}

/// `error_view_not_updatable(view, command, mergeActionList, detail)` — report
/// the error for an attempt to update a non-updatable view. Always returns
/// `Err`.
pub fn error_view_not_updatable(
    view: &Relation<'_>,
    command: CmdType,
    merge_action_list: &[nodes::nodes::NodePtr<'_>],
    detail: Option<&str>,
) -> PgError {
    let name = view.name();
    let detail_owned = detail.map(|d| d.to_string());
    let mk = |msg: String, hint: &str| -> PgError {
        let mut e = PgError::new(ERROR, msg).with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        if let Some(d) = &detail_owned {
            e = e.with_detail(d.clone());
        }
        e.with_hint(hint.to_string())
    };

    match command {
        CmdType::CMD_INSERT => mk(
            format!("cannot insert into view \"{name}\""),
            "To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule.",
        ),
        CmdType::CMD_UPDATE => mk(
            format!("cannot update view \"{name}\""),
            "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an unconditional ON UPDATE DO INSTEAD rule.",
        ),
        CmdType::CMD_DELETE => mk(
            format!("cannot delete from view \"{name}\""),
            "To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an unconditional ON DELETE DO INSTEAD rule.",
        ),
        CmdType::CMD_MERGE => {
            let trig = view.rd_trigdesc.as_deref();
            for node in merge_action_list {
                let Some(action) = (**node).as_mergeaction() else {
                    return PgError::new(
                        ERROR,
                        format!("unrecognized node type: {:?}", (**node).tag()),
                    );
                };
                match action.commandType {
                    CmdType::CMD_INSERT => {
                        if !trig.is_some_and(|t| t.trig_insert_instead_row) {
                            return mk(
                                format!("cannot insert into view \"{name}\""),
                                "To enable inserting into the view using MERGE, provide an INSTEAD OF INSERT trigger.",
                            );
                        }
                    }
                    CmdType::CMD_UPDATE => {
                        if !trig.is_some_and(|t| t.trig_update_instead_row) {
                            return mk(
                                format!("cannot update view \"{name}\""),
                                "To enable updating the view using MERGE, provide an INSTEAD OF UPDATE trigger.",
                            );
                        }
                    }
                    CmdType::CMD_DELETE => {
                        if !trig.is_some_and(|t| t.trig_delete_instead_row) {
                            return mk(
                                format!("cannot delete from view \"{name}\""),
                                "To enable deleting from the view using MERGE, provide an INSTEAD OF DELETE trigger.",
                            );
                        }
                    }
                    CmdType::CMD_NOTHING => {}
                    other => {
                        return PgError::new(
                            ERROR,
                            format!("unrecognized commandType: {}", other as i32),
                        )
                    }
                }
            }
            // The caller guarantees at least one action lacks a trigger, so we
            // should have returned above; fall through to a generic error.
            PgError::new(ERROR, format!("cannot merge into view \"{name}\""))
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        }
        other => PgError::new(ERROR, format!("unrecognized CmdType: {}", other as i32)),
    }
}

// ===========================================================================
// get_view_query (rewriteHandler.c:2483)
// ===========================================================================

/// `get_view_query(view)` — the `Query` from a view's `_RETURN` (ON SELECT)
/// rule. The caller must have verified the relation is a view. The C returns a
/// read-only pointer into the relcache's `rd_rules`; we return the canonical
/// owned [`Query`] image re-projected into `mcx` by the `relation_rules` reader.
pub fn get_view_query<'mcx>(mcx: Mcx<'mcx>, view: &Relation<'mcx>) -> PgResult<Query<'mcx>> {
    debug_assert_eq!(view.rd_rel.relkind, types_tuple::access::RELKIND_VIEW);

    let rulelocks = relcache_seams::relation_rules::call(mcx, view.rd_id)?;
    if let Some(rulelocks) = rulelocks {
        for rule in rulelocks.rules.iter() {
            if rule.event == CmdType::CMD_SELECT {
                // A _RETURN rule should have only one action.
                if rule.actions.len() != 1 {
                    return Err(PgError::new(
                        ERROR,
                        "invalid _RETURN rule action specification".to_string(),
                    ));
                }
                return rule.actions[0].clone_in(mcx);
            }
        }
    }

    Err(PgError::new(
        ERROR,
        "failed to find _RETURN rule for view".to_string(),
    ))
}

// ===========================================================================
// relation_is_updatable (rewriteHandler.c:2865)
// ===========================================================================

/// `ALL_EVENTS` (rewriteHandler.c) — `(1<<CMD_INSERT)|(1<<CMD_UPDATE)|(1<<CMD_DELETE)`.
const ALL_EVENTS: i32 =
    (1 << CmdType::CMD_INSERT as i32) | (1 << CmdType::CMD_UPDATE as i32) | (1 << CmdType::CMD_DELETE as i32);

/// `relation_is_updatable(reloid, outer_reloids, include_triggers, include_cols)`
/// — the bitmask of `CMD_*` events `reloid` supports for auto-updatable-view
/// purposes. The seam entry passes `include_col` as a single column number (the
/// C `bms_make_singleton(col)`), or `None` for the relation-level probe; the
/// `outer_reloids` recursion guard starts empty.
pub fn relation_is_updatable(
    reloid: types_core::Oid,
    include_triggers: bool,
    include_col: Option<i32>,
) -> PgResult<i32> {
    let ctx = mcx::MemoryContext::new("relation_is_updatable");
    let mcx = ctx.mcx();
    let include_cols = match include_col {
        Some(col) => Some(nodes_core::bitmapset::bms_make_singleton(mcx, col)?),
        None => None,
    };
    let mut outer_reloids: Vec<types_core::Oid> = Vec::new();
    relation_is_updatable_internal(mcx, reloid, &mut outer_reloids, include_triggers, include_cols.as_deref())
}

fn relation_is_updatable_internal<'mcx>(
    mcx: Mcx<'mcx>,
    reloid: types_core::Oid,
    outer_reloids: &mut Vec<types_core::Oid>,
    include_triggers: bool,
    include_cols: Option<&nodes::bitmapset::Bitmapset<'_>>,
) -> PgResult<i32> {
    use nodes_core::bitmapset::{bms_int_members, bms_is_empty};

    let mut events = 0;

    // Since this function recurses, it could be driven to stack overflow.
    postgres_seams::check_stack_depth::call()?;

    let rel = common_relation_seams::try_relation_open::call(
        mcx,
        reloid,
        types_storage::lock::AccessShareLock,
    )?;

    // If the relation doesn't exist, return zero rather than throwing an error.
    let Some(rel) = rel else {
        return Ok(0);
    };

    let close = |rel: Relation<'mcx>| -> PgResult<()> {
        rel.close(types_storage::lock::AccessShareLock)
    };

    // If we detect a recursive view, report that it is not updatable.
    if outer_reloids.contains(&rel.rd_id) {
        close(rel)?;
        return Ok(0);
    }

    // If the relation is a table, it is always updatable.
    if rel.rd_rel.relkind == types_tuple::access::RELKIND_RELATION
        || rel.rd_rel.relkind == types_tuple::access::RELKIND_PARTITIONED_TABLE
    {
        close(rel)?;
        return Ok(ALL_EVENTS);
    }

    // Look for unconditional DO INSTEAD rules, and note supported events.
    let rulelocks = relcache_seams::relation_rules::call(mcx, rel.rd_id)?;
    if let Some(rulelocks) = &rulelocks {
        for rule in rulelocks.rules.iter() {
            if rule.isInstead && rule.qual.is_none() {
                events |= (1 << rule.event as i32) & ALL_EVENTS;
            }
        }
        if events == ALL_EVENTS {
            close(rel)?;
            return Ok(events);
        }
    }

    // Similarly look for INSTEAD OF triggers, if they are to be included.
    if include_triggers {
        if let Some(trig) = rel.rd_trigdesc.as_deref() {
            if trig.trig_insert_instead_row {
                events |= 1 << CmdType::CMD_INSERT as i32;
            }
            if trig.trig_update_instead_row {
                events |= 1 << CmdType::CMD_UPDATE as i32;
            }
            if trig.trig_delete_instead_row {
                events |= 1 << CmdType::CMD_DELETE as i32;
            }
            if events == ALL_EVENTS {
                close(rel)?;
                return Ok(events);
            }
        }
    }

    // If this is a foreign table, check which update events it supports. The
    // repo's FdwRoutine carrier does not model the modify callbacks, so this
    // leg is computed by the foreign owner behind a seam (see the seam doc).
    if rel.rd_rel.relkind == types_tuple::access::RELKIND_FOREIGN_TABLE {
        events |= foreign_seams::foreign_rel_updatable_events::call(rel.rd_id)?;
        close(rel)?;
        return Ok(events);
    }

    // Check if this is an automatically updatable view.
    if rel.rd_rel.relkind == types_tuple::access::RELKIND_VIEW {
        let viewquery = get_view_query(mcx, &rel)?;

        if view_query_is_auto_updatable(&viewquery, false)?.is_none() {
            // Determine which of the view's columns are updatable.
            let mut updatable_cols: Option<PgBox<'mcx, nodes::bitmapset::Bitmapset<'mcx>>> =
                None;
            view_cols_are_auto_updatable(
                mcx,
                &viewquery,
                None,
                Some(&mut updatable_cols),
                &mut None,
            )?;

            if let Some(inc) = include_cols {
                updatable_cols = bms_int_members(updatable_cols, Some(inc));
            }

            let mut auto_events = if bms_is_empty(updatable_cols.as_deref()) {
                1 << CmdType::CMD_DELETE as i32 // May support DELETE
            } else {
                ALL_EVENTS // May support all events
            };

            // The base relation must also support these update commands.
            let jt = viewquery
                .jointree
                .as_ref()
                .expect("auto-updatable view has a jointree");
            let rtr = (*jt.fromlist[0])
                .as_rangetblref()
                .unwrap_or_else(|| panic!("auto-updatable view fromlist[0] is not a RangeTblRef"));
            let base_rte = rt_fetch(&viewquery.rtable, rtr.rtindex);
            debug_assert_eq!(base_rte.rtekind, RTEKind::RTE_RELATION);

            if base_rte.relkind != RELKIND_RELATION && base_rte.relkind != RELKIND_PARTITIONED_TABLE {
                let baseoid = base_rte.relid;
                outer_reloids.push(rel.rd_id);
                let new_include_cols =
                    adjust_view_column_set(mcx, updatable_cols.as_deref(), &viewquery.targetList)?;
                auto_events &= relation_is_updatable_internal(
                    mcx,
                    baseoid,
                    outer_reloids,
                    include_triggers,
                    new_include_cols.as_deref(),
                )?;
                outer_reloids.pop();
            }
            events |= auto_events;
        }
    }

    close(rel)?;
    Ok(events)
}

// ===========================================================================
// Virtual generated-column expansion (rewriteHandler.c:4449-4553)
// ===========================================================================

/// `expand_generated_columns_internal(node, rel, rt_index, rte, result_relation)`
/// — replace Vars matching `rt_index` with the relation's VIRTUAL generated
/// column expressions, if any.
fn expand_generated_columns_internal<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    rel: &Relation<'mcx>,
    rt_index: i32,
    rte: &RangeTblEntry<'mcx>,
    result_relation: i32,
) -> PgResult<()> {
    let tupdesc = &rel.rd_att;
    let has_virtual = tupdesc
        .constr
        .as_ref()
        .is_some_and(|c| c.has_generated_virtual);
    if !has_virtual {
        return Ok(());
    }

    let mut tlist: Vec<TargetEntry<'mcx>> = Vec::new();
    let natts = tupdesc.natts as usize;
    for i in 0..natts {
        let attr = tupdesc.attr(i);
        if attr.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            // build_generation_expression returns at the arena `'static`; bring
            // it into `mcx` for the in-place ChangeVarNodes walk (`Expr` invariant).
            let mut defexpr: Expr<'mcx> =
                build_generation_expression(mcx, rel, (i + 1) as i32)?.clone_in(mcx)?;
            // ChangeVarNodes(defexpr, 1, rt_index, 0)
            let mut defnode = Node::mk_expr(mcx, defexpr)?;
            ChangeVarNodes(&mut defnode, 1, rt_index, 0, mcx);
            defexpr = defnode
                .into_expr()
                .unwrap_or_else(|| unreachable!("ChangeVarNodes preserves the node kind"));
            let te = make_target_entry(mcx, defexpr, (i + 1) as i16, None, false)?;
            tlist.push(te);
        }
    }

    debug_assert!(!tlist.is_empty());

    ReplaceVarsFromTargetList(
        node,
        rt_index,
        0,
        rte,
        &tlist,
        result_relation,
        ReplaceVarsNoMatchOption::ChangeVarno,
        rt_index,
        &mut None,
        mcx,
    )?;

    Ok(())
}

/// `expand_generated_columns_in_expr(node, rel, rt_index)` — expand virtual
/// generated columns in a standalone expression (not part of a query).
pub fn expand_generated_columns_in_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Expr<'static>>,
    rel: &Relation<'mcx>,
    rt_index: i32,
) -> PgResult<Option<Expr<'static>>> {
    let Some(node) = node else {
        return Ok(None);
    };

    let tupdesc = &rel.rd_att;
    let has_virtual = tupdesc
        .constr
        .as_ref()
        .is_some_and(|c| c.has_generated_virtual);
    if !has_virtual {
        return Ok(Some(node));
    }

    // Bring the parser-arena `'static` node into `mcx` for the `Node`-walk;
    // the result is re-erased to `'static` at the return (sanctioned boundary).
    let node: Expr<'mcx> = node.clone_in(mcx)?;

    // rte = makeNode(RangeTblEntry); eref name doesn't matter.
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.eref = Some(mcx::alloc_in(
        mcx,
        nodes::rawnodes::Alias {
            aliasname: Some(PgString::from_str_in(&rel.name(), mcx)?),
            colnames: mcx::PgVec::new_in(mcx),
        },
    )?);
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = rel.rd_id;

    let mut wrapped = Node::mk_expr(mcx, node)?;
    expand_generated_columns_internal(mcx, &mut wrapped, rel, rt_index, &rte, 0)?;
    let out = wrapped
        .into_expr()
        .unwrap_or_else(|| unreachable!("expand_generated_columns_internal keeps an Expr an Expr"));
    Ok(Some(out.erase_lifetime()))
}

/// `build_generation_expression(rel, attrno)` — build the generation expression
/// for a VIRTUAL generated column; errors if there is none.
pub fn build_generation_expression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attrno: i32,
) -> PgResult<Expr<'static>> {
    let rd_att = &rel.rd_att;
    let att_tup = rd_att.attr((attrno - 1) as usize);
    let attcollid = att_tup.attcollation;

    let defexpr = build_column_default(mcx, rel, attrno)?;
    let Some(defexpr) = defexpr else {
        return Err(PgError::new(
            ERROR,
            format!(
                "no generation expression found for column number {} of table \"{}\"",
                attrno,
                rel.name()
            ),
        ));
    };
    let mut defexpr: Expr = defexpr.clone_in(mcx)?;

    // If the column definition's collation differs from the generation
    // expression's, wrap a COLLATE clause around it.
    if attcollid != InvalidOid && attcollid != expr_collation(Some(&defexpr))? {
        let ce = CollateExpr {
            arg: Some(Box::new(defexpr)),
            collOid: attcollid,
            location: -1,
        };
        defexpr = Expr::CollateExpr(ce);
    }

    // Re-erase to the arena `'static` the seam contract expects (the generation
    // expression is interned into the relcache/arena; `Expr` is invariant).
    Ok(defexpr.erase_lifetime())
}

// ===========================================================================
// Local helpers
// ===========================================================================

/// `rt_fetch(rti, rtable)` — 1-based range-table fetch.
fn rt_fetch<'a, 'mcx>(
    rtable: &'a [RangeTblEntry<'mcx>],
    rti: i32,
) -> &'a RangeTblEntry<'mcx> {
    &rtable[(rti - 1) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    /// Build a minimal view `Query` selecting two plain Var columns from a
    /// single base relation (RTE 1), as the parser emits for `SELECT a, b FROM t`.
    fn simple_view_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut q = Query::new(mcx);

        // RTE 1: the base table.
        let mut base = RangeTblEntry::new_in(mcx);
        base.rtekind = RTEKind::RTE_RELATION;
        base.relkind = RELKIND_RELATION;
        base.relid = 16400;
        q.rtable.push(base);

        // jointree: FROM (RangeTblRef 1)
        let mut fromlist = mcx::PgVec::new_in(mcx);
        fromlist.push(mcx::alloc_in(mcx, Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex: 1 }).unwrap()).unwrap());
        q.jointree = Some(
            mcx::alloc_in(
                mcx,
                nodes::rawnodes::FromExpr {
                    fromlist,
                    quals: None,
                },
            )
            .unwrap(),
        );

        // targetList: a (varattno 1), b (varattno 2), both plain Vars of RTE 1.
        for attno in 1..=2 {
            let var = nodes_core::makefuncs::make_var(1, attno, 23, -1, InvalidOid, 0);
            let tle =
                make_target_entry(mcx, Expr::Var(var), attno, Some("c"), false).unwrap();
            q.targetList.push(tle);
        }

        q
    }

    #[test]
    fn simple_select_is_auto_updatable() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let q = simple_view_query(mcx);
        assert!(view_query_is_auto_updatable(&q, false).unwrap().is_none());
        // With check_cols it still passes — it has updatable columns.
        assert!(view_query_is_auto_updatable(&q, true).unwrap().is_none());
    }

    #[test]
    fn distinct_view_is_not_auto_updatable() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let mut q = simple_view_query(mcx);
        // Adding a DISTINCT clause makes it non-auto-updatable.
        q.distinctClause
            .push(mcx::alloc_in(mcx, Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex: 1 }).unwrap()).unwrap());
        let detail = view_query_is_auto_updatable(&q, false).unwrap();
        assert_eq!(
            detail,
            Some("Views containing DISTINCT are not automatically updatable.")
        );
    }

    #[test]
    fn aggregate_view_is_not_auto_updatable() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let mut q = simple_view_query(mcx);
        q.hasAggs = true;
        let detail = view_query_is_auto_updatable(&q, false).unwrap();
        assert_eq!(
            detail,
            Some("Views that return aggregate functions are not automatically updatable.")
        );
    }
}

