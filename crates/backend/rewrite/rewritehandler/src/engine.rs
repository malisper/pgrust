//! `backend/rewrite/rewriteHandler.c` — the rule-application engine (slices
//! 4-5): the RIR (`ON SELECT`/view) expansion engine and the DML
//! (`INSERT`/`UPDATE`/`DELETE`) rule-firing engine, plus the top-level
//! [`QueryRewrite`] entry.
//!
//! # Reading the rule trees
//!
//! C reads a relation's rule list directly off the open relation
//! (`relation->rd_rules->rules[i]`). The trimmed per-query `rel::Relation`
//! handle this engine holds is node-vocabulary-free and carries no `rd_rules`, so
//! the rules are fetched by `Oid` through the relcache
//! [`::relcache_seams::relation_rules`] reader, which
//! re-projects the cached `RuleLock` into the caller's `mcx` arena (the C
//! `copyObject` of the relcache rules the rewriter performs before mutating).
//!
//! # `'static` SubLink sub-selects (precise seam-panic)
//!
//! The lifetime-free `Expr` enum embeds a `SubLink`'s sub-`Query` at the
//! notional `'static` lifetime (`SubLink.subselect: PgBox<'static, Query>`),
//! whereas this engine runs on a per-query `'mcx` arena and opens relations with
//! an `Mcx<'mcx>`. The C `fireRIRrules`/`AcquireRewriteLocks` descend INTO
//! sub-link sub-queries (to expand views referenced inside scalar/EXISTS
//! sub-selects), which would require re-homing a rewritten `'mcx` query back into
//! a `'static` slot — not expressible without a lifetime-laundering keystone.
//! Those sub-link-descent paths are therefore loud panics (mirror-pg-and-panic),
//! and named precisely. The common `SELECT`/`INSERT`/`UPDATE`/`DELETE`-on-view
//! spine (the view RTE in the range-table, recursing through `rte.subquery` /
//! `cte.ctequery`, which ARE `'mcx`) does not hit them.

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};

use ::types_core::primitive::Index;
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_GENERATED_ALWAYS,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SYNTAX_ERROR, ERROR,
};
use ::nodes::copy_query::{Query, QuerySource};
use ::nodes::modifytable::{OnConflictAction, OverridingKind};
use ::nodes::nodes::{CmdType, Node, NodePtr};
use ::nodes::parsenodes::{RTEKind, RangeTblEntry};
use ::nodes::primnodes::{CoerceToDomain, Expr, FieldStore, SetToDefault, SubscriptingRef, Var};
use ::nodes::rawnodes::{CommonTableExpr, LockClauseStrength, LockWaitPolicy};
use ::types_acl::acl::ACL_SELECT_FOR_UPDATE;
use ::parser_analyze::applyLockingClause;
use ::parser_relation::getRTEPermissionInfo;
use ::nodes::value::StringNode;

use ::table::table_open;
use ::nodes_core::makefuncs::{flat_copy_target_entry, make_null_const, make_target_entry};
use ::nodes_core::nodefuncs::expr_type;
use ::equalfuncs::equal_node;
use ::rewrite_core::change::ChangeVarNodes;
use ::rewrite_core::manip_rule::{AddInvertedQual, AddQual, CombineRangeTables};
use ::rewrite_core::offset::OffsetVarNodes;
use ::rewrite_core::replace::{ReplaceVarsFromTargetList, ReplaceVarsNoMatchOption};
use ::rewrite_core::walkers::{checkExprHasSubLink, rangeTableEntry_used};
use relcache_seams::{relation_rules, RewriteRuleImage};
use ::types_storage::lock::NoLock;
use ::types_tuple::heaptuple::FormData_pg_attribute;

use crate::{build_generation_expression, view_has_instead_trigger};

// `attgenerated` chars (catalog/pg_attribute.h).
const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

/// `get_generated_columns(rel, rt_index, include_stored)` (rewriteHandler.c) —
/// build a target list of `TargetEntry`s, one per VIRTUAL (and, when
/// `include_stored`, STORED) generated column of `rel`, whose expression is the
/// column's generation expression with its self-references re-pointed at
/// `rt_index`. Used by `rewriteRuleAction` to expose `NEW.<gencol>` references.
fn get_generated_columns<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    rt_index: i32,
    include_stored: bool,
) -> PgResult<Vec<::nodes::primnodes::TargetEntry<'mcx>>> {
    let mut gen_cols: Vec<::nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    let tupdesc = &rel.rd_att;

    let has_gen = tupdesc.constr.as_ref().is_some_and(|c| {
        c.has_generated_virtual || (include_stored && c.has_generated_stored)
    });
    if !has_gen {
        return Ok(gen_cols);
    }

    let natts = tupdesc.natts as usize;
    for i in 0..natts {
        let attr = tupdesc.attr(i);
        if attr.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL
            || (include_stored && attr.attgenerated == ATTRIBUTE_GENERATED_STORED)
        {
            let defexpr = build_generation_expression(mcx, rel, (i + 1) as i32)?;
            // ChangeVarNodes(defexpr, 1, rt_index, 0). The generation expression
            // comes back at the arena `'static`; bring it into `mcx` for the
            // in-place `Node`-walk (`Expr` is invariant over its lifetime).
            let mut defnode = Node::mk_expr(mcx, defexpr.clone_in(mcx)?)?;
            ChangeVarNodes(&mut defnode, 1, rt_index, 0, mcx);
            let defexpr = defnode
                .into_expr()
                .unwrap_or_else(|| unreachable!("ChangeVarNodes preserves the node kind"));
            let te = make_target_entry(mcx, defexpr, (i + 1) as i16, None, false)?;
            gen_cols.push(te);
        }
    }

    Ok(gen_cols)
}

// `RELKIND_*` (the `i8` stored in `RangeTblEntry.relkind`) used by the engine.
const RELKIND_VIEW: i8 = b'v' as i8;
const RELKIND_MATVIEW: i8 = b'm' as i8;
// `RELKIND_RELATION` / `RELKIND_PARTITIONED_TABLE` — the relkinds that can carry
// RLS policies (rowsecurity pass).
const RELKIND_PLAIN_RELATION: i8 = b'r' as i8;
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

type Relation<'mcx> = rel::Relation<'mcx>;

// `PRS2_OLD_VARNO` / `PRS2_NEW_VARNO` (primnodes.h).
const PRS2_OLD_VARNO: i32 = 1;
const PRS2_NEW_VARNO: i32 = 2;

// `OverridingKind` shorthands.
use OverridingKind::{OVERRIDING_NOT_SET, OVERRIDING_SYSTEM_VALUE, OVERRIDING_USER_VALUE};

// `attidentity` chars (catalog/pg_attribute.h).
const ATTRIBUTE_IDENTITY_ALWAYS: i8 = b'a' as i8;
const ATTRIBUTE_IDENTITY_BY_DEFAULT: i8 = b'd' as i8;

// `restrict_nonsystem_relation_kind` bit (tcop/tcopprot.h).
const RESTRICT_RELKIND_VIEW: i32 = 0x01;
// `FirstNormalObjectId` (access/transam.h).
const FirstNormalObjectId: Oid = 16384;
// `SESSION_REPLICATION_ROLE_REPLICA` (utils/guc.h).
const SESSION_REPLICATION_ROLE_REPLICA: i32 = 1;

// `ev_enabled` chars (catalog/pg_rewrite.h).
const RULE_FIRES_ON_ORIGIN: u8 = b'O';
const RULE_FIRES_ON_REPLICA: u8 = b'R';
const RULE_DISABLED: u8 = b'D';

/// `elog(ERROR, msg)` shorthand.
fn elog(msg: impl Into<String>) -> PgError {
    PgError::new(ERROR, msg.into())
}

/// `NameStr(att->attname)` as an owned `String`.
fn attname_of(att: &FormData_pg_attribute) -> String {
    String::from_utf8_lossy(att.attname.name_str()).into_owned()
}

// ===========================================================================
// rewriteTargetListIU + helpers (rewriteHandler.c:774-1222)
// ===========================================================================

/// `process_matched_tle(src_tle, prior_tle, attrName)` (rewriteHandler.c:1047)
/// — combine multiple assignments to the same target attribute into one nested
/// FieldStore/SubscriptingRef assignment.
fn process_matched_tle<'mcx>(
    mcx: Mcx<'mcx>,
    src_tle: &::nodes::primnodes::TargetEntry<'mcx>,
    prior_tle: Option<&::nodes::primnodes::TargetEntry<'mcx>>,
    attr_name: &str,
) -> PgResult<::nodes::primnodes::TargetEntry<'mcx>> {
    // Normal case: first assignment to the attribute.
    let Some(prior_tle) = prior_tle else {
        return flat_copy_target_entry(mcx, src_tle);
    };

    let mut src_expr: Expr = src_tle.expr.as_deref().expect("src tle expr").clone_in(mcx)?;
    let mut prior_expr: Expr = prior_tle.expr.as_deref().expect("prior tle expr").clone_in(mcx)?;

    // If both are CoerceToDomain over a matching domain, strip and reconstitute.
    let mut coerce_expr: Option<CoerceToDomain> = None;
    if let (Expr::CoerceToDomain(s), Expr::CoerceToDomain(p)) = (&src_expr, &prior_expr) {
        if s.resulttype == p.resulttype {
            coerce_expr = Some(CoerceToDomain {
                arg: None,
                resulttype: s.resulttype,
                resulttypmod: s.resulttypmod,
                resultcollid: s.resultcollid,
                coercionformat: s.coercionformat,
                location: s.location,
            });
            let s_arg = match &src_expr {
                Expr::CoerceToDomain(s) => match s.arg.as_deref() {
                    Some(a) => Some(a.clone_in(mcx)?),
                    None => None,
                },
                _ => unreachable!(),
            };
            let p_arg = match &prior_expr {
                Expr::CoerceToDomain(p) => match p.arg.as_deref() {
                    Some(a) => Some(a.clone_in(mcx)?),
                    None => None,
                },
                _ => unreachable!(),
            };
            src_expr = s_arg.ok_or_else(|| elog("CoerceToDomain without arg"))?;
            prior_expr = p_arg.ok_or_else(|| elog("CoerceToDomain without arg"))?;
        }
    }

    let src_input = get_assignment_input(Some(&src_expr));
    let prior_input = get_assignment_input(Some(&prior_expr));
    if src_input.is_none()
        || prior_input.is_none()
        || expr_type(Some(&src_expr))? != expr_type(Some(&prior_expr))?
    {
        return Err(PgError::new(
            ERROR,
            format!("multiple assignments to same column \"{attr_name}\""),
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    let src_input = src_input.unwrap();
    let prior_input = prior_input.unwrap();

    // Walk to the bottom of the prior assignment nest.
    let mut priorbottom: &Expr = prior_input;
    loop {
        match get_assignment_input(Some(priorbottom)) {
            None => break, // found the original Var reference
            Some(newbottom) => priorbottom = newbottom,
        }
    }
    if !equal_node(
        &Node::mk_expr(mcx, priorbottom.clone_in(mcx)?)?,
        &Node::mk_expr(mcx, src_input.clone_in(mcx)?)?,
    ) {
        return Err(PgError::new(
            ERROR,
            format!("multiple assignments to same column \"{attr_name}\""),
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }

    // Nest 'em.
    let mut newexpr: Expr = match &src_expr {
        Expr::FieldStore(src_fs) => {
            let new_fs = if let Expr::FieldStore(prior_fs) = &prior_expr {
                // combine the two
                let mut newvals = clone_expr_vec(mcx, &prior_fs.newvals)?;
                for v in src_fs.newvals.iter() {
                    newvals.push(v.clone_in(mcx)?);
                }
                let mut fieldnums = prior_fs.fieldnums.clone();
                fieldnums.extend(src_fs.fieldnums.iter().copied());
                FieldStore {
                    arg: clone_opt_box_expr(mcx, &prior_fs.arg)?,
                    newvals,
                    fieldnums,
                    resulttype: prior_fs.resulttype,
                }
            } else {
                // general case, just nest 'em
                FieldStore {
                    arg: Some(Box::new(prior_expr.clone_in(mcx)?)),
                    newvals: clone_expr_vec(mcx, &src_fs.newvals)?,
                    fieldnums: src_fs.fieldnums.clone(),
                    resulttype: src_fs.resulttype,
                }
            };
            Expr::FieldStore(new_fs)
        }
        Expr::SubscriptingRef(_) => {
            let Expr::SubscriptingRef(src_sr_copy) = src_expr.clone_in(mcx)? else {
                unreachable!()
            };
            let new_sr = SubscriptingRef {
                refexpr: Some(Box::new(prior_expr.clone_in(mcx)?)),
                ..src_sr_copy
            };
            Expr::SubscriptingRef(new_sr)
        }
        _ => return Err(elog("cannot happen")),
    };

    if let Some(coerce) = coerce_expr {
        // put back the CoerceToDomain
        let newcoerce = CoerceToDomain {
            arg: Some(Box::new(newexpr)),
            ..coerce
        };
        newexpr = Expr::CoerceToDomain(newcoerce);
    }

    let mut result = flat_copy_target_entry(mcx, src_tle)?;
    result.expr = Some(alloc_in(mcx, newexpr)?);
    Ok(result)
}

/// Deep-copy a `Vec<Expr>` into `mcx` (C: `copyObject` of a `List *`). Routes
/// each element through `Expr::clone_in` so an `Aggref`/`SubLink`/`SubPlan`
/// child is deep-copied rather than hitting the panicking derived `.clone()`.
fn clone_expr_vec<'mcx>(mcx: Mcx<'mcx>, v: &[Expr<'_>]) -> PgResult<Vec<Expr<'mcx>>> {
    let mut out = Vec::with_capacity(v.len());
    for e in v {
        out.push(e.clone_in(mcx)?);
    }
    Ok(out)
}

/// Deep-copy an `Option<Box<Expr>>` into `mcx`.
fn clone_opt_box_expr<'mcx>(
    mcx: Mcx<'mcx>,
    e: &Option<Box<Expr<'_>>>,
) -> PgResult<Option<Box<Expr<'mcx>>>> {
    match e {
        Some(b) => Ok(Some(Box::new(b.clone_in(mcx)?))),
        None => Ok(None),
    }
}

/// `get_assignment_input(node)` (rewriteHandler.c:1201) — if node is an
/// assignment node (FieldStore / SubscriptingRef store), return its input.
fn get_assignment_input<'a, 'b>(node: Option<&'a Expr<'b>>) -> Option<&'a Expr<'b>> {
    match node? {
        Expr::FieldStore(fs) => fs.arg.as_deref(),
        Expr::SubscriptingRef(sr) => {
            if sr.refassgnexpr.is_none() {
                None
            } else {
                sr.refexpr.as_deref()
            }
        }
        _ => None,
    }
}

/// `searchForDefault(rte)` (rewriteHandler.c:1300) — does any VALUES list item
/// contain a `SetToDefault`?
fn searchForDefault(rte: &RangeTblEntry<'_>) -> bool {
    for sublist in rte.values_lists.iter() {
        if let Some(list) = (**sublist).as_list() {
            for col in list.iter() {
                if (**col).as_expr().is_some_and(|e| matches!(e, Expr::SetToDefault(_))) {
                    return true;
                }
            }
        }
    }
    false
}

/// `findDefaultOnlyColumns(rte)` (rewriteHandler.c:1326) — the set of (1-based)
/// VALUES column numbers that contain ONLY `SetToDefault` items in every row.
fn findDefaultOnlyColumns<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'_>,
) -> PgResult<Option<PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>>> {
    use ::nodes_core::bitmapset::{bms_add_member, bms_del_member, bms_is_empty};
    let mut default_only_cols: Option<PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>> = None;
    let mut initialized = false;

    for sublist_node in rte.values_lists.iter() {
        let Some(sublist) = (**sublist_node).as_list() else {
            continue;
        };
        if !initialized {
            // Populate the initial result bitmap from the first row.
            let mut i = 0;
            for col in sublist.iter() {
                i += 1;
                if (**col).as_expr().is_some_and(|e| matches!(e, Expr::SetToDefault(_))) {
                    let prev = default_only_cols.take();
                    default_only_cols = Some(bms_add_member(mcx, prev, i)?);
                }
            }
            initialized = true;
        } else {
            // Update the result bitmap from this next row.
            let mut i = 0;
            for col in sublist.iter() {
                i += 1;
                if !(**col).as_expr().is_some_and(|e| matches!(e, Expr::SetToDefault(_))) {
                    let prev = default_only_cols.take();
                    default_only_cols = bms_del_member(prev, i);
                }
            }
        }
        if bms_is_empty(default_only_cols.as_deref()) {
            break;
        }
    }
    Ok(default_only_cols)
}

/// `rewriteTargetListIU(...)` (rewriteHandler.c:774) — rewrite an
/// INSERT/UPDATE/MERGE-action target list into standard form (apply defaults,
/// merge duplicate assignments, sort junk last). Returns the new target list.
/// `unused_values_attrnos` (the C `Bitmapset **`) is filled with VALUES columns
/// whose target entries were replaced by default expressions.
#[allow(clippy::too_many_arguments)]
pub fn rewriteTargetListIU<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &[::nodes::primnodes::TargetEntry<'mcx>],
    command_type: CmdType,
    override_kind: OverridingKind,
    target_relation: &Relation<'mcx>,
    values_rte: Option<&RangeTblEntry<'mcx>>,
    values_rte_index: i32,
    unused_values_attrnos: &mut Option<PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>>,
) -> PgResult<PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>>> {
    use ::nodes_core::bitmapset::{bms_add_member, bms_is_member};

    let rd_att = &target_relation.rd_att;
    let numattrs = rd_att.natts as usize;

    // new_tles[attrno-1] : the merged TLE for each real attribute.
    let mut new_tles: Vec<Option<::nodes::primnodes::TargetEntry<'mcx>>> =
        (0..numattrs).map(|_| None).collect();
    let mut junk_tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    let mut next_junk_attrno = (numattrs + 1) as i16;
    let mut default_only_cols: Option<PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>> = None;

    for old_tle in target_list.iter() {
        if !old_tle.resjunk {
            // Normal attr: stash into new_tles[]
            let attrno = old_tle.resno as i32;
            if attrno < 1 || attrno as usize > numattrs {
                return Err(elog(format!("bogus resno {attrno} in targetlist")));
            }
            let att_tup: &FormData_pg_attribute = rd_att.attr((attrno - 1) as usize);
            // ignore deleted attributes
            if att_tup.attisdropped {
                continue;
            }
            let merged = process_matched_tle(
                mcx,
                old_tle,
                new_tles[(attrno - 1) as usize].as_ref(),
                &attname_of(att_tup),
            )?;
            new_tles[(attrno - 1) as usize] = Some(merged);
        } else {
            // junk entry: re-resno above the real columns.
            let mut tle = flat_copy_target_entry(mcx, old_tle)?;
            if old_tle.resno != next_junk_attrno {
                tle.resno = next_junk_attrno;
            }
            junk_tlist.push(tle);
            next_junk_attrno += 1;
        }
    }

    let mut new_tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = Vec::new();

    for attrno in 1..=numattrs as i32 {
        let mut new_tle = new_tles[(attrno - 1) as usize].take();
        let att_tup: &FormData_pg_attribute = rd_att.attr((attrno - 1) as usize);
        if att_tup.attisdropped {
            continue;
        }

        let mut apply_default = (new_tle.is_none() && command_type == CmdType::CMD_INSERT)
            || new_tle
                .as_ref()
                .and_then(|t| t.expr.as_deref())
                .is_some_and(|e| matches!(e, Expr::SetToDefault(_)));

        if command_type == CmdType::CMD_INSERT {
            let mut values_attrno = 0i32;
            // Source attribute number for VALUES-RTE values.
            if values_rte.is_some() {
                if let Some(var) = new_tle
                    .as_ref()
                    .and_then(|t| t.expr.as_deref())
                    .and_then(|e| e.as_var())
                {
                    if var.varno == values_rte_index {
                        values_attrno = var.varattno as i32;
                    }
                }
            }

            // GENERATED ALWAYS identity: only DEFAULT (unless OVERRIDING).
            if att_tup.attidentity == ATTRIBUTE_IDENTITY_ALWAYS && !apply_default {
                if override_kind == OVERRIDING_USER_VALUE {
                    apply_default = true;
                } else if override_kind != OVERRIDING_SYSTEM_VALUE {
                    if values_attrno != 0 {
                        if default_only_cols.is_none() {
                            default_only_cols =
                                findDefaultOnlyColumns(mcx, values_rte.unwrap())?;
                        }
                        if bms_is_member(values_attrno, default_only_cols.as_deref()) {
                            apply_default = true;
                        }
                    }
                    if !apply_default {
                        return Err(PgError::new(
                            ERROR,
                            format!(
                                "cannot insert a non-DEFAULT value into column \"{}\"",
                                attname_of(att_tup)
                            ),
                        )
                        .with_sqlstate(ERRCODE_GENERATED_ALWAYS)
                        .with_detail(format!(
                            "Column \"{}\" is an identity column defined as GENERATED ALWAYS.",
                            attname_of(att_tup)
                        ))
                        .with_hint("Use OVERRIDING SYSTEM VALUE to override.".to_string()));
                    }
                }
            }

            // GENERATED BY DEFAULT identity: apply default if OVERRIDING USER VALUE.
            if att_tup.attidentity == ATTRIBUTE_IDENTITY_BY_DEFAULT
                && override_kind == OVERRIDING_USER_VALUE
            {
                apply_default = true;
            }

            // Generated columns: only DEFAULT.
            if att_tup.attgenerated != 0 && !apply_default {
                if values_attrno != 0 {
                    if default_only_cols.is_none() {
                        default_only_cols = findDefaultOnlyColumns(mcx, values_rte.unwrap())?;
                    }
                    if bms_is_member(values_attrno, default_only_cols.as_deref()) {
                        apply_default = true;
                    }
                }
                if !apply_default {
                    return Err(PgError::new(
                        ERROR,
                        format!(
                            "cannot insert a non-DEFAULT value into column \"{}\"",
                            attname_of(att_tup)
                        ),
                    )
                    .with_sqlstate(ERRCODE_GENERATED_ALWAYS)
                    .with_detail(format!(
                        "Column \"{}\" is a generated column.",
                        attname_of(att_tup)
                    )));
                }
            }

            // Track no-longer-used VALUES columns.
            if values_attrno != 0 && apply_default {
                let prev = unused_values_attrnos.take();
                *unused_values_attrnos = Some(bms_add_member(mcx, prev, values_attrno)?);
            }
        }

        if command_type == CmdType::CMD_UPDATE {
            if att_tup.attidentity == ATTRIBUTE_IDENTITY_ALWAYS && new_tle.is_some() && !apply_default
            {
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "column \"{}\" can only be updated to DEFAULT",
                        attname_of(att_tup)
                    ),
                )
                .with_sqlstate(ERRCODE_GENERATED_ALWAYS)
                .with_detail(format!(
                    "Column \"{}\" is an identity column defined as GENERATED ALWAYS.",
                    attname_of(att_tup)
                )));
            }
            if att_tup.attgenerated != 0 && new_tle.is_some() && !apply_default {
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "column \"{}\" can only be updated to DEFAULT",
                        attname_of(att_tup)
                    ),
                )
                .with_sqlstate(ERRCODE_GENERATED_ALWAYS)
                .with_detail(format!(
                    "Column \"{}\" is a generated column.",
                    attname_of(att_tup)
                )));
            }
        }

        if att_tup.attgenerated != 0 {
            // virtual stores null; stored fixed in executor.
            new_tle = None;
        } else if apply_default {
            let new_expr_box = crate::build_column_default(mcx, target_relation, attrno)?;
            let mut new_expr: Option<Expr<'mcx>> = match new_expr_box {
                Some(b) => Some(b.clone_in(mcx)?),
                None => None,
            };

            if new_expr.is_none() {
                // No default: INSERT can omit; UPDATE must set NULL explicitly.
                if command_type == CmdType::CMD_INSERT {
                    new_tle = None;
                } else {
                    // coerce_null_to_domain returns at the parser-arena `'static`;
                    // bring it into `mcx` to match `new_expr: Option<Expr<'mcx>>`
                    // (`Expr` is invariant over its lifetime).
                    new_expr = Some(
                        coerce::coerce_null_to_domain(
                            mcx,
                            att_tup.atttypid,
                            att_tup.atttypmod,
                            att_tup.attcollation,
                            att_tup.attlen as i32,
                            att_tup.attbyval,
                        )?
                        .clone_in(mcx)?,
                    );
                }
            }

            if let Some(e) = new_expr {
                new_tle = Some(make_target_entry(
                    mcx,
                    e,
                    attrno as i16,
                    Some(&attname_of(att_tup)),
                    false,
                )?);
            }
        }

        if let Some(t) = new_tle {
            new_tlist.push(t);
        }
    }

    // list_concat(new_tlist, junk_tlist)
    let mut result: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    for t in new_tlist {
        result.push(t);
    }
    for t in junk_tlist {
        result.push(t);
    }
    Ok(result)
}

/// `rewriteValuesRTE(...)` (rewriteHandler.c:1414) — replace DEFAULT items in a
/// VALUES RTE's lists with the appropriate default expressions. Returns true if
/// all DEFAULT items were replaced.
pub fn rewriteValuesRTE<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &mut Query<'mcx>,
    rti: i32,
    target_relation: &Relation<'mcx>,
    unused_cols: Option<&::nodes::bitmapset::Bitmapset<'mcx>>,
) -> PgResult<bool> {
    use ::nodes_core::bitmapset::bms_is_member;

    debug_assert!(parsetree.commandType == CmdType::CMD_INSERT);

    // Quick scan: bail if no DEFAULT placeholders.
    {
        let rte = &parsetree.rtable[(rti - 1) as usize];
        debug_assert!(rte.rtekind == RTEKind::RTE_VALUES);
        if !searchForDefault(rte) {
            return Ok(true);
        }
    }

    // Map each VALUES column (1-based) to the targetlist resno that consumes it.
    let numattrs = {
        let rte = &parsetree.rtable[(rti - 1) as usize];
        rte.values_lists
            .first()
            .and_then(|n| (**n).as_list())
            .map_or(0, |list| list.len())
    };
    let mut attrnos = vec![0i16; numattrs];
    for tle in parsetree.targetList.iter() {
        if let Some(var) = tle.expr.as_deref().and_then(|e| e.as_var()) {
            if var.varno == rti {
                let attrno = var.varattno;
                if attrno >= 1 && (attrno as usize) <= numattrs {
                    attrnos[(attrno - 1) as usize] = tle.resno;
                }
            }
        }
    }

    // Auto-updatable view? (unresolved defaults left untouched in that case.)
    let mut is_auto_updatable_view = false;
    if target_relation.rd_rel.relkind == RELKIND_VIEW as u8
        && !view_has_instead_trigger(target_relation, CmdType::CMD_INSERT, &[])?
    {
        let mut has_update = false;
        let locks = matchLocks(
            mcx,
            CmdType::CMD_INSERT,
            target_relation,
            parsetree.resultRelation,
            parsetree,
            &mut has_update,
        )?;
        let found = locks
            .iter()
            .any(|rule| rule.isInstead && rule.qual.is_none());
        if !found {
            is_auto_updatable_view = true;
        }
    }

    let rd_att = &target_relation.rd_att;
    let mut all_replaced = true;
    let mut new_values: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

    // Take the existing sublists out (we rebuild them).
    let old_values = core_take_vec(&mut parsetree.rtable[(rti - 1) as usize].values_lists);

    for sublist_node in old_values.iter() {
        let Some(sublist) = (**sublist_node).as_list() else {
            new_values.push(alloc_in(mcx, sublist_node.clone_in(mcx)?)?);
            continue;
        };
        let mut new_list: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        let mut i = 0usize;
        for col in sublist.iter() {
            let attrno = attrnos[i];
            i += 1;
            if let Some(Expr::SetToDefault(def)) = (**col).as_expr() {
                let def: SetToDefault = *def;
                // unused column -> NULL const
                if bms_is_member(i as i32, unused_cols) {
                    let nc = make_null_const(mcx, def.typeId, def.typeMod, def.collation)?;
                    new_list.push(alloc_in(mcx, Node::mk_const(mcx, nc)?)?);
                    continue;
                }
                if attrno == 0 {
                    return Err(elog(format!("cannot set value in column {i} to DEFAULT")));
                }
                let att_tup: &FormData_pg_attribute = rd_att.attr((attrno - 1) as usize);
                let new_expr: Option<Expr<'mcx>> = if !att_tup.attisdropped {
                    match crate::build_column_default(mcx, target_relation, attrno as i32)? {
                        Some(b) => Some(b.clone_in(mcx)?),
                        None => None,
                    }
                } else {
                    None // force NULL if dropped
                };
                let new_expr = match new_expr {
                    Some(e) => e,
                    None => {
                        if is_auto_updatable_view {
                            // Leave the value untouched.
                            new_list.push(alloc_in(mcx, col.clone_in(mcx)?)?);
                            all_replaced = false;
                            continue;
                        }
                        // coerce_null_to_domain returns at the parser-arena
                        // `'static`; bring it into `mcx` to unify with the
                        // `Some(e)` arm's `'mcx` Expr (`Expr` is invariant).
                        coerce::coerce_null_to_domain(
                            mcx,
                            att_tup.atttypid,
                            att_tup.atttypmod,
                            att_tup.attcollation,
                            att_tup.attlen as i32,
                            att_tup.attbyval,
                        )?
                        .clone_in(mcx)?
                    }
                };
                new_list.push(alloc_in(mcx, Node::mk_expr(mcx, new_expr)?)?);
            } else {
                new_list.push(alloc_in(mcx, col.clone_in(mcx)?)?);
            }
        }
        new_values.push(alloc_in(mcx, Node::mk_list(mcx, new_list)?)?);
    }

    parsetree.rtable[(rti - 1) as usize].values_lists = new_values;
    Ok(all_replaced)
}

/// `rewriteValuesRTEToNulls(parsetree, rte)` (rewriteHandler.c:1599) — replace
/// every remaining DEFAULT item in the VALUES RTE with a NULL constant.
pub fn rewriteValuesRTEToNulls<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &mut Query<'mcx>,
    rti: i32,
) -> PgResult<()> {
    let old_values = core_take_vec(&mut parsetree.rtable[(rti - 1) as usize].values_lists);
    let mut new_values: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for sublist_node in old_values.iter() {
        let Some(sublist) = (**sublist_node).as_list() else {
            new_values.push(alloc_in(mcx, sublist_node.clone_in(mcx)?)?);
            continue;
        };
        let mut new_list: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        for col in sublist.iter() {
            if let Some(Expr::SetToDefault(def)) = (**col).as_expr() {
                let nc = make_null_const(mcx, def.typeId, def.typeMod, def.collation)?;
                new_list.push(alloc_in(mcx, Node::mk_const(mcx, nc)?)?);
            } else {
                new_list.push(alloc_in(mcx, col.clone_in(mcx)?)?);
            }
        }
        new_values.push(alloc_in(mcx, Node::mk_list(mcx, new_list)?)?);
    }
    parsetree.rtable[(rti - 1) as usize].values_lists = new_values;
    Ok(())
}

// ===========================================================================
// matchLocks (rewriteHandler.c:1637)
// ===========================================================================

/// `matchLocks(event, relation, varno, parsetree, hasUpdate)`
/// (rewriteHandler.c:1637) — return the rule locks on `relation` matching
/// `event`. Reads the rules via the [`relation_rules`] reader seam (`Oid`-keyed).
pub fn matchLocks<'mcx>(
    mcx: Mcx<'mcx>,
    event: CmdType,
    relation: &Relation<'mcx>,
    varno: i32,
    parsetree: &Query<'mcx>,
    has_update: &mut bool,
) -> PgResult<Vec<RewriteRuleImage<'mcx>>> {
    let Some(rulelocks) = relation_rules::call(mcx, relation.rd_id)? else {
        return Ok(Vec::new());
    };

    if parsetree.commandType != CmdType::CMD_SELECT && parsetree.resultRelation != varno {
        return Ok(Vec::new());
    }

    let session_replica = guc_tables::vars::SessionReplicationRole.read()
        == SESSION_REPLICATION_ROLE_REPLICA;

    let mut matching: Vec<RewriteRuleImage<'mcx>> = Vec::new();

    for one_lock in rulelocks.rules.into_iter() {
        if one_lock.event == CmdType::CMD_UPDATE {
            *has_update = true;
        }

        // Suppress disabled / wrong-replication-role non-SELECT rules.
        if one_lock.event != CmdType::CMD_SELECT {
            if session_replica {
                if one_lock.enabled == RULE_FIRES_ON_ORIGIN || one_lock.enabled == RULE_DISABLED {
                    continue;
                }
            } else if one_lock.enabled == RULE_FIRES_ON_REPLICA || one_lock.enabled == RULE_DISABLED
            {
                continue;
            }

            // Non-SELECT rules unsupported for MERGE.
            if parsetree.commandType == CmdType::CMD_MERGE {
                return Err(PgError::new(
                    ERROR,
                    format!("cannot execute MERGE on relation \"{}\"", relation.name()),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail("MERGE is not supported for relations with rules.".to_string()));
            }
        }

        if one_lock.event == event
            && (parsetree.commandType != CmdType::CMD_SELECT
                || query_range_table_entry_used(parsetree, varno))
        {
            matching.push(one_lock);
        }
    }

    Ok(matching)
}

/// `rangeTableEntry_used((Node *) parsetree, varno, 0)` over a whole `Query`.
fn query_range_table_entry_used(parsetree: &Query<'_>, rt_index: i32) -> bool {
    let mut used = false;
    let mut walker = |n: &Node| -> bool {
        if used {
            return true;
        }
        if rangeTableEntry_used(n, rt_index, 0) {
            used = true;
            return true;
        }
        false
    };
    ::nodes_core::node_walker::query_tree_walker(parsetree, &mut walker, 0);
    used
}

// ===========================================================================
// AcquireRewriteLocks (rewriteHandler.c:148)
// ===========================================================================

/// `AcquireRewriteLocks(parsetree, forExecute, forUpdatePushedDown)`
/// (rewriteHandler.c:148) — acquire the appropriate relation locks for every
/// relation in the query, fix up dropped JOIN alias vars, and update RTE
/// relkinds. Recurses through subquery RTEs and CTEs.
///
/// The sub-link descent (`acquireLocksOnSubLinks` over `'static` `SubLink`
/// sub-selects) is the precise `'static`-keystone panic; `hasSubLinks` is rare on
/// a rule action / view query and never on the plain DML/SELECT spine that
/// reaches here.
pub fn AcquireRewriteLocks<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &mut Query<'mcx>,
    for_execute: bool,
    for_update_pushed_down: bool,
) -> PgResult<()> {
    // First, process RTEs of the current query level.
    let nrtes = parsetree.rtable.len();
    for rt_index in 1..=nrtes {
        let rtekind = parsetree.rtable[rt_index - 1].rtekind;
        match rtekind {
            RTEKind::RTE_RELATION => {
                let lockmode = if !for_execute {
                    ::types_storage::lock::AccessShareLock
                } else if for_update_pushed_down {
                    let rte = &mut parsetree.rtable[rt_index - 1];
                    if rte.rellockmode == ::types_storage::lock::AccessShareLock {
                        rte.rellockmode = ::types_storage::lock::RowShareLock;
                    }
                    rte.rellockmode
                } else {
                    parsetree.rtable[rt_index - 1].rellockmode
                };
                let relid = parsetree.rtable[rt_index - 1].relid;
                let rel = table_open(mcx, relid, lockmode)?;
                // Update the RTE's relkind in case it changed.
                let relkind = rel.rd_rel.relkind as i8;
                rel.close(NoLock)?;
                parsetree.rtable[rt_index - 1].relkind = relkind;
            }
            RTEKind::RTE_JOIN => {
                // Scan the join's alias var list, replacing dropped-column Vars
                // with NULLs. (The C stores a literal NULL pointer; the owned
                // model has no null NodePtr, so a dropped column becomes a null
                // `Const` of the Var's type — the convention
                // `get_rte_attribute_is_dropped` recognizes.)
                let aliasvars = core_take_vec(&mut parsetree.rtable[rt_index - 1].joinaliasvars);
                let mut newaliasvars: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                let mut curinputvarno: i32 = 0;
                for aliasitem in aliasvars {
                    // Strip implicit coercions to find the underlying Var.
                    let (var_type, var_typmod, var_coll, dropped) = {
                        match strip_to_var(&aliasitem) {
                            Some(var) => {
                                debug_assert!(var.varlevelsup == 0);
                                if var.varno != curinputvarno {
                                    curinputvarno = var.varno;
                                    if curinputvarno as usize >= rt_index {
                                        return Err(elog(format!(
                                            "unexpected varno {curinputvarno} in JOIN RTE {rt_index}"
                                        )));
                                    }
                                }
                                let curinputrte =
                                    &parsetree.rtable[(curinputvarno - 1) as usize];
                                let dropped = ::parser_relation::get_rte_attribute_is_dropped(
                                    mcx,
                                    curinputrte,
                                    var.varattno,
                                )?;
                                (var.vartype, var.vartypmod, var.varcollid, dropped)
                            }
                            None => (InvalidOid, -1, InvalidOid, false),
                        }
                    };
                    if !dropped {
                        newaliasvars.push(aliasitem);
                    } else {
                        let nc = make_null_const(mcx, var_type, var_typmod, var_coll)?;
                        newaliasvars.push(alloc_in(mcx, Node::mk_const(mcx, nc)?)?);
                    }
                }
                parsetree.rtable[rt_index - 1].joinaliasvars = newaliasvars;
            }
            RTEKind::RTE_SUBQUERY => {
                let pushed = for_update_pushed_down
                    || ::parser_relation::get_parse_rowmark(parsetree, rt_index as Index)
                        .is_some();
                let mut sub = parsetree.rtable[rt_index - 1]
                    .subquery
                    .take()
                    .expect("subquery RTE has a subquery");
                AcquireRewriteLocks(mcx, &mut sub, for_execute, pushed)?;
                parsetree.rtable[rt_index - 1].subquery = Some(sub);
            }
            _ => {}
        }
    }

    // Recurse into WITH subqueries.
    let ncte = parsetree.cteList.len();
    for i in 0..ncte {
        let mut ctequery = take_cte_query(&mut parsetree.cteList[i]);
        if let Some(q) = ctequery.as_mut() {
            AcquireRewriteLocks(mcx, q, for_execute, false)?;
        }
        if let Some(q) = ctequery {
            set_cte_query(mcx, &mut parsetree.cteList[i], q)?;
        }
    }

    // Recurse into sub-link sub-queries.  But we already did the ones in the
    // rtable and cteList (so skip those, like the C walker's
    // QTW_IGNORE_RC_SUBQUERIES).
    if parsetree.hasSubLinks {
        let mut err: Option<::types_error::PgError> = None;
        {
            let mut walker = |node: &mut Node<'mcx>| {
                acquireLocksOnSubLinks(mcx, node, for_execute, &mut err)
            };
            ::nodes_core::node_walker::query_tree_mutator(
                parsetree,
                &mut walker,
                ::nodes_core::node_walker::QTW_IGNORE_RT_SUBQUERIES
                    | ::nodes_core::node_walker::QTW_IGNORE_CTE_SUBQUERIES,
                mcx,
            );
        }
        if let Some(e) = err {
            return Err(e);
        }
    }

    Ok(())
}

/// `acquireLocksOnSubLinks(node, context)` (rewriteHandler.c:295) — apply
/// [`AcquireRewriteLocks`] to each `SubLink`'s sub-select found in an expression
/// tree. Like the C, it has the form of a walker but modifies the `SubLink` in
/// place: it takes control at the `SubLink` to lock-acquire on its `subselect`
/// (and continues into the surrounding expression). It does NOT recurse into
/// `Query` nodes — the surrounding `AcquireRewriteLocks` already handled the
/// rtable/cteList subqueries (QTW_IGNORE_RC_SUBQUERIES). The `'static` subselect
/// is rebound to `'mcx` (the data is mcx-owned) exactly as `fireRIRonSubLink`
/// does. The C walker's `bool` becomes the early-abort signal carried alongside
/// the `err` out-param.
fn acquireLocksOnSubLinks<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    for_execute: bool,
    err: &mut Option<::types_error::PgError>,
) -> bool {
    if err.is_some() {
        return true;
    }
    if let Some(Expr::SubLink(sub)) = node.as_expr_mut() {
        // C: AcquireRewriteLocks(sublink->subselect, context->for_execute, false).
        if let Some(sub_box) = sub.subselect.take() {
            // `SubLink::subselect` now threads `'mcx` (the Expr-'mcx flip), so the
            // subquery is already arena-owned at `'mcx` — no `'static` relabel
            // transmute / re-intern is needed; mutate in place and re-box at `'mcx`.
            let mut subquery: Query<'mcx> = PgBox::into_inner(sub_box);
            if let Err(e) = AcquireRewriteLocks(mcx, &mut subquery, for_execute, false) {
                *err = Some(e);
                return true;
            }
            match ::mcx::alloc_in(mcx, subquery) {
                Ok(boxed) => sub.subselect = Some(boxed),
                Err(e) => {
                    *err = Some(e);
                    return true;
                }
            }
        }
        // Fall through to process the SubLink's testexpr (lefthand args).
    }

    // Do NOT recurse into Query nodes (the QTW behavior).
    ::nodes_core::node_walker::expression_tree_walker_mut(
        node,
        &mut |n| acquireLocksOnSubLinks(mcx, n, for_execute, err),
        mcx,
    )
}

/// Look through an implicit coercion to a `Var` (the C `strip_implicit_coercions`
/// then `IsA(.., Var)`). Returns `None` for a NULL slot, a merged USING column,
/// or any non-Var.
fn strip_to_var<'a>(node: &'a NodePtr<'_>) -> Option<&'a Var> {
    (**node)
        .as_expr()
        .and_then(|e| ::nodes_core::nodefuncs::strip_implicit_coercions(e).as_var())
}

// ===========================================================================
// rewriteRuleAction (rewriteHandler.c:351)
// ===========================================================================

/// `rewriteRuleAction(parsetree, rule_action, rule_qual, rt_index, event,
/// returning_flag)` (rewriteHandler.c:351). `rule_action`/`rule_qual` are taken
/// by value (the C makes modifiable copies of the relcache versions; the
/// [`relation_rules`] reader already handed us `mcx`-owned copies).
pub fn rewriteRuleAction<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    mut rule_action: Query<'mcx>,
    rule_qual: Option<PgBox<'mcx, Node<'mcx>>>,
    rt_index: i32,
    event: CmdType,
    returning_flag: &mut bool,
) -> PgResult<Query<'mcx>> {
    // Acquire locks on the rule action and fix deleted JOIN RTEs.
    AcquireRewriteLocks(mcx, &mut rule_action, true, false)?;
    let mut rule_qual = rule_qual;
    if let Some(q) = rule_qual.as_deref_mut() {
        acquire_locks_on_sublinks_node(mcx, q)?;
    }

    let current_varno = rt_index;
    let rt_length = parsetree.rtable.len() as i32;
    let new_varno = PRS2_NEW_VARNO + rt_length;

    // Find the sub-action: the INSERT...SELECT sub-query, or the action itself.
    let sub_action_idx = ::rewrite_core::getInsertSelectQueryIndex(&rule_action)?;

    // Operate on the sub-action in place. Helper closures take the right query.
    {
        // sub_action borrow.
        let sub_action: &mut Query<'mcx> = match sub_action_idx {
            None => &mut rule_action,
            Some(idx) => rule_action.rtable[idx - 1]
                .subquery
                .as_deref_mut()
                .expect("INSERT/SELECT sub-action subquery present"),
        };

        // OffsetVarNodes(sub_action, rt_length, 0); OffsetVarNodes(rule_qual, ...)
        let mut sub_node = Node::mk_query(mcx, core_clone(sub_action, mcx)?)?;
        OffsetVarNodes(&mut sub_node, rt_length, 0, mcx);
        // references to OLD should point at original rt_index
        ChangeVarNodes(&mut sub_node, PRS2_OLD_VARNO + rt_length, rt_index, 0, mcx);
        *sub_action = sub_node.into_query().unwrap_or_else(|| unreachable!());
    }
    if let Some(q) = rule_qual.as_deref_mut() {
        OffsetVarNodes(q, rt_length, 0, mcx);
        ChangeVarNodes(q, PRS2_OLD_VARNO + rt_length, rt_index, 0, mcx);
    }

    // The remaining steps need the sub-action again; re-borrow.
    let sub_action: &mut Query<'mcx> = match sub_action_idx {
        None => &mut rule_action,
        Some(idx) => rule_action.rtable[idx - 1]
            .subquery
            .as_deref_mut()
            .expect("INSERT/SELECT sub-action subquery present"),
    };

    // Mark subquery RTEs LATERAL if they reference the current query level.
    for rte in sub_action.rtable.iter_mut() {
        if rte.rtekind == RTEKind::RTE_SUBQUERY && !rte.lateral {
            if let Some(subq) = rte.subquery.as_deref() {
                let sn = Node::mk_query(mcx, core_clone(subq, mcx)?)?;
                if contain_vars_of_level(&sn, 1) {
                    rte.lateral = true;
                }
            }
        }
    }

    // Generate the expanded rtable: main parsetree rtable + rule action rtable.
    {
        let rtable_tail: PgVec<'mcx, RangeTblEntry<'mcx>> =
            core_take_vec(&mut sub_action.rtable);
        let perminfos_tail: PgVec<'mcx, ::nodes::parsenodes::RTEPermissionInfo<'mcx>> =
            core_take_vec(&mut sub_action.rteperminfos);

        // sub_action->rtable = copyObject(parsetree->rtable)
        let mut new_rtable: PgVec<'mcx, RangeTblEntry<'mcx>> = PgVec::new_in(mcx);
        for rte in parsetree.rtable.iter() {
            new_rtable.push(rte.clone_in(mcx)?);
        }
        let mut new_perminfos: PgVec<'mcx, ::nodes::parsenodes::RTEPermissionInfo<'mcx>> =
            PgVec::new_in(mcx);
        for pi in parsetree.rteperminfos.iter() {
            new_perminfos.push(pi.clone_in(mcx)?);
        }
        CombineRangeTables(
            &mut new_rtable,
            &mut new_perminfos,
            rtable_tail,
            perminfos_tail,
        );
        sub_action.rtable = new_rtable;
        sub_action.rteperminfos = new_perminfos;
    }

    // SubLinks in parsetree's rtable -> mark sub_action.
    if parsetree.hasSubLinks && !sub_action.hasSubLinks {
        for rte in parsetree.rtable.iter() {
            match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    if let Some(ts) = rte.tablesample.as_deref() {
                        sub_action.hasSubLinks = checkExprHasSubLink(ts);
                    }
                }
                RTEKind::RTE_FUNCTION => {
                    for f in rte.functions.iter() {
                        if checkExprHasSubLink(f) {
                            sub_action.hasSubLinks = true;
                            break;
                        }
                    }
                }
                RTEKind::RTE_TABLEFUNC => {
                    if let Some(tf) = rte.tablefunc.as_deref() {
                        sub_action.hasSubLinks = checkExprHasSubLink(tf);
                    }
                }
                RTEKind::RTE_VALUES => {
                    for v in rte.values_lists.iter() {
                        if checkExprHasSubLink(v) {
                            sub_action.hasSubLinks = true;
                            break;
                        }
                    }
                }
                _ => {}
            }
            for sq in rte.securityQuals.iter() {
                if checkExprHasSubLink(sq) {
                    sub_action.hasSubLinks = true;
                    break;
                }
            }
            if sub_action.hasSubLinks {
                break;
            }
        }
    }

    sub_action.hasRowSecurity |= parsetree.hasRowSecurity;

    // Build the action's jointree = main jointree (minus original rtindex) +
    // rule's jointree.
    if sub_action.commandType != CmdType::CMD_UTILITY {
        let keeporig = {
            let jointree_used = sub_action
                .jointree
                .as_deref()
                .map(|jt| {
                    let mut used = false;
                    for item in jt.fromlist.iter() {
                        if rangeTableEntry_used(item, rt_index, 0) {
                            used = true;
                            break;
                        }
                    }
                    if !used {
                        if let Some(q) = jt.quals.as_deref() {
                            used = rangeTableEntry_used(q, rt_index, 0);
                        }
                    }
                    used
                })
                .unwrap_or(false);
            let qual_used = rule_qual
                .as_deref()
                .map(|q| rangeTableEntry_used(q, rt_index, 0))
                .unwrap_or(false);
            let parse_qual_used = parsetree
                .jointree
                .as_deref()
                .and_then(|jt| jt.quals.as_deref())
                .map(|q| rangeTableEntry_used(q, rt_index, 0))
                .unwrap_or(false);
            !jointree_used && (qual_used || parse_qual_used)
        };

        let newjointree = ::rewrite_core::manip_rule::adjustJoinTreeList(
            parsetree, !keeporig, rt_index, mcx,
        )?;
        if !newjointree.is_empty() {
            if sub_action.setOperations.is_some() {
                return Err(PgError::new(
                    ERROR,
                    "conditional UNION/INTERSECT/EXCEPT statements are not implemented".to_string(),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            // sub_action->jointree->fromlist = list_concat(newjointree, fromlist)
            let mut had_sublink = false;
            {
                let jt = sub_action
                    .jointree
                    .as_deref_mut()
                    .expect("non-utility action has a jointree");
                let mut combined: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                for item in newjointree.iter() {
                    if parsetree.hasSubLinks && checkExprHasSubLink(item) {
                        had_sublink = true;
                    }
                    combined.push(alloc_in(mcx, item.clone_in(mcx)?)?);
                }
                for item in jt.fromlist.drain(..) {
                    combined.push(item);
                }
                jt.fromlist = combined;
            }
            if parsetree.hasSubLinks && !sub_action.hasSubLinks && had_sublink {
                sub_action.hasSubLinks = true;
            }
        }
    }

    // Copy CTEs from the original query into the rule action.
    if !parsetree.cteList.is_empty() && sub_action.commandType != CmdType::CMD_UTILITY {
        for cte in parsetree.cteList.iter() {
            let cte_name = cte_name_of(cte);
            for cte2 in sub_action.cteList.iter() {
                if cte_name.is_some() && cte_name == cte_name_of(cte2) {
                    return Err(PgError::new(
                        ERROR,
                        format!(
                            "WITH query name \"{}\" appears in both a rule action and the query being rewritten",
                            cte_name.as_deref().unwrap_or("")
                        ),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
                }
            }
        }
        for cte in parsetree.cteList.iter() {
            sub_action.cteList.push(alloc_in(mcx, cte.clone_in(mcx)?)?);
        }
        sub_action.hasRecursive |= parsetree.hasRecursive;
        sub_action.hasModifyingCTE |= parsetree.hasModifyingCTE;

        if sub_action.hasModifyingCTE && sub_action_idx.is_some() {
            return Err(PgError::new(
                ERROR,
                "INSERT ... SELECT rule actions are not supported for queries having data-modifying statements in WITH".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    // AddQual(sub_action, rule_qual); AddQual(sub_action, parsetree->jointree->quals);
    AddQual(sub_action, rule_qual.as_deref(), mcx)?;
    let parse_quals = parsetree
        .jointree
        .as_deref()
        .and_then(|jt| jt.quals.as_deref());
    AddQual(sub_action, parse_quals, mcx)?;

    // Rewrite NEW.attribute with the target-list entry's RHS.
    if (event == CmdType::CMD_INSERT || event == CmdType::CMD_UPDATE)
        && sub_action.commandType != CmdType::CMD_UTILITY
    {
        let new_rte = sub_action.rtable[(new_varno - 1) as usize].clone_in(mcx)?;
        let tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = parsetree
            .targetList
            .iter()
            .map(|t| t.clone_in(mcx))
            .collect::<PgResult<_>>()?;
        let result_relation = sub_action.resultRelation;
        let nomatch = if event == CmdType::CMD_UPDATE {
            ReplaceVarsNoMatchOption::ChangeVarno
        } else {
            ReplaceVarsNoMatchOption::SubstituteNull
        };
        let mut sub_node = Node::mk_query(mcx, core_clone(sub_action, mcx)?)?;
        let mut outer = None;
        ReplaceVarsFromTargetList(
            &mut sub_node,
            new_varno,
            0,
            &new_rte,
            &tlist,
            result_relation,
            nomatch,
            current_varno,
            &mut outer,
            mcx,
        )?;
        *sub_action = sub_node.into_query().unwrap_or_else(|| unreachable!());
    }

    // Now drop the sub_action borrow; handle RETURNING on rule_action.
    if parsetree.returningList.is_empty() {
        rule_action.returningList = PgVec::new_in(mcx);
        rule_action.has_returning_list = false;
    } else if !rule_action.returningList.is_empty() {
        if *returning_flag {
            return Err(PgError::new(
                ERROR,
                "cannot have RETURNING lists in multiple rules".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        *returning_flag = true;

        let result_rte = parsetree.rtable[(parsetree.resultRelation - 1) as usize].clone_in(mcx)?;
        let action_tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = rule_action
            .returningList
            .iter()
            .map(|t| t.clone_in(mcx))
            .collect::<PgResult<_>>()?;
        let action_result_relation = rule_action.resultRelation;

        // ReplaceVarsFromTargetList over parsetree->returningList, stored back
        // into rule_action->returningList.
        let mut ret_list: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
            PgVec::new_in(mcx);
        let mut had_sublink = rule_action.hasSubLinks;
        for tle in parsetree.returningList.iter() {
            let mut node = Node::mk_target_entry(mcx, tle.clone_in(mcx)?)?;
            let mut outer = Some(had_sublink);
            ReplaceVarsFromTargetList(
                &mut node,
                parsetree.resultRelation,
                0,
                &result_rte,
                &action_tlist,
                action_result_relation,
                ReplaceVarsNoMatchOption::ReportError,
                0,
                &mut outer,
                mcx,
            )?;
            if let Some(f) = outer {
                had_sublink = f;
            }
            ret_list.push(node.into_targetentry().unwrap_or_else(|| unreachable!()));
        }
        rule_action.hasSubLinks = had_sublink;
        rule_action.returningList = ret_list;
        rule_action.has_returning_list = true;

        rule_action.returningOldAlias = match &parsetree.returningOldAlias {
            Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };
        rule_action.returningNewAlias = match &parsetree.returningNewAlias {
            Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };

        if parsetree.hasSubLinks && !rule_action.hasSubLinks {
            for tle in rule_action.returningList.iter() {
                if let Some(e) = tle.expr.as_deref() {
                    if checkExprHasSubLink(&Node::mk_expr(mcx, e.clone_in(mcx)?)?) {
                        rule_action.hasSubLinks = true;
                        break;
                    }
                }
            }
        }
    }

    Ok(rule_action)
}

/// `contain_vars_of_level(node, levelsup)` — read-only probe (optimizer/util/var.c).
fn contain_vars_of_level(node: &Node<'_>, levelsup: i32) -> bool {
    vars::var::contain_vars_of_level(node, levelsup)
}

/// `acquireLocksOnSubLinks(node, ...)` over a single `Node` (the rule-qual /
/// inverted-qual entry points) — finds `SubLink`s and `AcquireRewriteLocks`
/// their (`'static`) sub-selects in place. `for_execute` is `true` at these
/// call sites (C: `rewriteRuleAction`/`CopyAndAddInvertedQual` pass a context
/// with `for_execute = true`). A no-op when the node has no sub-links.
fn acquire_locks_on_sublinks_node<'mcx>(mcx: Mcx<'mcx>, node: &mut Node<'mcx>) -> PgResult<()> {
    if !checkExprHasSubLink(node) {
        return Ok(());
    }
    let mut err: Option<::types_error::PgError> = None;
    acquireLocksOnSubLinks(mcx, node, true, &mut err);
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ===========================================================================
// fireRules + CopyAndAddInvertedQual (rewriteHandler.c:2321, 2392)
// ===========================================================================

/// `CopyAndAddInvertedQual(parsetree, rule_qual, rt_index, event)`
/// (rewriteHandler.c:2321).
fn CopyAndAddInvertedQual<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
    rule_qual: &Node<'mcx>,
    rt_index: i32,
    event: CmdType,
) -> PgResult<Query<'mcx>> {
    // Don't scribble on the passed qual.
    let mut new_qual: Node<'mcx> = rule_qual.clone_in(mcx)?;
    acquire_locks_on_sublinks_node(mcx, &mut new_qual)?;

    // Fix references to OLD.
    ChangeVarNodes(&mut new_qual, PRS2_OLD_VARNO, rt_index, 0, mcx);
    // Fix references to NEW.
    if event == CmdType::CMD_INSERT || event == CmdType::CMD_UPDATE {
        let rte = parsetree.rtable[(rt_index - 1) as usize].clone_in(mcx)?;
        let tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = parsetree
            .targetList
            .iter()
            .map(|t| t.clone_in(mcx))
            .collect::<PgResult<_>>()?;
        let result_relation = parsetree.resultRelation;
        let nomatch = if event == CmdType::CMD_UPDATE {
            ReplaceVarsNoMatchOption::ChangeVarno
        } else {
            ReplaceVarsNoMatchOption::SubstituteNull
        };
        let mut outer = Some(parsetree.hasSubLinks);
        ReplaceVarsFromTargetList(
            &mut new_qual,
            PRS2_NEW_VARNO,
            0,
            &rte,
            &tlist,
            result_relation,
            nomatch,
            rt_index,
            &mut outer,
            mcx,
        )?;
        if let Some(f) = outer {
            parsetree.hasSubLinks = f;
        }
    }

    AddInvertedQual(&mut parsetree, Some(&new_qual), mcx)?;
    Ok(parsetree)
}

/// `fireRules(parsetree, rt_index, event, locks, ...)` (rewriteHandler.c:2392).
#[allow(clippy::too_many_arguments)]
pub fn fireRules<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rt_index: i32,
    event: CmdType,
    locks: Vec<RewriteRuleImage<'mcx>>,
    instead_flag: &mut bool,
    returning_flag: &mut bool,
    qual_product: &mut Option<Query<'mcx>>,
) -> PgResult<Vec<Query<'mcx>>> {
    let mut results: Vec<Query<'mcx>> = Vec::new();

    for rule_lock in locks.into_iter() {
        let event_qual = rule_lock.qual;
        let actions = rule_lock.actions;

        // Determine QuerySource for the actions.
        let qsrc = if rule_lock.isInstead {
            if event_qual.is_some() {
                QuerySource::QSRC_QUAL_INSTEAD_RULE
            } else {
                *instead_flag = true;
                QuerySource::QSRC_INSTEAD_RULE
            }
        } else {
            QuerySource::QSRC_NON_INSTEAD_RULE
        };

        if qsrc == QuerySource::QSRC_QUAL_INSTEAD_RULE && !*instead_flag {
            if qual_product.is_none() {
                *qual_product = Some(core_clone(parsetree, mcx)?);
            }
            let qp = qual_product.take().unwrap();
            *qual_product = Some(CopyAndAddInvertedQual(
                mcx,
                qp,
                event_qual.as_deref().unwrap(),
                rt_index,
                event,
            )?);
        }

        // Process the rule's actions.
        for rule_action in actions.into_iter() {
            if rule_action.commandType == CmdType::CMD_NOTHING {
                continue;
            }
            let qual_clone = match &event_qual {
                Some(q) => Some(alloc_in(mcx, (**q).clone_in(mcx)?)?),
                None => None,
            };
            let mut ra = rewriteRuleAction(
                mcx,
                parsetree,
                rule_action,
                qual_clone,
                rt_index,
                event,
                returning_flag,
            )?;
            ra.querySource = qsrc;
            ra.canSetTag = false;
            results.push(ra);
        }
    }

    Ok(results)
}

// ===========================================================================
// ApplyRetrieveRule / markQueryForLocking / fireRIRrules
// ===========================================================================

/// `ApplyRetrieveRule(parsetree, rule, rt_index, relation, activeRIRs)`
/// (rewriteHandler.c:1712) — expand an `ON SELECT` (view) rule, converting the
/// view's relation RTE into a subquery RTE holding the (recursively-expanded)
/// view query.
fn ApplyRetrieveRule<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
    rule: &RewriteRuleImage<'mcx>,
    rt_index: i32,
    relation: &Relation<'mcx>,
    active_rirs: &mut Vec<Oid>,
) -> PgResult<Query<'mcx>> {
    if rule.actions.len() != 1 {
        return Err(elog("expected just one rule action"));
    }
    if rule.qual.is_some() {
        return Err(elog("cannot handle qualified ON SELECT rule"));
    }

    // Restricted expansion of non-system views.
    let restrict = guc_tables_seams::restrict_nonsystem_relation_kind::call();
    if (restrict & RESTRICT_RELKIND_VIEW) != 0 && relation.rd_id >= FirstNormalObjectId {
        return Err(PgError::new(
            ERROR,
            format!("access to non-system view \"{}\" is restricted", relation.name()),
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    if rt_index == parsetree.resultRelation {
        // View as the result relation (INSTEAD OF trigger target).
        match parsetree.commandType {
            CmdType::CMD_INSERT => return Ok(parsetree),
            CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE => {
                // For UPDATE/DELETE/MERGE, we need to expand the view so as to
                // have source data for the operation. But we also need an
                // unmodified RTE to serve as the target. So, copy the RTE and
                // add the copy to the rangetable. Note that the copy does not
                // get added to the jointree. Also note that there's a hack in
                // fireRIRrules to avoid calling this function again when it
                // arrives at the copied RTE.
                let newrte = parsetree.rtable[(rt_index - 1) as usize].clone_in(mcx)?;
                parsetree.rtable.push(newrte);
                parsetree.resultRelation = parsetree.rtable.len() as i32;
                // parsetree->mergeTargetRelation unchanged (use expanded view)

                // For the most part, Vars referencing the view should remain as
                // they are, meaning that they implicitly represent OLD values.
                // But in the RETURNING list if any, we want such Vars to
                // represent NEW values, so change them to reference the new RTE.
                //
                // Since ChangeVarNodes scribbles on the tree in-place, copy the
                // RETURNING list first for safety.
                let new_result_relation = parsetree.resultRelation;
                let mut new_returning: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
                    PgVec::new_in(mcx);
                for tle in parsetree.returningList.iter() {
                    let mut node = Node::mk_target_entry(mcx, tle.clone_in(mcx)?)?;
                    ChangeVarNodes(&mut node, rt_index, new_result_relation, 0, mcx);
                    new_returning.push(node.into_targetentry().unwrap_or_else(|| {
                        unreachable!("ChangeVarNodes preserves the node kind")
                    }));
                }
                parsetree.returningList = new_returning;

                // To allow the executor to compute the original view row to pass
                // to the INSTEAD OF trigger, we add a resjunk whole-row Var
                // referencing the original RTE. This will later get expanded
                // into a RowExpr computing all the OLD values of the view row.
                let rte = &parsetree.rtable[(rt_index - 1) as usize];
                let var = ::nodes_core::makefuncs::make_whole_row_var(
                    rte, rt_index, 0, false,
                )?;
                let resno = (parsetree.targetList.len() + 1) as i16;
                let tle = make_target_entry(
                    mcx,
                    Expr::Var(var),
                    resno,
                    Some("wholerow"),
                    true,
                )?;
                parsetree.targetList.push(tle);

                // Now, continue with expanding the original view RTE.
            }
            other => return Err(elog(format!("unrecognized commandType: {}", other as i32))),
        }
    }

    // FOR [KEY] UPDATE/SHARE applying to this view?
    let rc = ::parser_relation::get_parse_rowmark(&parsetree, rt_index as Index).cloned();

    // Make a modifiable copy of the view query and acquire locks.
    let mut rule_action = core_clone(&rule.actions[0], mcx)?;
    AcquireRewriteLocks(mcx, &mut rule_action, true, rc.is_some())?;

    // If FOR [KEY] UPDATE/SHARE of view, mark all the contained tables as
    // implicit FOR [KEY] UPDATE/SHARE, the same as the parser would have done
    // if the view's subquery had been written out explicitly.
    if let Some(rc) = rc.as_ref() {
        // markQueryForLocking(rule_action, rule_action->jointree, rc->strength,
        //                     rc->waitPolicy, true).  The jointree is detached so
        // the walk can read it while the same query's rtable/rteperminfos/rowMarks
        // are mutated.
        let jointree = rule_action.jointree.take();
        let result = (|| -> PgResult<()> {
            if let Some(jt) = jointree.as_deref() {
                for item in jt.fromlist.iter() {
                    markQueryForLocking(
                        mcx,
                        &mut rule_action,
                        item,
                        rc.strength,
                        rc.waitPolicy,
                        true,
                    )?;
                }
            }
            Ok(())
        })();
        rule_action.jointree = jointree;
        result?;
    }

    // Recursively expand view references inside the view.
    rule_action = fireRIRrules(mcx, rule_action, active_rirs)?;

    parsetree.hasRowSecurity |= rule_action.hasRowSecurity;

    // Plug the view query in as a subselect.
    // C: rte->security_barrier = RelationIsSecurityView(relation); the macro
    // asserts relkind == RELKIND_VIEW. ApplyRetrieveRule is only reached for view
    // ON SELECT rules, so `relation` is always a view here, matching the C path.
    let security_barrier = rewrite_relation_is_security_view(relation);
    let num_cols =
        execUtils::ExecCleanTargetListLength(&rule_action.targetList);

    let rte = &mut parsetree.rtable[(rt_index - 1) as usize];
    rte.rtekind = RTEKind::RTE_SUBQUERY;
    rte.subquery = Some(alloc_in(mcx, rule_action)?);
    rte.security_barrier = security_barrier;
    rte.tablesample = None;
    rte.inh = false;

    // Pad eref->colnames up to num_cols with "?column?".
    if let Some(eref) = rte.eref.as_deref_mut() {
        while (eref.colnames.len() as i32) < num_cols {
            eref.colnames
                .push(alloc_in(mcx, Node::mk_string(mcx, make_string(mcx, "?column?")?)?)?);
        }
    }

    Ok(parsetree)
}

/// `markQueryForLocking(qry, jtnode, strength, waitPolicy, pushedDown)`
/// (rewriteHandler.c:1893) — recursively mark all relations used by a view as
/// FOR [KEY] UPDATE/SHARE.
///
/// This may generate an invalid query, eg if some sub-query uses an aggregate.
/// We leave it to the planner to detect that.  NB: this must agree with the
/// parser's `transformLockingClause()`.
///
/// `jtnode` is borrowed from a jointree the caller has detached from `qry`, so
/// that `applyLockingClause`/`getRTEPermissionInfo` may mutate `qry`'s
/// rtable/rteperminfos/rowMarks while this walk reads the (immutable) jointree.
fn markQueryForLocking<'mcx>(
    mcx: Mcx<'mcx>,
    qry: &mut Query<'mcx>,
    jtnode: &NodePtr<'mcx>,
    strength: LockClauseStrength,
    wait_policy: LockWaitPolicy,
    pushed_down: bool,
) -> PgResult<()> {
    if let Some(rtr) = jtnode.as_rangetblref() {
        let rti = rtr.rtindex as Index;
        let rtekind = qry.rtable[(rti - 1) as usize].rtekind;

        if rtekind == RTEKind::RTE_RELATION {
            applyLockingClause(mcx, qry, rti, strength, wait_policy, pushed_down)?;

            let perminfo_idx =
                getRTEPermissionInfo(&qry.rteperminfos, &qry.rtable[(rti - 1) as usize])?;
            qry.rteperminfos[perminfo_idx].requiredPerms |= ACL_SELECT_FOR_UPDATE;
        } else if rtekind == RTEKind::RTE_SUBQUERY {
            applyLockingClause(mcx, qry, rti, strength, wait_policy, pushed_down)?;

            // FOR UPDATE/SHARE of subquery is propagated to subquery's rels.
            // Detach the subquery and its jointree so the recursive walk can
            // mutate the subquery while reading its jointree.
            let mut subquery = match qry.rtable[(rti - 1) as usize].subquery.take() {
                Some(sub) => PgBox::into_inner(sub),
                None => return Ok(()),
            };
            let result = (|| -> PgResult<()> {
                let subjointree = subquery.jointree.take();
                let inner = (|| -> PgResult<()> {
                    if let Some(jt) = subjointree.as_deref() {
                        for item in jt.fromlist.iter() {
                            markQueryForLocking(
                                mcx,
                                &mut subquery,
                                item,
                                strength,
                                wait_policy,
                                true,
                            )?;
                        }
                    }
                    Ok(())
                })();
                subquery.jointree = subjointree;
                inner
            })();
            qry.rtable[(rti - 1) as usize].subquery = Some(alloc_in(mcx, subquery)?);
            result?;
        }
        // other RTE types are unaffected by FOR UPDATE
    } else if let Some(f) = jtnode.as_fromexpr() {
        // The fromlist must be detached from qry to read it while mutating qry;
        // FromExpr nodes only occur nested inside an already-detached jointree,
        // so the borrow here aliases the detached tree, not qry.
        for l in f.fromlist.iter() {
            markQueryForLocking(mcx, qry, l, strength, wait_policy, pushed_down)?;
        }
    } else if let Some(j) = jtnode.as_joinexpr() {
        if let Some(larg) = j.larg.as_ref() {
            markQueryForLocking(mcx, qry, larg, strength, wait_policy, pushed_down)?;
        }
        if let Some(rarg) = j.rarg.as_ref() {
            markQueryForLocking(mcx, qry, rarg, strength, wait_policy, pushed_down)?;
        }
    } else {
        return Err(elog(format!(
            "unrecognized node type: {}",
            jtnode.node_tag().0
        )));
    }
    Ok(())
}

/// `fireRIRrules(parsetree, activeRIRs)` (rewriteHandler.c:1992) — apply all RIR
/// (`ON SELECT`/view) rules on every range-table entry, recursing into
/// subqueries and CTEs.
pub fn fireRIRrules<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
    active_rirs: &mut Vec<Oid>,
) -> PgResult<Query<'mcx>> {
    let orig_result_relation = parsetree.resultRelation;

    // Expand SEARCH and CYCLE clauses in CTEs.
    //
    // This is just a convenient place to do this, since we are already looking
    // at each Query. (rewriteHandler.c:1999)
    for i in 0..parsetree.cteList.len() {
        if cte_has_search_or_cycle(&parsetree.cteList[i]) {
            // cte = rewriteSearchAndCycle(cte); lfirst(lc) = cte;
            let old = core::mem::replace(
                &mut parsetree.cteList[i],
                // placeholder; overwritten just below
                alloc_in(mcx, Node::mk_string(mcx, make_string(mcx, "")?)?)?,
            );
            let cte = PgBox::into_inner(old).into_commontableexpr().ok_or_else(|| {
                elog("fireRIRrules: cteList entry is not a CommonTableExpr")
            })?;
            let rewritten =
                rewriteSearchCycle::rewriteSearchAndCycle(mcx, cte)?;
            parsetree.cteList[i] =
                alloc_in(mcx, Node::mk_common_table_expr(mcx, rewritten)?)?;
        }
    }

    // Process each RTE (rtable can grow as we go).
    let mut rt_index = 0usize;
    while rt_index < parsetree.rtable.len() {
        rt_index += 1;
        let rtekind = parsetree.rtable[rt_index - 1].rtekind;

        if rtekind == RTEKind::RTE_SUBQUERY {
            let sub = parsetree.rtable[rt_index - 1].subquery.take().unwrap();
            let rewritten = fireRIRrules(mcx, PgBox::into_inner(sub), active_rirs)?;
            parsetree.hasRowSecurity |= rewritten.hasRowSecurity;
            parsetree.rtable[rt_index - 1].subquery = Some(alloc_in(mcx, rewritten)?);
            continue;
        }
        if rtekind != RTEKind::RTE_RELATION {
            continue;
        }
        let relkind = parsetree.rtable[rt_index - 1].relkind;
        if relkind == RELKIND_MATVIEW {
            continue;
        }

        // ON CONFLICT EXCLUDED pseudo-relation.
        if let Some(oc) = parsetree.onConflict.as_deref() {
            if rt_index as i32 == oc.exclRelIndex {
                continue;
            }
        }

        // Skip unreferenced relations (avoids infinite expansion).
        if rt_index as i32 != parsetree.resultRelation
            && !query_range_table_entry_used(&parsetree, rt_index as i32)
        {
            continue;
        }
        // A new result relation introduced by ApplyRetrieveRule.
        if rt_index as i32 == parsetree.resultRelation
            && rt_index as i32 != orig_result_relation
        {
            continue;
        }

        let relid = parsetree.rtable[rt_index - 1].relid;
        let rel = table_open(mcx, relid, NoLock)?;

        // Collect ON SELECT rules.
        let rules = relation_rules::call(mcx, rel.rd_id)?;
        let select_rules: Vec<RewriteRuleImage<'mcx>> = match rules {
            Some(rl) => rl
                .rules
                .into_iter()
                .filter(|r| r.event == CmdType::CMD_SELECT)
                .collect(),
            None => Vec::new(),
        };

        if !select_rules.is_empty() {
            let relid = rel.rd_id;
            if active_rirs.contains(&relid) {
                let name = rel.name().to_string();
                rel.close(NoLock)?;
                return Err(PgError::new(
                    ERROR,
                    format!("infinite recursion detected in rules for relation \"{name}\""),
                )
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
            }
            active_rirs.push(relid);
            for rule in select_rules.iter() {
                parsetree =
                    ApplyRetrieveRule(mcx, parsetree, rule, rt_index as i32, &rel, active_rirs)?;
            }
            active_rirs.pop();
        }

        rel.close(NoLock)?;
    }

    // Recurse into CTE subqueries.
    for i in 0..parsetree.cteList.len() {
        let mut ctequery = take_cte_query(&mut parsetree.cteList[i]);
        if let Some(q) = ctequery.take() {
            let rewritten = fireRIRrules(mcx, q, active_rirs)?;
            parsetree.hasRowSecurity |= rewritten.hasRowSecurity;
            set_cte_query(mcx, &mut parsetree.cteList[i], rewritten)?;
        }
    }

    // Recurse into sublink subqueries, too.  But we already did the ones in
    // the rtable and cteList (QTW_IGNORE_RC_SUBQUERIES).
    if parsetree.hasSubLinks {
        let mut sublink_row_security = false;
        let mut err: Option<::types_error::PgError> = None;
        {
            let mut walker = |node: &mut Node<'mcx>| {
                fireRIRonSubLink(
                    mcx,
                    node,
                    active_rirs,
                    &mut sublink_row_security,
                    &mut err,
                )
            };
            ::nodes_core::node_walker::query_tree_mutator(
                &mut parsetree,
                &mut walker,
                ::nodes_core::node_walker::QTW_IGNORE_RT_SUBQUERIES
                    | ::nodes_core::node_walker::QTW_IGNORE_CTE_SUBQUERIES,
                mcx,
            );
        }
        if let Some(e) = err {
            return Err(e);
        }
        // Make sure the query is marked as having row security if any of its
        // sublinks do.
        parsetree.hasRowSecurity |= sublink_row_security;
    }

    // Apply any row-level security policies.  We do this last because it
    // requires special recursion detection if the new quals have sublink
    // subqueries, and if we did it in the loop above query_tree_walker would
    // then recurse into those quals a second time.
    let mut rt_index = 0usize;
    while rt_index < parsetree.rtable.len() {
        rt_index += 1;
        let rtekind = parsetree.rtable[rt_index - 1].rtekind;
        let relkind = parsetree.rtable[rt_index - 1].relkind;

        // Only normal relations can have RLS policies.
        if rtekind != RTEKind::RTE_RELATION
            || (relkind != RELKIND_PLAIN_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
        {
            continue;
        }

        let relid = parsetree.rtable[rt_index - 1].relid;
        let rel = table_open(mcx, relid, NoLock)?;

        // Fetch any new security quals that must be applied to this RTE. The C
        // reads several Query-level fields off `parsetree` inside
        // get_row_security_policies; pass them explicitly.
        let returning_present = !parsetree.returningList.is_empty();
        let on_conflict_update = parsetree
            .onConflict
            .as_deref()
            .is_some_and(|oc| oc.action == OnConflictAction::ONCONFLICT_UPDATE);
        let rls = {
            let rte = &parsetree.rtable[rt_index - 1];
            rowsecurity::get_row_security_policies(
                mcx,
                rte,
                rt_index as i32,
                parsetree.resultRelation,
                parsetree.commandType,
                returning_present,
                on_conflict_update,
                &parsetree.rteperminfos,
            )?
        };

        let rowsecurity::RlsPolicies {
            mut security_quals,
            mut with_check_options,
            has_row_security,
            has_sub_links,
        } = rls;

        if !security_quals.is_empty() || !with_check_options.is_empty() {
            if has_sub_links {
                // Recursively process the new quals, checking for infinite
                // recursion.
                if active_rirs.contains(&rel.rd_id) {
                    let name = rel.name().to_string();
                    rel.close(NoLock)?;
                    return Err(PgError::new(
                        ERROR,
                        format!("infinite recursion detected in policy for relation \"{name}\""),
                    )
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
                }

                active_rirs.push(rel.rd_id);

                // get_row_security_policies just passed back securityQuals
                // and/or withCheckOptions, and there were SubLinks, so lock any
                // relations they reference (normally acquired by the parser, but
                // these are added post-parsing).
                {
                    let mut err: Option<::types_error::PgError> = None;
                    for q in security_quals.iter_mut() {
                        acquireLocksOnSubLinks(mcx, q, true, &mut err);
                    }
                    for w in with_check_options.iter_mut() {
                        acquireLocksOnSubLinks(mcx, w, true, &mut err);
                    }
                    if let Some(e) = err {
                        active_rirs.pop();
                        rel.close(NoLock)?;
                        return Err(e);
                    }
                }

                // Now fire any RIR rules for them. We can ignore the resulting
                // hasRowSecurity since we only reach here when it is already set.
                {
                    let mut sublink_row_security = false;
                    let mut err: Option<::types_error::PgError> = None;
                    for q in security_quals.iter_mut() {
                        fireRIRonSubLink(
                            mcx,
                            q,
                            active_rirs,
                            &mut sublink_row_security,
                            &mut err,
                        );
                    }
                    for w in with_check_options.iter_mut() {
                        fireRIRonSubLink(
                            mcx,
                            w,
                            active_rirs,
                            &mut sublink_row_security,
                            &mut err,
                        );
                    }
                    if let Some(e) = err {
                        active_rirs.pop();
                        rel.close(NoLock)?;
                        return Err(e);
                    }
                }

                active_rirs.pop();
            }

            // Add the new security barrier quals to the START of the RTE's list
            // so they get applied before any existing barrier quals (which would
            // have come from a security-barrier view, and should get lower
            // priority than RLS conditions on the table itself).
            // rte->securityQuals = list_concat(securityQuals, rte->securityQuals)
            {
                let rte = &mut parsetree.rtable[rt_index - 1];
                let existing: Vec<NodePtr<'mcx>> = rte.securityQuals.drain(..).collect();
                for n in existing {
                    security_quals.push(n);
                }
                rte.securityQuals = security_quals;
            }

            // parsetree->withCheckOptions =
            //     list_concat(withCheckOptions, parsetree->withCheckOptions)
            {
                let existing: Vec<NodePtr<'mcx>> =
                    parsetree.withCheckOptions.drain(..).collect();
                for n in existing {
                    with_check_options.push(n);
                }
                parsetree.withCheckOptions = with_check_options;
            }
        }

        // Mark the query correctly if RLS applies, or if the new quals had
        // sublinks.
        if has_row_security {
            parsetree.hasRowSecurity = true;
        }
        if has_sub_links {
            parsetree.hasSubLinks = true;
        }

        rel.close(NoLock)?;
    }

    Ok(parsetree)
}

/// `fireRIRonSubLink(node, context)` (rewriteHandler.c:1957) — apply
/// [`fireRIRrules`] to each `SubLink` (subselect in expression) found in the
/// given tree. Although this has the form of a walker, we cheat and modify the
/// `SubLink` nodes in-place. We must take control at the `SubLink` node in order
/// to replace its `subselect` link with the possibly-rewritten subquery. We do
/// NOT recurse into `Query` nodes, because [`fireRIRrules`] already processed
/// subselects of subselects (the `expression_tree_walker_mut` `Query` arm
/// returns false). The C walker's `bool` return becomes the early-abort signal
/// carried alongside an `err` out-param (the `bool` shape can't return
/// `PgResult`).
fn fireRIRonSubLink<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    active_rirs: &mut Vec<Oid>,
    has_row_security: &mut bool,
    err: &mut Option<::types_error::PgError>,
) -> bool {
    if err.is_some() {
        return true;
    }
    if let Some(Expr::SubLink(sub)) = node.as_expr_mut() {
        // `SubLink::subselect` now threads `'mcx` (the Expr-'mcx flip): take the
        // already-arena-owned subselect, rewrite, and re-embed — no `'static`
        // relabel transmute / re-intern needed.
        if let Some(sub_box) = sub.subselect.take() {
            let subquery: Query<'mcx> = PgBox::into_inner(sub_box);
            match fireRIRrules(mcx, subquery, active_rirs) {
                Ok(rewritten) => {
                    *has_row_security |= rewritten.hasRowSecurity;
                    match ::mcx::alloc_in(mcx, rewritten) {
                        Ok(boxed) => sub.subselect = Some(boxed),
                        Err(e) => {
                            *err = Some(e);
                            return true;
                        }
                    }
                }
                Err(e) => {
                    *err = Some(e);
                    return true;
                }
            }
        }
        // Fall through to process lefthand args of SubLink (testexpr).
    }

    // Do NOT recurse into Query nodes; expression_tree_walker_mut's Query arm
    // returns false, matching the C QTW behavior.
    ::nodes_core::node_walker::expression_tree_walker_mut(
        node,
        &mut |n| fireRIRonSubLink(mcx, n, active_rirs, has_row_security, err),
        mcx,
    )
}

// ===========================================================================
// rewriteTargetView (rewriteHandler.c:3216)
// ===========================================================================

/// `rewriteTargetView(parsetree, view)` (rewriteHandler.c:3216) — rewrite a DML
/// query whose target relation is an auto-updatable view so the view's base
/// relation becomes the target relation. The view's `ON SELECT` `Query` is
/// pulled up: its single base RTE is appended to the outer query's range table,
/// the view targetlist Vars are re-pointed at it, every reference to the view
/// (targetlist resnos, quals, RTI references, `resultRelation`) is rewritten to
/// the base relation, and any view WHERE quals / WITH CHECK OPTIONs are carried
/// over. The (possibly views-on-views) result is handled by the caller's
/// recursion through `RewriteQuery`.
fn rewriteTargetView<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
    view: &Relation<'mcx>,
) -> PgResult<Query<'mcx>> {
    use ::nodes_core::bitmapset::{bms_is_empty, bms_union};
    use parser_relation::{addRTEPermissionInfo, getRTEPermissionInfo};
    use ::types_storage::lock::RowExclusiveLock;

    // Get the Query from the view's ON SELECT rule. get_view_query already
    // returns a fresh copyObject re-projected into mcx, so it is ours to munge.
    let mut viewquery = crate::get_view_query(mcx, view)?;

    // Locate the RTE and perminfo describing the view in the outer query.
    let view_result_relation = parsetree.resultRelation;
    let view_rte_idx = (view_result_relation - 1) as usize;
    let view_perminfo_idx = getRTEPermissionInfo(&parsetree.rteperminfos, &parsetree.rtable[view_rte_idx])?;

    // Are we doing INSERT/UPDATE, or MERGE containing INSERT/UPDATE?
    let mut insert_or_update = parsetree.commandType == CmdType::CMD_INSERT
        || parsetree.commandType == CmdType::CMD_UPDATE;
    if parsetree.commandType == CmdType::CMD_MERGE {
        for node in parsetree.mergeActionList.iter() {
            if let Some(action) = (**node).as_mergeaction() {
                if action.commandType == CmdType::CMD_INSERT
                    || action.commandType == CmdType::CMD_UPDATE
                {
                    insert_or_update = true;
                    break;
                }
            }
        }
    }

    // Check if the expansion of non-system views are restricted.
    let restrict = guc_tables_seams::restrict_nonsystem_relation_kind::call();
    if (restrict & RESTRICT_RELKIND_VIEW) != 0 && view.rd_id >= FirstNormalObjectId {
        return Err(PgError::new(
            ERROR,
            format!("access to non-system view \"{}\" is restricted", view.name()),
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // The view must be updatable, else fail. If INSERT/UPDATE (or MERGE
    // containing INSERT/UPDATE), also require at least one updatable column.
    let auto_update_detail = crate::view_query_is_auto_updatable(&viewquery, insert_or_update)?;
    if let Some(detail) = auto_update_detail {
        return Err(crate::error_view_not_updatable(
            view,
            parsetree.commandType,
            &parsetree.mergeActionList,
            Some(detail),
        ));
    }

    // For INSERT/UPDATE (or MERGE containing INSERT/UPDATE) the modified columns
    // must all be updatable.
    if insert_or_update {
        let mut modified_cols = bms_union(
            mcx,
            parsetree.rteperminfos[view_perminfo_idx].insertedCols.as_deref(),
            parsetree.rteperminfos[view_perminfo_idx].updatedCols.as_deref(),
        )?;

        for tle in parsetree.targetList.iter() {
            if !tle.resjunk {
                let prev = modified_cols.take();
                modified_cols = Some(::nodes_core::bitmapset::bms_add_member(
                    mcx,
                    prev,
                    tle.resno as i32 - crate::FirstLowInvalidHeapAttributeNumber,
                )?);
            }
        }

        if let Some(oc) = parsetree.onConflict.as_deref() {
            for tle_node in oc.onConflictSet.iter() {
                if let Some(tle) = (**tle_node).as_targetentry() {
                    if !tle.resjunk {
                        let prev = modified_cols.take();
                        modified_cols = Some(::nodes_core::bitmapset::bms_add_member(
                            mcx,
                            prev,
                            tle.resno as i32 - crate::FirstLowInvalidHeapAttributeNumber,
                        )?);
                    }
                }
            }
        }

        for node in parsetree.mergeActionList.iter() {
            if let Some(action) = (**node).as_mergeaction() {
                if action.commandType == CmdType::CMD_INSERT
                    || action.commandType == CmdType::CMD_UPDATE
                {
                    for tle_node in action.targetList.iter() {
                        if let Some(tle) = (**tle_node).as_targetentry() {
                            if !tle.resjunk {
                                let prev = modified_cols.take();
                                modified_cols = Some(::nodes_core::bitmapset::bms_add_member(
                                    mcx,
                                    prev,
                                    tle.resno as i32 - crate::FirstLowInvalidHeapAttributeNumber,
                                )?);
                            }
                        }
                    }
                }
            }
        }

        let mut non_updatable_col: Option<String> = None;
        let detail = crate::view_cols_are_auto_updatable(
            mcx,
            &viewquery,
            modified_cols.as_deref(),
            None,
            &mut non_updatable_col,
        )?;
        if let Some(detail) = detail {
            let col = non_updatable_col.unwrap_or_default();
            let view_name = view.name();
            let (msg, code) = match parsetree.commandType {
                CmdType::CMD_INSERT => (
                    format!("cannot insert into column \"{col}\" of view \"{view_name}\""),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ),
                CmdType::CMD_UPDATE => (
                    format!("cannot update column \"{col}\" of view \"{view_name}\""),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ),
                CmdType::CMD_MERGE => (
                    format!("cannot merge into column \"{col}\" of view \"{view_name}\""),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ),
                other => {
                    return Err(elog(&format!("unrecognized CmdType: {}", other as i32)));
                }
            };
            return Err(PgError::new(ERROR, msg)
                .with_sqlstate(code)
                .with_detail(detail.to_string()));
        }
    }

    // For MERGE, guard against a partial set of INSTEAD OF triggers.
    if parsetree.commandType == CmdType::CMD_MERGE {
        for node in parsetree.mergeActionList.iter() {
            if let Some(action) = (**node).as_mergeaction() {
                if action.commandType != CmdType::CMD_NOTHING
                    && view_has_instead_trigger(view, action.commandType, &[])?
                {
                    return Err(PgError::new(
                        ERROR,
                        format!("cannot merge into view \"{}\"", view.name()),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_detail(
                        "MERGE is not supported for views with INSTEAD OF triggers for some actions but not all."
                            .to_string(),
                    )
                    .with_hint(
                        "To enable merging into the view, either provide a full set of INSTEAD OF triggers or drop the existing INSTEAD OF triggers."
                            .to_string(),
                    ));
                }
            }
        }
    }

    // view_query_is_auto_updatable verified a single base relation.
    let jt = viewquery
        .jointree
        .as_ref()
        .expect("auto-updatable view has a jointree");
    debug_assert_eq!(jt.fromlist.len(), 1);
    let rtr = (*jt.fromlist[0])
        .as_rangetblref()
        .unwrap_or_else(|| panic!("auto-updatable view fromlist[0] is not a RangeTblRef"));
    let base_rt_index = rtr.rtindex;
    let base_perminfo_idx = {
        let base_rte = &viewquery.rtable[(base_rt_index - 1) as usize];
        debug_assert_eq!(base_rte.rtekind, RTEKind::RTE_RELATION);
        getRTEPermissionInfo(&viewquery.rteperminfos, base_rte)?
    };
    let base_relid = viewquery.rtable[(base_rt_index - 1) as usize].relid;

    // Acquire RowExclusiveLock on the base relation (it will become the target).
    let base_rel = table_open(mcx, base_relid, RowExclusiveLock)?;

    // Refresh the base RTE's relkind in case it changed since the view was made.
    viewquery.rtable[(base_rt_index - 1) as usize].relkind = base_rel.rd_rel.relkind as i8;

    // If the view query contains sublink subqueries, lock relations they refer
    // to (C: acquireLocksOnSubLinks with for_execute = true,
    // QTW_IGNORE_RC_SUBQUERIES). A no-op otherwise.
    if viewquery.hasSubLinks {
        let mut err: Option<::types_error::PgError> = None;
        {
            let mut walker =
                |node: &mut Node<'mcx>| acquireLocksOnSubLinks(mcx, node, true, &mut err);
            ::nodes_core::node_walker::query_tree_mutator(
                &mut viewquery,
                &mut walker,
                ::nodes_core::node_walker::QTW_IGNORE_RT_SUBQUERIES
                    | ::nodes_core::node_walker::QTW_IGNORE_CTE_SUBQUERIES,
                mcx,
            );
        }
        if let Some(e) = err {
            base_rel.close(NoLock)?;
            return Err(e);
        }
    }

    // Create the new target RTE describing the base relation (scribble on the
    // copied base_rte), append it to the outer query's range table.
    let new_rt_index;
    {
        let mut new_rte = viewquery.rtable[(base_rt_index - 1) as usize].clone_in(mcx)?;
        new_rte.rellockmode = RowExclusiveLock;
        // INSERTs never inherit; UPDATE/DELETE/MERGE use the view's inh flag.
        if parsetree.commandType == CmdType::CMD_INSERT {
            new_rte.inh = false;
        }
        new_rte.securityQuals = PgVec::new_in(mcx);
        new_rte.perminfoindex = 0;
        parsetree.rtable.push(new_rte);
        new_rt_index = parsetree.rtable.len() as i32;
    }

    // Adjust the view targetlist Vars to reference the new target RTE, making
    // their varnos new_rt_index instead of base_rt_index. We keep our own owned
    // copy of the (re-pointed) view targetlist for the replacements below.
    let mut view_targetlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = viewquery
        .targetList
        .iter()
        .map(|t| t.clone_in(mcx))
        .collect::<PgResult<_>>()?;
    for tle in view_targetlist.iter_mut() {
        let mut node = Node::mk_target_entry(mcx, tle.clone_in(mcx)?)?;
        ChangeVarNodes(&mut node, base_rt_index, new_rt_index, 0, mcx);
        *tle = node.into_targetentry().unwrap_or_else(|| unreachable!());
    }

    // Per-relation permission bits on the new RTE. Mark the new target with the
    // INSERT/UPDATE/DELETE perms the caller needs against the view, dropping the
    // ACL_SELECT bit. checkAsUser depends on the view's security_invoker flag.
    let new_perminfo_idx = {
        let new_rte = parsetree.rtable.last_mut().unwrap();
        addRTEPermissionInfo(&mut parsetree.rteperminfos, new_rte)?
    };
    {
        let check_as_user = if rewrite_relation_has_security_invoker(view) {
            InvalidOid
        } else {
            view.rd_rel.relowner
        };
        let required_perms = parsetree.rteperminfos[view_perminfo_idx].requiredPerms;
        // Per-column perms: keep the base view's selectedCols; set inserted/
        // updatedCols from the outer query's modified columns, mapped through
        // the (re-pointed) view targetlist.
        let base_selected = match viewquery.rteperminfos[base_perminfo_idx]
            .selectedCols
            .as_ref()
        {
            Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        };
        let view_inserted = parsetree.rteperminfos[view_perminfo_idx].insertedCols.as_deref();
        let new_inserted = crate::adjust_view_column_set(mcx, view_inserted, &view_targetlist)?;
        let view_updated = parsetree.rteperminfos[view_perminfo_idx].updatedCols.as_deref();
        let new_updated = crate::adjust_view_column_set(mcx, view_updated, &view_targetlist)?;

        let np = &mut parsetree.rteperminfos[new_perminfo_idx];
        debug_assert!(bms_is_empty(np.insertedCols.as_deref()) && bms_is_empty(np.updatedCols.as_deref()));
        np.checkAsUser = check_as_user;
        np.requiredPerms = required_perms;
        np.selectedCols = base_selected;
        np.insertedCols = new_inserted;
        np.updatedCols = new_updated;
    }

    // Move any security-barrier quals from the view RTE onto the new target RTE.
    {
        let view_secquals: Vec<NodePtr<'mcx>> =
            parsetree.rtable[view_rte_idx].securityQuals.drain(..).collect();
        let new_rte = parsetree.rtable.last_mut().unwrap();
        let mut sq = PgVec::new_in(mcx);
        sq.extend(view_secquals);
        new_rte.securityQuals = sq;
    }

    // Update all Vars in the outer query that reference the view to reference
    // the appropriate base-relation column instead.
    {
        let view_rte = parsetree.rtable[view_rte_idx].clone_in(mcx)?;
        let mut node = Node::mk_query(mcx, parsetree)?;
        let mut outer = None;
        ReplaceVarsFromTargetList(
            &mut node,
            view_result_relation,
            0,
            &view_rte,
            &view_targetlist,
            new_rt_index,
            ReplaceVarsNoMatchOption::ReportError,
            0,
            &mut outer,
            mcx,
        )?;
        parsetree = node.into_query().unwrap_or_else(|| unreachable!());
    }

    // Update all other RTI references that point to the view (e.g.
    // resultRelation) to point to the new base relation instead.
    {
        let mut node = Node::mk_query(mcx, parsetree)?;
        ChangeVarNodes(&mut node, view_result_relation, new_rt_index, 0, mcx);
        parsetree = node.into_query().unwrap_or_else(|| unreachable!());
    }
    debug_assert_eq!(parsetree.resultRelation, new_rt_index);

    // For INSERT/UPDATE we must also update resnos in the targetlist to refer to
    // columns of the base relation; same for MERGE INSERT/UPDATE action tlists.
    if parsetree.commandType != CmdType::CMD_DELETE {
        for tle in parsetree.targetList.iter_mut() {
            if tle.resjunk {
                continue;
            }
            tle.resno = view_tle_base_attno(&view_targetlist, tle.resno)?;
        }
        for node in parsetree.mergeActionList.iter_mut() {
            // MergeAction tlist resnos: re-point through the view tlist.
            let Some(action) = (**node).as_mergeaction_mut() else {
                continue;
            };
            if action.commandType == CmdType::CMD_INSERT
                || action.commandType == CmdType::CMD_UPDATE
            {
                for tle_node in action.targetList.iter_mut() {
                    if let Some(tle) = (**tle_node).as_targetentry_mut() {
                        if tle.resjunk {
                            continue;
                        }
                        tle.resno = view_tle_base_attno(&view_targetlist, tle.resno)?;
                    }
                }
            }
        }
    }

    // For INSERT .. ON CONFLICT .. DO UPDATE, we must also update assorted stuff
    // in the onConflict data structure. (rewriteHandler.c:3645)
    if parsetree
        .onConflict
        .as_deref()
        .is_some_and(|oc| oc.action == OnConflictAction::ONCONFLICT_UPDATE)
    {
        // Like the INSERT/UPDATE code above, update the resnos in the auxiliary
        // UPDATE targetlist to refer to columns of the base relation.
        // (rewriteHandler.c:3661)
        for tle_node in parsetree
            .onConflict
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!())
            .onConflictSet
            .iter_mut()
        {
            let Some(tle) = (**tle_node).as_targetentry_mut() else {
                continue;
            };
            if tle.resjunk {
                continue;
            }
            tle.resno = view_tle_base_attno(&view_targetlist, tle.resno)?;
        }

        // Also, create a new RTE for the EXCLUDED pseudo-relation, using the
        // query's new base rel (which may well have a different column list from
        // the view, hence a new column alias list). This matches
        // transformOnConflictClause. In particular, the relkind is set to
        // composite to signal that we're not dealing with an actual relation.
        // (rewriteHandler.c:3679)
        let old_excl_rel_index = parsetree
            .onConflict
            .as_deref()
            .unwrap_or_else(|| unreachable!())
            .exclRelIndex;

        let excl_alias =
            ::nodes_core::makefuncs::make_alias(mcx, "excluded", PgVec::new_in(mcx))?;
        let mut excl_pstate = small1::make_parsestate(mcx, None)?;
        let new_excl_nsitem = ::parser_relation::addRangeTableEntryForRelation(
            mcx,
            &mut excl_pstate,
            &base_rel,
            RowExclusiveLock,
            Some(excl_alias),
            false,
            false,
        )?;
        let mut new_excl_rte = new_excl_nsitem
            .p_rte
            .map(|b| (*b).clone_in(mcx))
            .transpose()?
            .ok_or_else(|| elog("rewriteTargetView: EXCLUDED nsitem has no RTE"))?;
        new_excl_rte.relkind = ::types_tuple::access::RELKIND_COMPOSITE_TYPE as i8;
        // Ignore the RTEPermissionInfo that would've been added.
        new_excl_rte.perminfoindex = 0;

        parsetree.rtable.push(new_excl_rte);
        let new_excl_rel_index = parsetree.rtable.len() as i32;
        parsetree
            .onConflict
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!())
            .exclRelIndex = new_excl_rel_index;

        // Replace the targetlist for the EXCLUDED pseudo-relation with a new one,
        // representing the columns from the new base relation.
        // (rewriteHandler.c:3705)
        let new_excl_tlist = ::parser_analyze::BuildOnConflictExcludedTargetlist(
            mcx,
            &base_rel,
            new_excl_rel_index,
        )?;
        parsetree
            .onConflict
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!())
            .exclRelTlist = new_excl_tlist;

        // Update all Vars in the ON CONFLICT clause that refer to the old
        // EXCLUDED pseudo-relation. We use the column mappings defined in the
        // view targetlist, but the outputs must refer to the new EXCLUDED
        // pseudo-relation rather than the new target RTE. "EXCLUDED.*" will be
        // expanded using the view's rowtype, which is correct.
        // (rewriteHandler.c:3719)
        let mut tmp_tlist: Vec<::nodes::primnodes::TargetEntry<'mcx>> = view_targetlist
            .iter()
            .map(|t| t.clone_in(mcx))
            .collect::<PgResult<_>>()?;
        for tle in tmp_tlist.iter_mut() {
            let mut node = Node::mk_target_entry(mcx, tle.clone_in(mcx)?)?;
            ChangeVarNodes(&mut node, new_rt_index, new_excl_rel_index, 0, mcx);
            *tle = node.into_targetentry().unwrap_or_else(|| unreachable!());
        }

        let view_rte = parsetree.rtable[view_rte_idx].clone_in(mcx)?;
        let oc = parsetree
            .onConflict
            .take()
            .unwrap_or_else(|| unreachable!());
        let mut oc_node = Node::mk_on_conflict_expr(mcx, (*oc).clone_in(mcx)?)?;
        let mut outer_has_sublinks = Some(parsetree.hasSubLinks);
        ReplaceVarsFromTargetList(
            &mut oc_node,
            old_excl_rel_index,
            0,
            &view_rte,
            &tmp_tlist,
            new_rt_index,
            ReplaceVarsNoMatchOption::ReportError,
            0,
            &mut outer_has_sublinks,
            mcx,
        )?;
        parsetree.hasSubLinks = outer_has_sublinks.unwrap_or(parsetree.hasSubLinks);
        let new_oc = oc_node
            .into_onconflictexpr()
            .unwrap_or_else(|| unreachable!());
        parsetree.onConflict = Some(alloc_in(mcx, new_oc)?);
    }

    // For UPDATE/DELETE/MERGE, pull up any WHERE quals from the view, re-pointed
    // at the new target. For INSERT the view's quals are ignored in the main
    // query (only the WITH CHECK OPTION, below, uses them).
    let view_quals_present = viewquery
        .jointree
        .as_ref()
        .and_then(|jt| jt.quals.as_deref())
        .is_some();
    if parsetree.commandType != CmdType::CMD_INSERT && view_quals_present {
        let mut viewqual = viewquery
            .jointree
            .as_ref()
            .and_then(|jt| jt.quals.as_deref())
            .unwrap()
            .clone_in(mcx)?;
        ChangeVarNodes(&mut viewqual, base_rt_index, new_rt_index, 0, mcx);

        if rewrite_relation_is_security_view(view) {
            // Security-barrier view: prepend the qual as a security qual on the
            // new RTE. Not exercised on the simple-view spine; the option flag
            // is not carried (always false), so this branch is unreachable.
            let new_rte = &mut parsetree.rtable[(new_rt_index - 1) as usize];
            let existing: Vec<NodePtr<'mcx>> = new_rte.securityQuals.drain(..).collect();
            let mut sq = PgVec::new_in(mcx);
            sq.push(alloc_in(mcx, viewqual)?);
            sq.extend(existing);
            new_rte.securityQuals = sq;
            if !parsetree.hasSubLinks {
                let added = &parsetree.rtable[(new_rt_index - 1) as usize].securityQuals[0];
                parsetree.hasSubLinks = checkExprHasSubLink(added);
            }
        } else {
            AddQual(&mut parsetree, Some(&viewqual), mcx)?;
        }
    }

    // For INSERT/UPDATE (or MERGE containing INSERT/UPDATE), handle the WITH
    // CHECK OPTION. The view-option flags (check_option/cascaded) are not carried
    // on the trimmed rd_options, so both read false here and no WCO is added —
    // a view defined WITH CHECK OPTION is the documented banked blocker.
    if insert_or_update {
        let mut has_wco = rewrite_relation_has_check_option(view);
        let mut cascaded = rewrite_relation_has_cascaded_check_option(view);

        if let Some(parent) = parsetree.withCheckOptions.first() {
            if let Some(parent_wco) = (**parent).as_withcheckoption() {
                if parent_wco.cascaded {
                    has_wco = true;
                    cascaded = true;
                }
            }
        }

        if has_wco && (cascaded || view_quals_present) {
            let mut wco = ::nodes::rawnodes::WithCheckOption {
                kind: ::nodes::rawnodes::WCOKind::WCO_VIEW_CHECK,
                relname: Some(PgString::from_str_in(view.name(), mcx)?),
                polname: None,
                qual: None,
                cascaded,
            };
            let mut added_sublink = false;
            if view_quals_present {
                let mut q = viewquery
                    .jointree
                    .as_ref()
                    .and_then(|jt| jt.quals.as_deref())
                    .unwrap()
                    .clone_in(mcx)?;
                ChangeVarNodes(&mut q, base_rt_index, new_rt_index, 0, mcx);
                if parsetree.commandType == CmdType::CMD_INSERT {
                    added_sublink = checkExprHasSubLink(&q);
                }
                wco.qual = Some(alloc_in(mcx, q)?);
            }
            let existing_wcos: Vec<NodePtr<'mcx>> =
                parsetree.withCheckOptions.drain(..).collect();
            let mut new_wcos = PgVec::new_in(mcx);
            new_wcos.push(alloc_in(mcx, Node::mk_with_check_option(mcx, wco)?)?);
            new_wcos.extend(existing_wcos);
            parsetree.withCheckOptions = new_wcos;
            if !parsetree.hasSubLinks && added_sublink {
                parsetree.hasSubLinks = true;
            }
        }
    }

    base_rel.close(NoLock)?;
    Ok(parsetree)
}

/// `get_tle_by_resno(view_targetlist, resno)` then take the base-relation
/// `varattno` (the C resno-remap helper inlined at three sites in
/// `rewriteTargetView`). Errors exactly as the C `elog` if the entry is missing,
/// junk, or not a plain Var.
fn view_tle_base_attno<'mcx>(
    view_targetlist: &[::nodes::primnodes::TargetEntry<'mcx>],
    resno: i16,
) -> PgResult<i16> {
    use ::parser_relation::get_tle_by_resno;
    let view_tle = get_tle_by_resno(view_targetlist, resno);
    match view_tle {
        Some(tle) if !tle.resjunk => {
            if let Some(var) = tle.expr.as_deref().and_then(Expr::as_var) {
                return Ok(var.varattno);
            }
            Err(elog(&format!(
                "attribute number {resno} not found in view targetlist"
            )))
        }
        _ => Err(elog(&format!(
            "attribute number {resno} not found in view targetlist"
        ))),
    }
}

/// `RelationHasSecurityInvoker(view)` via the rewriteHandler seam.
fn rewrite_relation_has_security_invoker(view: &Relation<'_>) -> bool {
    rewritehandler_seams::relation_has_security_invoker::call(view)
}
fn rewrite_relation_is_security_view(view: &Relation<'_>) -> bool {
    rewritehandler_seams::relation_is_security_view::call(view)
}
fn rewrite_relation_has_check_option(view: &Relation<'_>) -> bool {
    rewritehandler_seams::relation_has_check_option::call(view)
}
fn rewrite_relation_has_cascaded_check_option(view: &Relation<'_>) -> bool {
    rewritehandler_seams::relation_has_cascaded_check_option::call(view)
}

// ===========================================================================
// RewriteQuery + QueryRewrite (rewriteHandler.c:3882, 4566)
// ===========================================================================

/// A `rewrite_event` (rewriteHandler.c) — the (relation, event) recursion guard.
#[derive(Clone, Copy, PartialEq, Eq)]
struct RewriteEvent {
    relation: Oid,
    event: CmdType,
}

/// `RewriteQuery(parsetree, rewrite_events, orig_rt_length, num_ctes_processed)`
/// (rewriteHandler.c:3882) — the non-SELECT rule-firing driver. Returns the list
/// of result queries.
fn RewriteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: Query<'mcx>,
    rewrite_events: &mut Vec<RewriteEvent>,
    orig_rt_length: i32,
    mut num_ctes_processed: i32,
) -> PgResult<Vec<Query<'mcx>>> {
    let event = parsetree.commandType;
    let mut instead = false;
    let mut returning = false;
    let mut updatableview = false;
    let mut qual_product: Option<Query<'mcx>> = None;
    let mut rewritten: Vec<Query<'mcx>> = Vec::new();
    // Captured before `parsetree` is consumed into `rewritten`: the C end-of-
    // function check rejects a WITH query that rules expanded into multiple
    // (non-utility) result queries.
    let had_cte_list = !parsetree.cteList.is_empty();

    // First, recursively process data-modifying CTEs.
    let cte_count = parsetree.cteList.len() as i32;
    for i in 0..parsetree.cteList.len() {
        if (i as i32) >= cte_count - num_ctes_processed {
            break;
        }
        let ctequery_cmd = cte_query_command_type(&parsetree.cteList[i]);
        if ctequery_cmd == Some(CmdType::CMD_SELECT) {
            continue;
        }
        if ctequery_cmd.is_none() {
            continue;
        }
        let Some(ctequery) = take_cte_query(&mut parsetree.cteList[i]).take() else {
            continue;
        };
        let mut newstuff = RewriteQuery(mcx, ctequery, rewrite_events, 0, 0)?;
        if newstuff.len() == 1 {
            let cq = newstuff.pop().unwrap();
            match cq.commandType {
                CmdType::CMD_SELECT
                | CmdType::CMD_UPDATE
                | CmdType::CMD_INSERT
                | CmdType::CMD_DELETE
                | CmdType::CMD_MERGE => {}
                _ => {
                    return Err(PgError::new(
                        ERROR,
                        "DO INSTEAD NOTIFY rules are not supported for data-modifying statements in WITH".to_string(),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
                }
            }
            set_cte_query(mcx, &mut parsetree.cteList[i], cq)?;
        } else if newstuff.is_empty() {
            return Err(PgError::new(
                ERROR,
                "DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        } else {
            for q in newstuff.iter() {
                if q.querySource == QuerySource::QSRC_QUAL_INSTEAD_RULE {
                    return Err(PgError::new(ERROR,
                        "conditional DO INSTEAD rules are not supported for data-modifying statements in WITH".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
                }
                if q.querySource == QuerySource::QSRC_NON_INSTEAD_RULE {
                    return Err(PgError::new(ERROR,
                        "DO ALSO rules are not supported for data-modifying statements in WITH".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
                }
            }
            return Err(PgError::new(ERROR,
                "multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }
    num_ctes_processed = parsetree.cteList.len() as i32;

    if event != CmdType::CMD_SELECT && event != CmdType::CMD_UTILITY {
        let result_relation = parsetree.resultRelation;
        debug_assert!(result_relation != 0);
        let rt_relid = parsetree.rtable[(result_relation - 1) as usize].relid;
        debug_assert!(
            parsetree.rtable[(result_relation - 1) as usize].rtekind == RTEKind::RTE_RELATION
        );

        let rt_entry_relation = table_open(mcx, rt_relid, NoLock)?;
        let mut has_update = false;
        let mut values_rte_index = 0i32;
        let mut defaults_remaining = false;

        // Rewrite the targetlist for the command type.
        match event {
            CmdType::CMD_INSERT => {
                // Look for a multi-row INSERT...VALUES (a VALUES RTE in fromlist).
                let mut values_rte_found: Option<i32> = None;
                if let Some(jt) = parsetree.jointree.as_deref() {
                    for item in jt.fromlist.iter() {
                        if let Some(rtr) = (**item).as_rangetblref() {
                            if rtr.rtindex > orig_rt_length {
                                let rte = &parsetree.rtable[(rtr.rtindex - 1) as usize];
                                if rte.rtekind == RTEKind::RTE_VALUES {
                                    if values_rte_found.is_some() {
                                        return Err(elog("more than one VALUES RTE found"));
                                    }
                                    values_rte_found = Some(rtr.rtindex);
                                    values_rte_index = rtr.rtindex;
                                }
                            }
                        }
                    }
                }

                if let Some(vidx) = values_rte_found {
                    let mut unused: Option<PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>> =
                        None;
                    let values_rte = parsetree.rtable[(vidx - 1) as usize].clone_in(mcx)?;
                    let new_tlist = rewriteTargetListIU(
                        mcx,
                        &parsetree.targetList,
                        parsetree.commandType,
                        parsetree.r#override,
                        &rt_entry_relation,
                        Some(&values_rte),
                        vidx,
                        &mut unused,
                    )?;
                    parsetree.targetList = new_tlist;
                    if !rewriteValuesRTE(
                        mcx,
                        &mut parsetree,
                        vidx,
                        &rt_entry_relation,
                        unused.as_deref(),
                    )? {
                        defaults_remaining = true;
                    }
                } else {
                    let mut unused = None;
                    let new_tlist = rewriteTargetListIU(
                        mcx,
                        &parsetree.targetList,
                        parsetree.commandType,
                        parsetree.r#override,
                        &rt_entry_relation,
                        None,
                        0,
                        &mut unused,
                    )?;
                    parsetree.targetList = new_tlist;
                }

                // C `rewriteHandler.c`:
                //   if (parsetree->onConflict &&
                //       parsetree->onConflict->action == ONCONFLICT_UPDATE)
                //       parsetree->onConflict->onConflictSet =
                //           rewriteTargetListIU(parsetree->onConflict->onConflictSet,
                //                               CMD_UPDATE, parsetree->override,
                //                               rt_entry_relation, NULL, 0, NULL);
                //
                // The DO UPDATE SET targetlist references the EXCLUDED pseudo
                // relation (already in the rtable from analysis); it is
                // normalized against the real target relation's tupdesc exactly
                // like an UPDATE targetlist (defaults applied, dup assignments
                // merged, junk sorted last), with no VALUES RTE.
                let do_update = matches!(
                    parsetree.onConflict.as_deref(),
                    Some(oc) if oc.action == OnConflictAction::ONCONFLICT_UPDATE
                );
                if do_update {
                    // Downcast the `PgVec<NodePtr>` SET list to `TargetEntry`s
                    // (the stored form is the raw-node list, mirroring C's
                    // `List *onConflictSet` of TargetEntry).
                    let oc = parsetree.onConflict.as_deref().unwrap();
                    let mut old_set: Vec<::nodes::primnodes::TargetEntry<'mcx>> =
                        Vec::with_capacity(oc.onConflictSet.len());
                    for tle_node in oc.onConflictSet.iter() {
                        let Some(tle) = (**tle_node).as_targetentry() else {
                            return Err(elog(
                                "onConflictSet entry is not a TargetEntry",
                            ));
                        };
                        old_set.push(tle.clone_in(mcx)?);
                    }

                    let mut unused = None;
                    let new_set = rewriteTargetListIU(
                        mcx,
                        &old_set,
                        CmdType::CMD_UPDATE,
                        parsetree.r#override,
                        &rt_entry_relation,
                        None,
                        0,
                        &mut unused,
                    )?;

                    // Box the rewritten `TargetEntry`s back into the
                    // `PgVec<NodePtr>` SET list.
                    let mut new_set_nodes: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                    for tle in new_set.into_iter() {
                        new_set_nodes
                            .push(alloc_in(mcx, Node::mk_target_entry(mcx, tle)?)?);
                    }
                    let oc = parsetree.onConflict.as_deref_mut().unwrap();
                    oc.onConflictSet = new_set_nodes;
                }
            }
            CmdType::CMD_UPDATE => {
                debug_assert!(parsetree.r#override == OVERRIDING_NOT_SET);
                let mut unused = None;
                let new_tlist = rewriteTargetListIU(
                    mcx,
                    &parsetree.targetList,
                    parsetree.commandType,
                    parsetree.r#override,
                    &rt_entry_relation,
                    None,
                    0,
                    &mut unused,
                )?;
                parsetree.targetList = new_tlist;
            }
            CmdType::CMD_MERGE => {
                // `rewriteHandler.c:4097` — Rewrite each MERGE action targetlist
                // separately. MERGE actions do not permit multi-row INSERTs, so
                // there is no VALUES RTE to deal with here.
                debug_assert!(parsetree.r#override == OVERRIDING_NOT_SET);
                for node in parsetree.mergeActionList.iter_mut() {
                    let Some(action) = (**node).as_mergeaction_mut() else {
                        continue;
                    };
                    match action.commandType {
                        // CMD_NOTHING / CMD_DELETE — nothing to do here.
                        CmdType::CMD_NOTHING | CmdType::CMD_DELETE => {}
                        CmdType::CMD_UPDATE | CmdType::CMD_INSERT => {
                            // Downcast the raw-node SET list to `TargetEntry`s
                            // (the stored form mirrors C's `List *targetList`).
                            let mut old_tlist: Vec<
                                ::nodes::primnodes::TargetEntry<'mcx>,
                            > = Vec::with_capacity(action.targetList.len());
                            for tle_node in action.targetList.iter() {
                                let Some(tle) = (**tle_node).as_targetentry() else {
                                    rt_entry_relation.close(NoLock)?;
                                    return Err(elog(
                                        "MergeAction targetList entry is not a TargetEntry",
                                    ));
                                };
                                old_tlist.push(tle.clone_in(mcx)?);
                            }

                            let mut unused = None;
                            let new_tlist = rewriteTargetListIU(
                                mcx,
                                &old_tlist,
                                action.commandType,
                                action.r#override,
                                &rt_entry_relation,
                                None,
                                0,
                                &mut unused,
                            )?;

                            // Box the rewritten `TargetEntry`s back into the
                            // raw-node SET list.
                            let mut new_tlist_nodes: PgVec<'mcx, NodePtr<'mcx>> =
                                PgVec::new_in(mcx);
                            for tle in new_tlist.into_iter() {
                                new_tlist_nodes
                                    .push(alloc_in(mcx, Node::mk_target_entry(mcx, tle)?)?);
                            }
                            action.targetList = new_tlist_nodes;
                        }
                        other => {
                            rt_entry_relation.close(NoLock)?;
                            return Err(elog(format!(
                                "unrecognized commandType: {}",
                                other as i32
                            )));
                        }
                    }
                }
            }
            CmdType::CMD_DELETE => { /* nothing to do */ }
            other => {
                rt_entry_relation.close(NoLock)?;
                return Err(elog(format!("unrecognized commandType: {}", other as i32)));
            }
        }

        // Collect and apply rules.
        let locks = matchLocks(
            mcx,
            event,
            &rt_entry_relation,
            result_relation,
            &parsetree,
            &mut has_update,
        )?;
        let product_orig_rt_length = parsetree.rtable.len() as i32;
        let mut product_queries = fireRules(
            mcx,
            &parsetree,
            result_relation,
            event,
            locks,
            &mut instead,
            &mut returning,
            &mut qual_product,
        )?;

        // Finalize VALUES RTEs with remaining DEFAULTs for product queries.
        if defaults_remaining && !product_queries.is_empty() {
            for pt in product_queries.iter_mut() {
                // The product VALUES RTE may be in an INSERT...SELECT subquery.
                let target_idx: Option<usize> = if pt.commandType == CmdType::CMD_INSERT
                    && pt
                        .jointree
                        .as_deref()
                        .map(|jt| jt.fromlist.len() == 1)
                        .unwrap_or(false)
                {
                    let jt = pt.jointree.as_deref().unwrap();
                    if let Some(rtr) = (*jt.fromlist[0]).as_rangetblref() {
                        let src = &pt.rtable[(rtr.rtindex - 1) as usize];
                        if src.rtekind == RTEKind::RTE_SUBQUERY
                            && src
                                .subquery
                                .as_deref()
                                .map(|q| q.commandType == CmdType::CMD_SELECT)
                                .unwrap_or(false)
                        {
                            Some(rtr.rtindex as usize)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                match target_idx {
                    Some(sub_idx) => {
                        let sub = pt.rtable[sub_idx - 1].subquery.as_deref_mut().unwrap();
                        if sub.rtable[(values_rte_index - 1) as usize].rtekind
                            != RTEKind::RTE_VALUES
                        {
                            return Err(elog("failed to find VALUES RTE in product query"));
                        }
                        rewriteValuesRTEToNulls(mcx, sub, values_rte_index)?;
                    }
                    None => {
                        if pt.rtable[(values_rte_index - 1) as usize].rtekind
                            != RTEKind::RTE_VALUES
                        {
                            return Err(elog("failed to find VALUES RTE in product query"));
                        }
                        rewriteValuesRTEToNulls(mcx, pt, values_rte_index)?;
                    }
                }
            }
        }

        // Auto-updatable view rewrite. If there was no unqualified INSTEAD rule,
        // and the target is a view without INSTEAD OF triggers, see if the view
        // can be automatically updated and, if so, rewrite the query here and
        // add it to product_queries so it gets recursively rewritten.
        //
        // The view-rewritten query (the C `pt == parsetree`) is recursed with
        // `orig_rt_length` (to finish any VALUES RTE it contained), while the
        // fireRules product queries are recursed with `product_orig_rt_length`;
        // we therefore carry it separately.
        let mut view_rewritten: Option<Query<'mcx>> = None;
        if !instead
            && rt_entry_relation.rd_rel.relkind == RELKIND_VIEW as u8
            && !view_has_instead_trigger(&rt_entry_relation, event, &parsetree.mergeActionList)?
        {
            // Qualified INSTEAD rules block automatic updating.
            if qual_product.is_some() {
                let err = crate::error_view_not_updatable(
                    &rt_entry_relation,
                    parsetree.commandType,
                    &parsetree.mergeActionList,
                    Some("Views with conditional DO INSTEAD rules are not automatically updatable."),
                );
                rt_entry_relation.close(NoLock)?;
                return Err(err);
            }

            // Attempt the rewrite (throws if the view can't be auto-updated).
            parsetree = match rewriteTargetView(mcx, parsetree, &rt_entry_relation) {
                Ok(q) => q,
                Err(e) => {
                    rt_entry_relation.close(NoLock)?;
                    return Err(e);
                }
            };

            // product_queries holds any DO ALSO rule actions. The rewritten
            // query goes before (INSERT) or after (UPDATE/DELETE/MERGE) those,
            // but since we recurse it with a different orig_rt_length we keep it
            // in `view_rewritten` and stitch the ordering into `rewritten` below.
            view_rewritten = Some(parsetree);
            // parsetree has been consumed; re-borrow a placeholder so subsequent
            // reads (RETURNING check) see the suppressed-instead state.
            parsetree = view_rewritten.as_ref().unwrap().clone_in(mcx)?;

            instead = true;
            returning = true;
            updatableview = true;
        }

        // Whether fireRules produced any product queries (the C
        // `product_queries != NIL` test, captured before they are consumed by
        // the recursive rewrite below — a product query can itself rewrite to
        // nothing, so `rewritten` is not an equivalent witness).
        let product_queries_nonempty = !product_queries.is_empty();

        // Recursively rewrite product queries (with recursion guard).
        if !product_queries.is_empty() || view_rewritten.is_some() {
            let guard = RewriteEvent {
                relation: rt_entry_relation.rd_id,
                event,
            };
            if rewrite_events.contains(&guard) {
                let name = rt_entry_relation.name().to_string();
                rt_entry_relation.close(NoLock)?;
                return Err(PgError::new(
                    ERROR,
                    format!("infinite recursion detected in rules for relation \"{name}\""),
                )
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
            }
            rewrite_events.push(guard);
            // C ordering: for an INSERT updatable view, the rewritten query is
            // lcons'd (first); otherwise it is lappend'd (last). Recurse it with
            // `orig_rt_length`; the fireRules product queries with
            // `product_orig_rt_length`.
            let view_is_insert =
                view_rewritten.as_ref().map(|q| q.commandType == CmdType::CMD_INSERT);
            if view_is_insert == Some(true) {
                if let Some(vq) = view_rewritten.take() {
                    rewritten.extend(RewriteQuery(mcx, vq, rewrite_events, orig_rt_length, num_ctes_processed)?);
                }
            }
            for pt in product_queries.into_iter() {
                let newstuff =
                    RewriteQuery(mcx, pt, rewrite_events, product_orig_rt_length, num_ctes_processed)?;
                rewritten.extend(newstuff);
            }
            if let Some(vq) = view_rewritten.take() {
                // non-INSERT updatable view: appended after the DO ALSO actions.
                rewritten.extend(RewriteQuery(mcx, vq, rewrite_events, orig_rt_length, num_ctes_processed)?);
            }
            rewrite_events.pop();
        }

        // RETURNING-without-rule-returning errors.
        if (instead || qual_product.is_some())
            && !parsetree.returningList.is_empty()
            && !returning
        {
            let (msg, hint) = match event {
                CmdType::CMD_INSERT => (
                    format!("cannot perform INSERT RETURNING on relation \"{}\"", rt_entry_relation.name()),
                    "You need an unconditional ON INSERT DO INSTEAD rule with a RETURNING clause.",
                ),
                CmdType::CMD_UPDATE => (
                    format!("cannot perform UPDATE RETURNING on relation \"{}\"", rt_entry_relation.name()),
                    "You need an unconditional ON UPDATE DO INSTEAD rule with a RETURNING clause.",
                ),
                CmdType::CMD_DELETE => (
                    format!("cannot perform DELETE RETURNING on relation \"{}\"", rt_entry_relation.name()),
                    "You need an unconditional ON DELETE DO INSTEAD rule with a RETURNING clause.",
                ),
                other => {
                    rt_entry_relation.close(NoLock)?;
                    return Err(elog(format!("unrecognized commandType: {}", other as i32)));
                }
            };
            rt_entry_relation.close(NoLock)?;
            return Err(PgError::new(ERROR, msg)
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint(hint.to_string()));
        }

        // ON CONFLICT + rules error.
        // C: onConflict && (product_queries != NIL || hasUpdate) && !updatableview
        if parsetree.onConflict.is_some()
            && (product_queries_nonempty || has_update)
            && !updatableview
        {
            rt_entry_relation.close(NoLock)?;
            return Err(PgError::new(
                ERROR,
                "INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        let _ = &mut updatableview;

        rt_entry_relation.close(NoLock)?;
    }

    // Add the original (or qual_product) query.
    if !instead {
        if parsetree.commandType == CmdType::CMD_INSERT {
            if let Some(qp) = qual_product.take() {
                rewritten.insert(0, qp);
            } else {
                rewritten.insert(0, parsetree);
            }
        } else if let Some(qp) = qual_product.take() {
            rewritten.push(qp);
        } else {
            rewritten.push(parsetree);
        }
    } else {
        // parsetree consumed conceptually; nothing to add.
    }

    // If the original query has a CTE list, and we generated more than one
    // (non-utility) query, reject it: CTEs must be evaluated exactly once.
    if had_cte_list {
        let qcount = rewritten
            .iter()
            .filter(|q| q.commandType != CmdType::CMD_UTILITY)
            .count();
        if qcount > 1 {
            return Err(PgError::new(
                ERROR,
                "WITH cannot be used in a query that is rewritten by rules into multiple queries"
                    .to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    Ok(rewritten)
}

/// `QueryRewrite(parsetree)` (rewriteHandler.c:4566) — the top-level rule
/// rewriter entry: fire non-SELECT rules, then RIR rules, then assign the
/// command tag. Returns the list of result queries.
pub fn QueryRewrite<'mcx>(mcx: Mcx<'mcx>, parsetree: Query<'mcx>) -> PgResult<Vec<Query<'mcx>>> {
    debug_assert!(parsetree.querySource == QuerySource::QSRC_ORIGINAL);
    debug_assert!(parsetree.canSetTag);

    let input_query_id = parsetree.queryId;
    let orig_cmd_type = parsetree.commandType;

    // Step 1: non-SELECT rules.
    let mut rewrite_events: Vec<RewriteEvent> = Vec::new();
    let querylist = RewriteQuery(mcx, parsetree, &mut rewrite_events, 0, 0)?;

    // Step 2: RIR rules on each query.
    let mut results: Vec<Query<'mcx>> = Vec::new();
    for query in querylist.into_iter() {
        let mut active_rirs: Vec<Oid> = Vec::new();
        let mut q = fireRIRrules(mcx, query, &mut active_rirs)?;
        q.queryId = input_query_id;
        results.push(q);
    }

    // Step 3: command-tag assignment.
    let mut found_original = false;
    let mut last_instead: Option<usize> = None;
    for (i, query) in results.iter().enumerate() {
        if query.querySource == QuerySource::QSRC_ORIGINAL {
            debug_assert!(query.canSetTag);
            debug_assert!(!found_original);
            found_original = true;
        } else {
            debug_assert!(!query.canSetTag);
            if query.commandType == orig_cmd_type
                && (query.querySource == QuerySource::QSRC_INSTEAD_RULE
                    || query.querySource == QuerySource::QSRC_QUAL_INSTEAD_RULE)
            {
                last_instead = Some(i);
            }
        }
    }
    if !found_original {
        if let Some(i) = last_instead {
            results[i].canSetTag = true;
        }
    }

    Ok(results)
}

// ===========================================================================
// Small local helpers (value-model renderings of C pointer idioms)
// ===========================================================================

/// `copyObject` of a `Query` into `mcx`.
fn core_clone<'mcx>(q: &Query<'mcx>, mcx: Mcx<'mcx>) -> PgResult<Query<'mcx>> {
    q.clone_in(mcx)
}

/// `mem::take` of a `PgVec` (`list_concat`/`= NIL` destructive idioms).
fn core_take_vec<'mcx, T>(v: &mut PgVec<'mcx, T>) -> PgVec<'mcx, T> {
    let mcx = *v.allocator();
    core::mem::replace(v, PgVec::new_in(mcx))
}

/// `makeString(pstrdup(s))` — a `String` value node.
fn make_string<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<StringNode<'mcx>> {
    Ok(StringNode {
        sval: PgString::from_str_in(s, mcx)?,
    })
}

/// `&mut Node::CommonTableExpr` from a `cteList` element (the C
/// `lfirst_node(CommonTableExpr, lc)`).
fn cte_mut<'a, 'mcx>(node: &'a mut NodePtr<'mcx>) -> Option<&'a mut CommonTableExpr<'mcx>> {
    (**node).as_commontableexpr_mut()
}

/// `&Node::CommonTableExpr` from a `cteList` element.
fn cte_ref<'a, 'mcx>(node: &'a NodePtr<'mcx>) -> Option<&'a CommonTableExpr<'mcx>> {
    (**node).as_commontableexpr()
}

/// The CTE's name (`cte->ctename`).
fn cte_name_of(node: &NodePtr<'_>) -> Option<String> {
    cte_ref(node).and_then(|c| c.ctename.as_deref().map(|s| s.to_string()))
}

/// `cte->search_clause || cte->cycle_clause`.
fn cte_has_search_or_cycle(node: &NodePtr<'_>) -> bool {
    cte_ref(node).is_some_and(|c| c.search_clause.is_some() || c.cycle_clause.is_some())
}

/// `castNode(Query, cte->ctequery)->commandType`.
fn cte_query_command_type(node: &NodePtr<'_>) -> Option<CmdType> {
    cte_ref(node)
        .and_then(|c| c.ctequery.as_deref())
        .and_then(|n| n.as_query())
        .map(|q| q.commandType)
}

/// Take the CTE's `ctequery` (a `Node::Query`) out as an owned `Query`.
fn take_cte_query<'mcx>(node: &mut NodePtr<'mcx>) -> Option<Query<'mcx>> {
    let cte = cte_mut(node)?;
    cte.ctequery
        .take()
        .and_then(|boxed| PgBox::into_inner(boxed).into_query())
}

/// Store an owned `Query` back as the CTE's `ctequery`.
fn set_cte_query<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut NodePtr<'mcx>,
    q: Query<'mcx>,
) -> PgResult<()> {
    if let Some(cte) = cte_mut(node) {
        cte.ctequery = Some(alloc_in(mcx, Node::mk_query(mcx, q)?)?);
    }
    Ok(())
}
