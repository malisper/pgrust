use core::fmt::Write as _;

use adt_quote::quote_identifier;
use format_type::{format_type_extended, FORMAT_TYPE_FORCE_QUALIFY, FORMAT_TYPE_TYPEMOD_GIVEN};
use mcx::{Mcx, PgString, PgVec};
use nodes_core::{expr_type, expr_typmod, strip_implicit_coercions};
use types_core::{
    catalog::{
        BITOID, BOOLOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NUMERICOID,
        OIDOID, PG_CATALOG_NAMESPACE, UNKNOWNOID, VARBITOID,
    },
    Oid,
};
use types_error::{PgError, PgResult};
use types_nodes::equal::equal;
use types_nodes::{
    BoolExprType, CoercionForm, Const, JoinType, Node, NodeTag, NullTestType,
};
use types_pathnodes::run::PlannerRun;
use types_pathnodes::RelId;

use crate::relinfo::fpinfo;
use crate::shippable;

// reg* type OIDs (pg_type.dat) and the catalogs their contents live in,
// needed by foreign_expr_walker's regproc/regoper/... shippability checks.
const REGPROCOID: Oid = 24;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCOLLATIONOID: Oid = 4191;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGNAMESPACEOID: Oid = 4089;
const REGROLEOID: Oid = 4096;

const FIRST_NORMAL_OBJECT_ID: Oid = types_core::catalog::FirstNormalObjectId;
const DEFAULT_COLLATION_OID: Oid = types_core::catalog::DEFAULT_COLLATION_OID;

const PROCEDURE_RELATION_ID: Oid = types_core::catalog::PROCEDURE_RELATION_ID;
const OPERATOR_RELATION_ID: Oid = types_core::catalog::OPERATOR_RELATION_ID;
const RELATION_RELATION_ID: Oid = types_core::catalog::RELATION_RELATION_ID;
const TYPE_RELATION_ID: Oid = types_core::catalog::TYPE_RELATION_ID;
const COLLATION_RELATION_ID: Oid = types_core::catalog::COLLATION_RELATION_ID;
const NAMESPACE_RELATION_ID: Oid = types_core::catalog::NAMESPACE_RELATION_ID;
const AUTH_ID_RELATION_ID: Oid = types_core::catalog::AUTH_ID_RELATION_ID;
const TS_CONFIG_RELATION_ID: Oid = 3602;
const TS_DICT_RELATION_ID: Oid = 3600;

const SELF_ITEM_POINTER_ATTNUM: i16 = -1;
const TABLE_OID_ATTNUM: i16 = -7;
const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 =
    types_tuple::htup::FirstLowInvalidHeapAttributeNumber;

const REL_ALIAS_PREFIX: &str = "r";

// StringInfo-shaped append over PgString. Divergence: OOM on these cold
// deparse appends is dropped (buffer truncates) rather than raised, matching
// the `write!`-discards pattern used elsewhere on cold formatting paths; the
// deparse buffer is small and per-plan.
trait Push {
    fn push_str(&mut self, s: &str);
    fn push(&mut self, c: char);
}
impl Push for PgString<'_> {
    #[inline]
    fn push_str(&mut self, s: &str) {
        let _ = self.try_push_str(s);
    }
    #[inline]
    fn push(&mut self, c: char) {
        let _ = self.try_push(c);
    }
}

// ---------- shippability walker (is_foreign_expr / foreign_expr_walker) ----------

#[derive(Clone, Copy, PartialEq)]
enum FdwCollateState {
    None,
    Safe,
    Unsafe,
}

#[derive(Clone, Copy)]
struct LocCxt {
    collation: Oid,
    state: FdwCollateState,
}

impl LocCxt {
    fn empty() -> Self {
        LocCxt { collation: types_core::InvalidOid, state: FdwCollateState::None }
    }
}

struct GlobCxt<'a, 'mcx> {
    run: &'a PlannerRun<'mcx>,
    foreignrel: RelId,
    relids_rel: RelId,
    serverid: Oid,
    shippable_extensions: &'a [Oid],
    mcx: Mcx<'mcx>,
}

impl<'mcx> GlobCxt<'_, 'mcx> {
    fn relids(&self) -> &types_pathnodes::Relids<'mcx> {
        &self.run.root.rel(self.relids_rel).relids
    }
}

fn is_shippable_obj(glob: &GlobCxt<'_, '_>, oid: Oid, class_id: Oid) -> PgResult<bool> {
    shippable::is_shippable(glob.mcx, oid, class_id, glob.serverid, glob.shippable_extensions)
}

/// classifyConditions: split RestrictInfos into remote-safe and local subsets.
pub fn classify_conditions<'mcx>(
    run: &PlannerRun<'mcx>,
    baserel: RelId,
    input_conds: &[types_pathnodes::RinfoId],
    remote_conds: &mut PgVec<'mcx, types_pathnodes::RinfoId>,
    local_conds: &mut PgVec<'mcx, types_pathnodes::RinfoId>,
) -> PgResult<()> {
    for &ri in input_conds {
        let clause = run.root.rinfo(ri).clause;
        if is_foreign_expr(run, baserel, *run.root.expr_node(clause))? {
            remote_conds.push(ri);
        } else {
            local_conds.push(ri);
        }
    }
    Ok(())
}

/// is_foreign_expr: true if expr is safe to evaluate on the foreign server.
pub fn is_foreign_expr<'mcx>(
    run: &PlannerRun<'mcx>,
    baserel: RelId,
    expr: Node<'mcx>,
) -> PgResult<bool> {
    let fp = fpinfo(run.root.rel(baserel)).borrow();
    let relids_rel = if is_upper_rel(run, baserel) {
        fp.outerrel.expect("upperrel has outerrel")
    } else {
        baserel
    };
    let glob = GlobCxt {
        run,
        foreignrel: baserel,
        relids_rel,
        serverid: fp.serverid(),
        shippable_extensions: &fp.shippable_extensions,
        mcx: run.mcx,
    };
    let mut loc = LocCxt::empty();
    if !foreign_expr_walker(&glob, expr, &mut loc, None)? {
        return Ok(false);
    }
    if loc.state == FdwCollateState::Unsafe {
        return Ok(false);
    }
    // Mutable functions can't be shipped (results not stable).
    if clauses::classify::contain_mutable_functions(expr)? {
        return Ok(false);
    }
    Ok(true)
}

fn is_upper_rel(run: &PlannerRun<'_>, rel: RelId) -> bool {
    run.root.rel(rel).reloptkind == types_pathnodes::RELOPT_UPPER_REL
}

fn collation_result(inner: &LocCxt, collation: Oid) -> FdwCollateState {
    if collation == types_core::InvalidOid {
        FdwCollateState::None
    } else if inner.state == FdwCollateState::Safe && collation == inner.collation {
        FdwCollateState::Safe
    } else if collation == DEFAULT_COLLATION_OID {
        FdwCollateState::None
    } else {
        FdwCollateState::Unsafe
    }
}

fn merge_collation(outer: &mut LocCxt, collation: Oid, state: FdwCollateState) {
    use FdwCollateState::*;
    if (state as u8) > (outer.state as u8) {
        outer.collation = collation;
        outer.state = state;
    } else if state == outer.state {
        match state {
            None | Unsafe => {}
            Safe => {
                if collation != outer.collation {
                    if outer.collation == DEFAULT_COLLATION_OID {
                        outer.collation = collation;
                    } else if collation != DEFAULT_COLLATION_OID {
                        outer.state = Unsafe;
                    }
                }
            }
        }
    }
}

fn foreign_expr_walker<'mcx>(
    glob: &GlobCxt<'_, 'mcx>,
    node: Node<'mcx>,
    outer_cxt: &mut LocCxt,
    case_arg_cxt: Option<&LocCxt>,
) -> PgResult<bool> {
    let mut check_type = true;
    let mut inner = LocCxt::empty();
    let collation: Oid;
    let state: FdwCollateState;

    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if types_pathnodes::relids::relids_is_member(var.varno, glob.relids())
                && var.varlevelsup == 0
            {
                if var.varattno < 0 && var.varattno as i16 != SELF_ITEM_POINTER_ATTNUM {
                    return Ok(false);
                }
                collation = var.varcollid;
                state = if collation != types_core::InvalidOid {
                    FdwCollateState::Safe
                } else {
                    FdwCollateState::None
                };
            } else {
                collation = var.varcollid;
                state = if collation == types_core::InvalidOid
                    || collation == DEFAULT_COLLATION_OID
                {
                    FdwCollateState::None
                } else {
                    FdwCollateState::Unsafe
                };
            }
        }
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            if !c.constisnull {
                let class_id = match c.consttype {
                    REGPROCOID | REGPROCEDUREOID => Some((PROCEDURE_RELATION_ID, false)),
                    REGOPEROID | REGOPERATOROID => Some((OPERATOR_RELATION_ID, false)),
                    REGCLASSOID => Some((RELATION_RELATION_ID, false)),
                    REGTYPEOID => Some((TYPE_RELATION_ID, false)),
                    REGCOLLATIONOID => Some((COLLATION_RELATION_ID, false)),
                    REGCONFIGOID => Some((TS_CONFIG_RELATION_ID, true)),
                    REGDICTIONARYOID => Some((TS_DICT_RELATION_ID, true)),
                    REGNAMESPACEOID => Some((NAMESPACE_RELATION_ID, false)),
                    REGROLEOID => Some((AUTH_ID_RELATION_ID, false)),
                    _ => None,
                };
                if let Some((class, ts_weakened)) = class_id {
                    let objid = c.constvalue.as_oid();
                    // TS objects below FirstNormalObjectId are always shippable.
                    if !(ts_weakened && objid < FIRST_NORMAL_OBJECT_ID)
                        && !is_shippable_obj(glob, objid, class)?
                    {
                        return Ok(false);
                    }
                }
            }
            collation = c.constcollid;
            state = if collation == types_core::InvalidOid || collation == DEFAULT_COLLATION_OID {
                FdwCollateState::None
            } else {
                FdwCollateState::Unsafe
            };
        }
        NodeTag::T_Param => {
            let p = node.as_param().unwrap();
            if p.paramkind == types_nodes::ParamKind::PARAM_MULTIEXPR {
                return Ok(false);
            }
            collation = p.paramcollid;
            state = if collation == types_core::InvalidOid || collation == DEFAULT_COLLATION_OID {
                FdwCollateState::None
            } else {
                FdwCollateState::Unsafe
            };
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            if sr.refassgnexpr.is_some() {
                return Ok(false);
            }
            if !walk_opt_list(glob, &sr.refupperindexpr, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            inner = LocCxt::empty();
            if !walk_opt_list(glob, &sr.reflowerindexpr, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            inner = LocCxt::empty();
            if let Some(refexpr) = sr.refexpr {
                if !foreign_expr_walker(glob, refexpr, &mut inner, case_arg_cxt)? {
                    return Ok(false);
                }
            }
            collation = sr.refcollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_FuncExpr => {
            let fe = node.as_func_expr().unwrap();
            if !is_shippable_obj(glob, fe.funcid, PROCEDURE_RELATION_ID)? {
                return Ok(false);
            }
            if !walk_list(glob, &fe.args, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            if fe.inputcollid != types_core::InvalidOid
                && (inner.state != FdwCollateState::Safe || fe.inputcollid != inner.collation)
            {
                return Ok(false);
            }
            collation = fe.funccollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_OpExpr => {
            let oe = node.as_op_expr().unwrap();
            if !is_shippable_obj(glob, oe.opno, OPERATOR_RELATION_ID)? {
                return Ok(false);
            }
            if !walk_list(glob, &oe.args, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            if oe.inputcollid != types_core::InvalidOid
                && (inner.state != FdwCollateState::Safe || oe.inputcollid != inner.collation)
            {
                return Ok(false);
            }
            collation = oe.opcollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_DistinctExpr => {
            let oe = node.as_distinct_expr().unwrap();
            if !is_shippable_obj(glob, oe.opno, OPERATOR_RELATION_ID)? {
                return Ok(false);
            }
            if !walk_list(glob, &oe.args, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            if oe.inputcollid != types_core::InvalidOid
                && (inner.state != FdwCollateState::Safe || oe.inputcollid != inner.collation)
            {
                return Ok(false);
            }
            collation = oe.opcollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let oe = node.as_scalar_array_op_expr().unwrap();
            if !is_shippable_obj(glob, oe.opno, OPERATOR_RELATION_ID)? {
                return Ok(false);
            }
            if !walk_list(glob, &oe.args, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            if oe.inputcollid != types_core::InvalidOid
                && (inner.state != FdwCollateState::Safe || oe.inputcollid != inner.collation)
            {
                return Ok(false);
            }
            collation = types_core::InvalidOid;
            state = FdwCollateState::None;
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            if !foreign_expr_walker(glob, r.arg, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            collation = r.resultcollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            if !walk_list(glob, &b.args, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            collation = types_core::InvalidOid;
            state = FdwCollateState::None;
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            if let Some(arg) = nt.arg {
                if !foreign_expr_walker(glob, arg, &mut inner, case_arg_cxt)? {
                    return Ok(false);
                }
            }
            collation = types_core::InvalidOid;
            state = FdwCollateState::None;
        }
        NodeTag::T_CaseExpr => {
            let ce = node.as_case_expr().unwrap();
            let mut arg_cxt = LocCxt::empty();
            if let Some(arg) = ce.arg {
                if !foreign_expr_walker(glob, arg, &mut arg_cxt, case_arg_cxt)? {
                    return Ok(false);
                }
            }
            for whennode in ce.args.iter() {
                let cw = whennode.as_case_when().unwrap();
                if ce.arg.is_some() {
                    // Optimizer may have rewritten the WHEN; only an OpExpr of
                    // "CaseTestExpr = RHS" shape is deparsable.
                    let when_expr = cw.expr.expect("CaseWhen has expr");
                    let Some(op) = when_expr.as_op_expr() else {
                        return Ok(false);
                    };
                    if op.args.len() != 2
                        || strip_implicit_coercions(op.args.nth(0)).node_tag()
                            != NodeTag::T_CaseTestExpr
                    {
                        return Ok(false);
                    }
                }
                let mut tmp = LocCxt::empty();
                if let Some(expr) = cw.expr {
                    if !foreign_expr_walker(glob, expr, &mut tmp, Some(&arg_cxt))? {
                        return Ok(false);
                    }
                }
                if let Some(result) = cw.result {
                    if !foreign_expr_walker(glob, result, &mut inner, case_arg_cxt)? {
                        return Ok(false);
                    }
                }
            }
            if let Some(defresult) = ce.defresult {
                if !foreign_expr_walker(glob, defresult, &mut inner, case_arg_cxt)? {
                    return Ok(false);
                }
            }
            collation = ce.casecollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_CaseTestExpr => {
            let c = node.as_case_test_expr().unwrap();
            let Some(arg) = case_arg_cxt else {
                return Ok(false);
            };
            collation = c.collation;
            state = if collation == types_core::InvalidOid {
                FdwCollateState::None
            } else if arg.state == FdwCollateState::Safe && collation == arg.collation {
                FdwCollateState::Safe
            } else if collation == DEFAULT_COLLATION_OID {
                FdwCollateState::None
            } else {
                FdwCollateState::Unsafe
            };
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            if !walk_list(glob, &a.elements, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            collation = a.array_collid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            if !is_upper_rel(glob.run, glob.foreignrel) {
                return Ok(false);
            }
            if agg.aggsplit != types_nodes::primnodes::AGGSPLIT_SIMPLE {
                return Ok(false);
            }
            if !is_shippable_obj(glob, agg.aggfnoid, PROCEDURE_RELATION_ID)? {
                return Ok(false);
            }
            for n in agg.args.iter() {
                let arg = match n.as_target_entry() {
                    Some(tle) => tle.expr,
                    None => n,
                };
                if !foreign_expr_walker(glob, arg, &mut inner, case_arg_cxt)? {
                    return Ok(false);
                }
            }
            for srt_node in agg.aggorder.iter() {
                let srt = srt_node
                    .as_variant::<types_nodes::parsenodes::SortGroupClause>()
                    .expect("aggorder holds SortGroupClause");
                let tle = get_sortgroupref_tle(srt.tleSortGroupRef, &agg.args);
                let sortcoltype = expr_type(tle.expr);
                let typentry = typcache::lookup_type_cache(
                    sortcoltype,
                    typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_GT_OPR,
                )?;
                if srt.sortop != typentry.lt_opr()
                    && srt.sortop != typentry.gt_opr()
                    && !is_shippable_obj(glob, srt.sortop, OPERATOR_RELATION_ID)?
                {
                    return Ok(false);
                }
            }
            if let Some(aggfilter) = agg.aggfilter {
                if !foreign_expr_walker(glob, aggfilter, &mut inner, case_arg_cxt)? {
                    return Ok(false);
                }
            }
            if agg.inputcollid != types_core::InvalidOid
                && (inner.state != FdwCollateState::Safe || agg.inputcollid != inner.collation)
            {
                return Ok(false);
            }
            collation = agg.aggcollid;
            state = collation_result(&inner, collation);
        }
        NodeTag::T_List => {
            let l = node.as_list().unwrap();
            if !walk_list(glob, l, &mut inner, case_arg_cxt)? {
                return Ok(false);
            }
            collation = inner.collation;
            state = inner.state;
            check_type = false;
        }
        _ => return Ok(false),
    }

    if check_type && !is_shippable_obj(glob, expr_type(node), TYPE_RELATION_ID)? {
        return Ok(false);
    }
    merge_collation(outer_cxt, collation, state);
    Ok(true)
}

fn walk_list<'mcx>(
    glob: &GlobCxt<'_, 'mcx>,
    list: &types_nodes::list::NodeList<'mcx>,
    inner: &mut LocCxt,
    case_arg_cxt: Option<&LocCxt>,
) -> PgResult<bool> {
    for n in list.iter() {
        if !foreign_expr_walker(glob, n, inner, case_arg_cxt)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn walk_opt_list<'mcx>(
    glob: &GlobCxt<'_, 'mcx>,
    list: &types_nodes::list::OptNodeList<'mcx>,
    inner: &mut LocCxt,
    case_arg_cxt: Option<&LocCxt>,
) -> PgResult<bool> {
    for cell in list.iter() {
        if let Some(n) = cell {
            if !foreign_expr_walker(glob, n, inner, case_arg_cxt)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn get_sortgroupref_tle<'mcx>(
    sortref: u32,
    target_list: &'mcx types_nodes::list::NodeList<'mcx>,
) -> &'mcx types_nodes::TargetEntry<'mcx> {
    for n in target_list.iter() {
        let tle = n.as_target_entry().expect("targetList entry");
        if tle.ressortgroupref == sortref {
            return tle;
        }
    }
    panic!("ORDER/GROUP BY expression not found in targetlist");
}

/// is_foreign_param: does this top-level expr have to be sent as a Param?
pub fn is_foreign_param<'mcx>(run: &PlannerRun<'mcx>, baserel: RelId, expr: Node<'mcx>) -> bool {
    match expr.node_tag() {
        NodeTag::T_Var => {
            let var = expr.as_var().unwrap();
            let relids_rel = {
                let fp = fpinfo(run.root.rel(baserel)).borrow();
                if is_upper_rel(run, baserel) {
                    fp.outerrel.expect("upperrel outerrel")
                } else {
                    baserel
                }
            };
            let relids = &run.root.rel(relids_rel).relids;
            !(types_pathnodes::relids::relids_is_member(var.varno, relids)
                && var.varlevelsup == 0)
        }
        NodeTag::T_Param => true,
        _ => false,
    }
}

// ---------- name / literal helpers ----------

pub const fn get_jointype_name(jointype: JoinType) -> &'static str {
    match jointype {
        JoinType::JOIN_INNER => "INNER",
        JoinType::JOIN_LEFT => "LEFT",
        JoinType::JOIN_RIGHT => "RIGHT",
        JoinType::JOIN_FULL => "FULL",
        JoinType::JOIN_SEMI => "SEMI",
        _ => panic!("unsupported join type"),
    }
}

fn deparse_type_name(type_oid: Oid, typemod: i32) -> PgResult<String> {
    let mut flags = FORMAT_TYPE_TYPEMOD_GIVEN;
    if !shippable::is_builtin(type_oid) {
        flags |= FORMAT_TYPE_FORCE_QUALIFY;
    }
    Ok(format_type_extended(type_oid, typemod, flags)?.expect("format_type non-null"))
}

/// deparseStringLiteral: single-quoted, with E'' when backslashes appear.
pub fn deparse_string_literal(buf: &mut PgString<'_>, val: &str) {
    const ESCAPE_STRING_SYNTAX: char = 'E';
    if val.contains('\\') {
        buf.push(ESCAPE_STRING_SYNTAX);
    }
    buf.push('\'');
    for ch in val.chars() {
        if ch == '\'' || ch == '\\' {
            buf.push(ch);
        }
        buf.push(ch);
    }
    buf.push('\'');
}

fn append_quoted_identifier(buf: &mut PgString<'_>, mcx: Mcx<'_>, ident: &str) -> PgResult<()> {
    let q = quote_identifier(mcx, ident.as_bytes())?;
    // SAFETY: quote_identifier preserves the (UTF-8) ident bytes, only adding
    // ASCII `"` quoting.
    buf.push_str(unsafe { core::str::from_utf8_unchecked(q.as_bytes()) });
    Ok(())
}

// ---------- deparse context ----------

pub struct DeparseCtx<'a, 'mcx> {
    pub run: &'a PlannerRun<'mcx>,
    pub foreignrel: RelId,
    pub scanrel: RelId,
    pub buf: PgString<'mcx>,
    /// None = EXPLAIN (params become placeholders); Some = real params_list.
    pub params_list: Option<PgVec<'mcx, Node<'mcx>>>,
    pub mcx: Mcx<'mcx>,
}

impl<'a, 'mcx> DeparseCtx<'a, 'mcx> {
    fn scanrel_relids(&self) -> &types_pathnodes::Relids<'mcx> {
        &self.run.root.rel(self.scanrel).relids
    }
}

fn deparse_expr<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => deparse_var(ctx, node),
        NodeTag::T_Const => deparse_const(ctx, node.as_const().unwrap(), 0),
        NodeTag::T_Param => deparse_param(ctx, node),
        NodeTag::T_SubscriptingRef => deparse_subscripting_ref(ctx, node),
        NodeTag::T_FuncExpr => deparse_func_expr(ctx, node),
        NodeTag::T_OpExpr => deparse_op_expr(ctx, node),
        NodeTag::T_DistinctExpr => deparse_distinct_expr(ctx, node),
        NodeTag::T_ScalarArrayOpExpr => deparse_scalar_array_op_expr(ctx, node),
        NodeTag::T_RelabelType => deparse_relabel_type(ctx, node.as_relabel_type().unwrap()),
        NodeTag::T_BoolExpr => deparse_bool_expr(ctx, node.as_bool_expr().unwrap()),
        NodeTag::T_NullTest => deparse_null_test(ctx, node.as_null_test().unwrap()),
        NodeTag::T_CaseExpr => deparse_case_expr(ctx, node.as_case_expr().unwrap()),
        NodeTag::T_ArrayExpr => deparse_array_expr(ctx, node.as_array_expr().unwrap()),
        NodeTag::T_Aggref => deparse_aggref(ctx, node.as_aggref().unwrap()),
        other => Err(Box::new(PgError::error(format!(
            "unsupported expression type for deparse: {other:?}"
        )))),
    }
}

fn deparse_var<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<()> {
    let var = node.as_var().unwrap();
    let (qualify_col, is_foreign) = {
        let relids = ctx.scanrel_relids();
        (
            types_pathnodes::relids::relids_num_members(relids) > 1,
            types_pathnodes::relids::relids_is_member(var.varno, relids) && var.varlevelsup == 0,
        )
    };

    if is_foreign {
        let rte = ctx.run.rte(var.varno as usize);
        deparse_column_ref(ctx, var.varno, var.varattno, rte, qualify_col)?;
    } else if ctx.params_list.is_some() {
        let pindex = param_index(ctx, node)?;
        print_remote_param(ctx, pindex, var.vartype, var.vartypmod)?;
    } else {
        print_remote_placeholder(ctx, var.vartype, var.vartypmod)?;
    }
    Ok(())
}

fn param_index<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<usize> {
    let list = ctx.params_list.as_mut().expect("param_index only with params_list");
    for (i, existing) in list.iter().enumerate() {
        if equal(node, *existing) {
            return Ok(i + 1);
        }
    }
    list.push(node);
    Ok(list.len())
}

fn deparse_const(ctx: &mut DeparseCtx<'_, '_>, node: &Const, showtype: i32) -> PgResult<()> {
    if node.constisnull {
        ctx.buf.push_str("NULL");
        if showtype >= 0 {
            let ty = deparse_type_name(node.consttype, node.consttypmod)?;
            let _ = write!(ctx.buf, "::{ty}");
        }
        return Ok(());
    }

    let (typoutput, _) = lsyscache::getTypeOutputInfo(node.consttype)?;
    let extval = output_function_call(ctx.mcx, typoutput, node.constvalue)?;
    let extval = extval.as_str();

    let mut isfloat = false;
    let mut isstring = false;

    match node.consttype {
        INT2OID | INT4OID | INT8OID | OIDOID | FLOAT4OID | FLOAT8OID | NUMERICOID => {
            if extval.bytes().all(|b| b"0123456789+-eE.".contains(&b)) {
                if extval.starts_with('+') || extval.starts_with('-') {
                    let _ = write!(ctx.buf, "({extval})");
                } else {
                    ctx.buf.push_str(extval);
                }
                if extval.bytes().any(|b| b"eE.".contains(&b)) {
                    isfloat = true;
                }
            } else {
                let _ = write!(ctx.buf, "'{extval}'");
            }
        }
        BITOID | VARBITOID => {
            let _ = write!(ctx.buf, "B'{extval}'");
        }
        BOOLOID => {
            ctx.buf.push_str(if extval == "t" { "true" } else { "false" });
        }
        _ => {
            deparse_string_literal(&mut ctx.buf, extval);
            isstring = true;
        }
    }

    if showtype == -1 {
        return Ok(());
    }

    let needlabel = match node.consttype {
        BOOLOID | INT4OID | UNKNOWNOID => false,
        NUMERICOID => !isfloat || node.consttypmod >= 0,
        _ => {
            if showtype == -2 {
                !isstring
            } else {
                true
            }
        }
    };
    if needlabel || showtype > 0 {
        let ty = deparse_type_name(node.consttype, node.consttypmod)?;
        let _ = write!(ctx.buf, "::{ty}");
    }
    Ok(())
}

fn deparse_param<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<()> {
    let p = node.as_param().unwrap();
    let (paramtype, paramtypmod) = (p.paramtype, p.paramtypmod);
    if ctx.params_list.is_some() {
        let pindex = param_index(ctx, node)?;
        print_remote_param(ctx, pindex, paramtype, paramtypmod)?;
    } else {
        print_remote_placeholder(ctx, paramtype, paramtypmod)?;
    }
    Ok(())
}

fn deparse_subscripting_ref<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    let sr = node.as_subscripting_ref().unwrap();
    ctx.buf.push('(');
    let refexpr = sr.refexpr.expect("SubscriptingRef refexpr");
    if refexpr.node_tag() == NodeTag::T_Var {
        deparse_expr(ctx, refexpr)?;
    } else {
        ctx.buf.push('(');
        deparse_expr(ctx, refexpr)?;
        ctx.buf.push(')');
    }
    let lower = &sr.reflowerindexpr;
    let has_lower = !lower.is_nil();
    for (i, up) in sr.refupperindexpr.iter().enumerate() {
        ctx.buf.push('[');
        if has_lower {
            if let Some(low) = lower.nth(i) {
                deparse_expr(ctx, low)?;
            }
            ctx.buf.push(':');
        }
        if let Some(up) = up {
            deparse_expr(ctx, up)?;
        }
        ctx.buf.push(']');
    }
    ctx.buf.push(')');
    Ok(())
}

fn deparse_func_expr<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<()> {
    let f = node.as_func_expr().unwrap();
    if f.funcformat == CoercionForm::COERCE_IMPLICIT_CAST {
        return deparse_expr(ctx, f.args.nth(0));
    }
    if f.funcformat == CoercionForm::COERCE_EXPLICIT_CAST {
        let rettype = f.funcresulttype;
        let coerced_typmod = expr_typmod(node);
        deparse_expr(ctx, f.args.nth(0))?;
        let ty = deparse_type_name(rettype, coerced_typmod)?;
        let _ = write!(ctx.buf, "::{ty}");
        return Ok(());
    }
    let node = f;
    let use_variadic = node.funcvariadic;
    append_function_name(ctx, node.funcid)?;
    ctx.buf.push('(');
    let n = node.args.len();
    for (i, arg) in node.args.iter().enumerate() {
        if i > 0 {
            ctx.buf.push_str(", ");
        }
        if use_variadic && i + 1 == n {
            ctx.buf.push_str("VARIADIC ");
        }
        deparse_expr(ctx, arg)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn is_plain_foreign_var(ctx: &DeparseCtx<'_, '_>, node: Node<'_>) -> bool {
    let node = if let Some(r) = node.as_relabel_type() {
        if r.relabelformat == CoercionForm::COERCE_IMPLICIT_CAST {
            r.arg
        } else {
            node
        }
    } else {
        node
    };
    if let Some(var) = node.as_var() {
        let relids = ctx.scanrel_relids();
        return types_pathnodes::relids::relids_is_member(var.varno, relids)
            && var.varlevelsup == 0;
    }
    false
}

fn deparse_op_expr<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<()> {
    let oe = node.as_op_expr().unwrap();
    let (oprname, oprnamespace) = operator_name_nsp(oe.opno)?;
    let nargs = oe.args.len();
    // oprkind 'b' iff binary (C asserts this matches list length).
    let binary = nargs == 2;

    ctx.buf.push('(');
    let mut can_suppress_right = false;
    if binary {
        let left = oe.args.nth(0);
        let right = oe.args.nth(1);
        let left_type = expr_type(left);
        let right_type = expr_type(right);
        let mut can_suppress_left = false;
        if left_type == right_type {
            if left.node_tag() == NodeTag::T_Const {
                can_suppress_left = is_plain_foreign_var(ctx, right);
            } else if right.node_tag() == NodeTag::T_Const {
                can_suppress_right = is_plain_foreign_var(ctx, left);
            }
        }
        if can_suppress_left {
            deparse_const(ctx, left.as_const().unwrap(), -2)?;
        } else {
            deparse_expr(ctx, left)?;
        }
        ctx.buf.push(' ');
    }
    deparse_operator_name(&mut ctx.buf, ctx.mcx, &oprname, oprnamespace)?;
    ctx.buf.push(' ');
    let right = oe.args.nth(nargs - 1);
    if can_suppress_right {
        deparse_const(ctx, right.as_const().unwrap(), -2)?;
    } else {
        deparse_expr(ctx, right)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn deparse_distinct_expr<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, node: Node<'mcx>) -> PgResult<()> {
    let de = node.as_distinct_expr().unwrap();
    debug_assert_eq!(de.args.len(), 2);
    ctx.buf.push('(');
    deparse_expr(ctx, de.args.nth(0))?;
    ctx.buf.push_str(" IS DISTINCT FROM ");
    deparse_expr(ctx, de.args.nth(1))?;
    ctx.buf.push(')');
    Ok(())
}

fn deparse_scalar_array_op_expr<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    let oe = node.as_scalar_array_op_expr().unwrap();
    let (oprname, oprnamespace) = operator_name_nsp(oe.opno)?;
    debug_assert_eq!(oe.args.len(), 2);
    ctx.buf.push('(');
    deparse_expr(ctx, oe.args.nth(0))?;
    ctx.buf.push(' ');
    deparse_operator_name(&mut ctx.buf, ctx.mcx, &oprname, oprnamespace)?;
    let _ = write!(ctx.buf, " {} (", if oe.useOr { "ANY" } else { "ALL" });
    deparse_expr(ctx, oe.args.nth(1))?;
    ctx.buf.push(')');
    ctx.buf.push(')');
    Ok(())
}

fn deparse_relabel_type<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: &types_nodes::RelabelType<'mcx>,
) -> PgResult<()> {
    deparse_expr(ctx, node.arg)?;
    if node.relabelformat != CoercionForm::COERCE_IMPLICIT_CAST {
        let ty = deparse_type_name(node.resulttype, node.resulttypmod)?;
        let _ = write!(ctx.buf, "::{ty}");
    }
    Ok(())
}

fn deparse_bool_expr<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: &types_nodes::BoolExpr<'mcx>,
) -> PgResult<()> {
    let op = match node.boolop {
        BoolExprType::AND_EXPR => "AND",
        BoolExprType::OR_EXPR => "OR",
        BoolExprType::NOT_EXPR => {
            ctx.buf.push_str("(NOT ");
            deparse_expr(ctx, node.args.nth(0))?;
            ctx.buf.push(')');
            return Ok(());
        }
    };
    ctx.buf.push('(');
    for (i, arg) in node.args.iter().enumerate() {
        if i > 0 {
            let _ = write!(ctx.buf, " {op} ");
        }
        deparse_expr(ctx, arg)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn deparse_null_test<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: &types_nodes::NullTest<'mcx>,
) -> PgResult<()> {
    ctx.buf.push('(');
    let arg = node.arg.expect("NullTest arg");
    deparse_expr(ctx, arg)?;
    let is_null = node.nulltesttype == NullTestType::IS_NULL;
    if node.argisrow || !lsyscache::type_is_rowtype(expr_type(arg))? {
        ctx.buf.push_str(if is_null { " IS NULL)" } else { " IS NOT NULL)" });
    } else {
        ctx.buf.push_str(if is_null {
            " IS NOT DISTINCT FROM NULL)"
        } else {
            " IS DISTINCT FROM NULL)"
        });
    }
    Ok(())
}

fn deparse_case_expr<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: &types_nodes::primnodes::CaseExpr<'mcx>,
) -> PgResult<()> {
    ctx.buf.push_str("(CASE");
    if let Some(arg) = node.arg {
        ctx.buf.push(' ');
        deparse_expr(ctx, arg)?;
    }
    for whennode in node.args.iter() {
        let cw = whennode.as_case_when().unwrap();
        ctx.buf.push_str(" WHEN ");
        let when_expr = cw.expr.expect("CaseWhen expr");
        if node.arg.is_none() {
            deparse_expr(ctx, when_expr)?;
        } else {
            let op = when_expr.as_op_expr().expect("CASE arg WHEN is OpExpr");
            deparse_expr(ctx, op.args.nth(1))?;
        }
        ctx.buf.push_str(" THEN ");
        deparse_expr(ctx, cw.result.expect("CaseWhen result"))?;
    }
    if let Some(defresult) = node.defresult {
        ctx.buf.push_str(" ELSE ");
        deparse_expr(ctx, defresult)?;
    }
    ctx.buf.push_str(" END)");
    Ok(())
}

fn deparse_array_expr<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: &types_nodes::ArrayExpr<'mcx>,
) -> PgResult<()> {
    ctx.buf.push_str("ARRAY[");
    for (i, elem) in node.elements.iter().enumerate() {
        if i > 0 {
            ctx.buf.push_str(", ");
        }
        deparse_expr(ctx, elem)?;
    }
    ctx.buf.push(']');
    if node.elements.is_nil() {
        let ty = deparse_type_name(node.array_typeid, -1)?;
        let _ = write!(ctx.buf, "::{ty}");
    }
    Ok(())
}

fn deparse_aggref<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    node: &'mcx types_nodes::primnodes::Aggref<'mcx>,
) -> PgResult<()> {
    debug_assert_eq!(node.aggsplit, types_nodes::primnodes::AGGSPLIT_SIMPLE);
    let use_variadic = node.aggvariadic;
    append_function_name(ctx, node.aggfnoid)?;
    ctx.buf.push('(');
    if !node.aggdistinct.is_nil() {
        ctx.buf.push_str("DISTINCT ");
    }
    let ordered_set = node.aggkind == types_nodes::primnodes::AGGKIND_ORDERED_SET
        || node.aggkind == types_nodes::primnodes::AGGKIND_HYPOTHETICAL;
    if ordered_set {
        for (i, arg) in node.aggdirectargs.iter().enumerate() {
            if i > 0 {
                ctx.buf.push_str(", ");
            }
            deparse_expr(ctx, arg)?;
        }
        ctx.buf.push_str(") WITHIN GROUP (ORDER BY ");
        append_agg_order_by(ctx, &node.aggorder, &node.args)?;
    } else {
        if node.aggstar {
            ctx.buf.push('*');
        } else {
            let n = node.args.len();
            let mut first = true;
            for (i, argnode) in node.args.iter().enumerate() {
                let tle = argnode.as_target_entry().unwrap();
                if tle.resjunk {
                    continue;
                }
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                if use_variadic && i + 1 == n {
                    ctx.buf.push_str("VARIADIC ");
                }
                deparse_expr(ctx, tle.expr)?;
            }
        }
        if !node.aggorder.is_nil() {
            ctx.buf.push_str(" ORDER BY ");
            append_agg_order_by(ctx, &node.aggorder, &node.args)?;
        }
    }
    if let Some(aggfilter) = node.aggfilter {
        ctx.buf.push_str(") FILTER (WHERE ");
        deparse_expr(ctx, aggfilter)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn append_agg_order_by<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    order_list: &types_nodes::list::NodeList<'mcx>,
    target_list: &'mcx types_nodes::list::NodeList<'mcx>,
) -> PgResult<()> {
    for (i, srtnode) in order_list.iter().enumerate() {
        let srt = srtnode
            .as_variant::<types_nodes::parsenodes::SortGroupClause>()
            .expect("aggorder holds SortGroupClause");
        if i > 0 {
            ctx.buf.push_str(", ");
        }
        let sortexpr = deparse_sort_group_clause(ctx, srt.tleSortGroupRef, target_list, false)?;
        append_order_by_suffix(ctx, srt.sortop, expr_type(sortexpr), srt.nulls_first)?;
    }
    Ok(())
}

fn append_order_by_suffix<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    sortop: Oid,
    sortcoltype: Oid,
    nulls_first: bool,
) -> PgResult<()> {
    let typentry = typcache::lookup_type_cache(
        sortcoltype,
        typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_GT_OPR,
    )?;
    if sortop == typentry.lt_opr() {
        ctx.buf.push_str(" ASC");
    } else if sortop == typentry.gt_opr() {
        ctx.buf.push_str(" DESC");
    } else {
        ctx.buf.push_str(" USING ");
        let (oprname, oprnamespace) = operator_name_nsp(sortop)?;
        deparse_operator_name(&mut ctx.buf, ctx.mcx, &oprname, oprnamespace)?;
    }
    ctx.buf.push_str(if nulls_first { " NULLS FIRST" } else { " NULLS LAST" });
    Ok(())
}

fn deparse_sort_group_clause<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    reference: u32,
    tlist: &'mcx types_nodes::list::NodeList<'mcx>,
    force_colno: bool,
) -> PgResult<Node<'mcx>> {
    let tle = get_sortgroupref_tle(reference, tlist);
    let expr = tle.expr;
    if force_colno {
        debug_assert!(!tle.resjunk);
        let _ = write!(ctx.buf, "{}", tle.resno);
    } else if expr.node_tag() == NodeTag::T_Const {
        deparse_const(ctx, expr.as_const().unwrap(), 1)?;
    } else if expr.node_tag() == NodeTag::T_Var {
        deparse_expr(ctx, expr)?;
    } else {
        ctx.buf.push('(');
        deparse_expr(ctx, expr)?;
        ctx.buf.push(')');
    }
    Ok(expr)
}

fn deparse_operator_name(
    buf: &mut PgString<'_>,
    mcx: Mcx<'_>,
    oprname: &str,
    oprnamespace: Oid,
) -> PgResult<()> {
    if oprnamespace != PG_CATALOG_NAMESPACE {
        let nsp = lsyscache::get_namespace_name(mcx, oprnamespace)?
            .expect("operator namespace exists");
        buf.push_str("OPERATOR(");
        append_quoted_identifier(buf, mcx, nsp.as_str())?;
        let _ = write!(buf, ".{oprname})");
    } else {
        buf.push_str(oprname);
    }
    Ok(())
}

fn print_remote_param<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    paramindex: usize,
    paramtype: Oid,
    paramtypmod: i32,
) -> PgResult<()> {
    let ptypename = deparse_type_name(paramtype, paramtypmod)?;
    let _ = write!(ctx.buf, "${paramindex}::{ptypename}");
    Ok(())
}

fn print_remote_placeholder<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    paramtype: Oid,
    paramtypmod: i32,
) -> PgResult<()> {
    let ptypename = deparse_type_name(paramtype, paramtypmod)?;
    let _ = write!(ctx.buf, "((SELECT null::{ptypename})::{ptypename})");
    Ok(())
}

fn append_function_name<'mcx>(ctx: &mut DeparseCtx<'_, 'mcx>, funcid: Oid) -> PgResult<()> {
    let pronamespace = lsyscache::function::get_func_namespace(funcid)?;
    if pronamespace != PG_CATALOG_NAMESPACE {
        let schema =
            lsyscache::get_namespace_name(ctx.mcx, pronamespace)?.expect("func namespace");
        append_quoted_identifier(&mut ctx.buf, ctx.mcx, schema.as_str())?;
        ctx.buf.push('.');
    }
    let proname = lsyscache::get_func_name(ctx.mcx, funcid)?.expect("func name");
    append_quoted_identifier(&mut ctx.buf, ctx.mcx, proname.as_str())?;
    Ok(())
}

fn operator_name_nsp(opno: Oid) -> PgResult<(String, Oid)> {
    let (name, nsp) = syscache_seams::pg_operator_oprnamensp::call(opno)?
        .expect("cache lookup for operator");
    // SAFETY: NameData is a NUL-padded server-encoding cstring.
    let s = name.name_str();
    let s = core::str::from_utf8(s).expect("operator name is UTF-8").to_string();
    Ok((s, nsp))
}

fn output_function_call<'mcx>(
    mcx: Mcx<'mcx>,
    typoutput: Oid,
    value: datum::Datum,
) -> PgResult<PgString<'mcx>> {
    let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let d = types_fmgr::function_call1_coll_in(&mut finfo, types_core::InvalidOid, mcx, value)?;
    // SAFETY: output functions return a NUL-terminated cstring datum; copied
    // out before finfo (and its scratch) dies.
    let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    PgString::from_str_in(s.to_str().expect("output fn result is UTF-8"), mcx)
}

// ---------- column / relation / target-list ----------

fn add_rel_qualifier(buf: &mut PgString<'_>, varno: i32) {
    let _ = write!(buf, "{REL_ALIAS_PREFIX}{varno}.");
}

fn deparse_column_ref<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    varno: i32,
    varattno: i16,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    qualify_col: bool,
) -> PgResult<()> {
    if varattno == SELF_ITEM_POINTER_ATTNUM {
        if qualify_col {
            add_rel_qualifier(&mut ctx.buf, varno);
        }
        ctx.buf.push_str("ctid");
    } else if varattno < 0 {
        let fetchval = if varattno == TABLE_OID_ATTNUM { rte.relid } else { 0 };
        if qualify_col {
            ctx.buf.push_str("CASE WHEN (");
            add_rel_qualifier(&mut ctx.buf, varno);
            let _ = write!(ctx.buf, "*)::text IS NOT NULL THEN {fetchval} END");
        } else {
            let _ = write!(ctx.buf, "{fetchval}");
        }
    } else if varattno == 0 {
        // Whole-row reference: ROW(columns) with an outer-join NULL guard.
        let rel = table::table_open(ctx.mcx, rte.relid, types_rel::lock::NoLock)?;
        if qualify_col {
            ctx.buf.push_str("CASE WHEN (");
            add_rel_qualifier(&mut ctx.buf, varno);
            ctx.buf.push_str("*)::text IS NOT NULL THEN ");
        }
        ctx.buf.push_str("ROW(");
        let mut attrs_used = types_nodes::Bitmapset::empty();
        attrs_used.add_member(ctx.mcx, 0 - FIRST_LOW_INVALID_HEAP_ATTNUM)?;
        let mut retrieved = PgVec::new_in(ctx.mcx);
        deparse_target_list(
            &mut ctx.buf,
            ctx.mcx,
            ctx.run,
            varno,
            &rel,
            rte,
            false,
            &attrs_used,
            qualify_col,
            &mut retrieved,
        )?;
        ctx.buf.push(')');
        if qualify_col {
            ctx.buf.push_str(" END");
        }
        table::table_close(rel, types_rel::lock::NoLock)?;
    } else {
        let mut colname: Option<String> = None;
        for opt in foreigncmds::foreign::GetForeignColumnOptions(ctx.mcx, rte.relid, varattno)?.iter()
        {
            if opt.name == "column_name" {
                colname = Some(opt.require_value()?.to_string());
                break;
            }
        }
        let colname = match colname {
            Some(c) => c,
            None => lsyscache::get_attname(ctx.mcx, rte.relid, varattno, false)?
                .expect("column exists")
                .as_str()
                .to_string(),
        };
        if qualify_col {
            add_rel_qualifier(&mut ctx.buf, varno);
        }
        append_quoted_identifier(&mut ctx.buf, ctx.mcx, &colname)?;
    }
    Ok(())
}

/// deparseRelation: schema.table with table_name/schema_name option overrides.
pub fn deparse_relation<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<()> {
    let table = foreigncmds::foreign::GetForeignTable(mcx, rel.rd_id)?;
    let mut nspname: Option<String> = None;
    let mut relname: Option<String> = None;
    for opt in table.options.iter() {
        if opt.name == "schema_name" {
            nspname = Some(opt.require_value()?.to_string());
        } else if opt.name == "table_name" {
            relname = Some(opt.require_value()?.to_string());
        }
    }
    let nspname = match nspname {
        Some(n) => n,
        None => lsyscache::get_namespace_name(mcx, rel.namespace())?
            .expect("namespace")
            .as_str()
            .to_string(),
    };
    let relname = match relname {
        Some(r) => r,
        None => rel.name().to_string(),
    };
    append_quoted_identifier(buf, mcx, &nspname)?;
    buf.push('.');
    append_quoted_identifier(buf, mcx, &relname)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn deparse_target_list<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    is_returning: bool,
    attrs_used: &types_nodes::Bitmapset<'mcx>,
    qualify_col: bool,
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<()> {
    let _ = run;
    let tupdesc = &rel.rd_att;
    let natts = tupdesc.natts as i32;
    let have_wholerow = attrs_used.is_member(0 - FIRST_LOW_INVALID_HEAP_ATTNUM);
    let mut first = true;
    for i in 1..=natts {
        let attr = tupdesc.attr(i as usize - 1);
        if attr.attisdropped {
            continue;
        }
        if have_wholerow || attrs_used.is_member(i - FIRST_LOW_INVALID_HEAP_ATTNUM) {
            if !first {
                buf.push_str(", ");
            } else if is_returning {
                buf.push_str(" RETURNING ");
            }
            first = false;
            deparse_column_ref_buf(buf, mcx, rtindex, i as i16, rte, qualify_col)?;
            retrieved_attrs.push(i);
        }
    }
    // ctid, if needed.
    if attrs_used.is_member(SELF_ITEM_POINTER_ATTNUM as i32 - FIRST_LOW_INVALID_HEAP_ATTNUM) {
        if !first {
            buf.push_str(", ");
        } else if is_returning {
            buf.push_str(" RETURNING ");
        }
        first = false;
        if qualify_col {
            add_rel_qualifier(buf, rtindex);
        }
        buf.push_str("ctid");
        retrieved_attrs.push(SELF_ITEM_POINTER_ATTNUM as i32);
    }
    if first && !is_returning {
        buf.push_str("NULL");
    }
    Ok(())
}

// deparse_column_ref without an expr context (targetlist path never emits
// params/whole-row-CASE beyond qualifiers): plain column reference only.
fn deparse_column_ref_buf<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    varno: i32,
    varattno: i16,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    qualify_col: bool,
) -> PgResult<()> {
    if varattno == SELF_ITEM_POINTER_ATTNUM {
        if qualify_col {
            add_rel_qualifier(buf, varno);
        }
        buf.push_str("ctid");
        return Ok(());
    }
    let mut colname: Option<String> = None;
    for opt in foreigncmds::foreign::GetForeignColumnOptions(mcx, rte.relid, varattno)?.iter() {
        if opt.name == "column_name" {
            colname = Some(opt.require_value()?.to_string());
            break;
        }
    }
    let colname = match colname {
        Some(c) => c,
        None => lsyscache::get_attname(mcx, rte.relid, varattno, false)?
            .expect("column exists")
            .as_str()
            .to_string(),
    };
    if qualify_col {
        add_rel_qualifier(buf, varno);
    }
    append_quoted_identifier(buf, mcx, &colname)
}

// ---------- SELECT-statement construction (base relation) ----------

/// deparseSelectStmtForRel for a base (simple) foreign relation. Join/upper
/// deparse is phase 2 (needs the join/grouping planner arms); reaching it here
/// is a loud panic.
pub fn deparse_select_stmt_for_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    rel: RelId,
    remote_conds: &[types_pathnodes::RinfoId],
    params_list: Option<PgVec<'mcx, Node<'mcx>>>,
) -> PgResult<(PgString<'mcx>, PgVec<'mcx, i32>, Option<PgVec<'mcx, Node<'mcx>>>)> {
    let reloptkind = run.root.rel(rel).reloptkind;
    if !matches!(reloptkind, types_pathnodes::RELOPT_BASEREL | types_pathnodes::RELOPT_OTHER_MEMBER_REL)
    {
        panic!(
            "postgres_fdw: join/upper-relation deparse is phase 2 (reloptkind {reloptkind:?})"
        );
    }
    let mcx = run.mcx;
    let mut ctx = DeparseCtx {
        run,
        foreignrel: rel,
        scanrel: rel,
        buf: PgString::new_in(mcx),
        params_list,
        mcx,
    };
    let mut retrieved_attrs = PgVec::new_in(mcx);

    // SELECT clause.
    ctx.buf.push_str("SELECT ");
    {
        let fp = fpinfo(run.root.rel(rel)).borrow();
        let rte = run.rte(run.root.rel(rel).relid as usize);
        let opened = table::table_open(mcx, rte.relid, types_rel::lock::NoLock)?;
        let mut buf = core::mem::replace(&mut ctx.buf, PgString::new_in(mcx));
        deparse_target_list(
            &mut buf, mcx, run, run.root.rel(rel).relid as i32, &opened, rte, false,
            &fp.attrs_used, false, &mut retrieved_attrs,
        )?;
        drop(fp);
        ctx.buf = buf;
        table::table_close(opened, types_rel::lock::NoLock)?;
    }

    // FROM + WHERE.
    ctx.buf.push_str(" FROM ");
    {
        let rte = run.rte(run.root.rel(rel).relid as usize);
        let opened = table::table_open(mcx, rte.relid, types_rel::lock::NoLock)?;
        let mut buf = core::mem::replace(&mut ctx.buf, PgString::new_in(mcx));
        deparse_relation(&mut buf, mcx, &opened)?;
        ctx.buf = buf;
        table::table_close(opened, types_rel::lock::NoLock)?;
    }
    append_where_clause(&mut ctx, remote_conds)?;

    Ok((ctx.buf, retrieved_attrs, ctx.params_list))
}

fn append_where_clause<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    exprs: &[types_pathnodes::RinfoId],
) -> PgResult<()> {
    if !exprs.is_empty() {
        ctx.buf.push_str(" WHERE ");
        append_conditions(ctx, exprs)?;
    }
    Ok(())
}

fn append_conditions<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    exprs: &[types_pathnodes::RinfoId],
) -> PgResult<()> {
    let nestlevel = crate::transmission::set_transmission_modes();
    for (i, &ri) in exprs.iter().enumerate() {
        let clause = ctx.run.root.rinfo(ri).clause;
        let expr = *ctx.run.root.expr_node(clause);
        if i > 0 {
            ctx.buf.push_str(" AND ");
        }
        ctx.buf.push('(');
        deparse_expr(ctx, expr)?;
        ctx.buf.push(')');
    }
    crate::transmission::reset_transmission_modes(nestlevel);
    Ok(())
}

// ---------- DML deparse (INSERT / UPDATE / DELETE + direct modify) ----------
//
// These are the plan-time deparse entry points that postgresPlanForeignModify /
// postgresPlanDirectModify call in C (deparse.c:2081-2445). They are ported and
// self-contained here; the executor + planner wiring that CALLS them is the
// phase-4 DML-executor substrate (FdwModifyRoutine seam + createplan
// PlanForeignModify + preptlist AddForeignUpdateTargets + the nodemodifytable /
// nodeforeignscan branches). See notes/contrib-pgfdw-p3.md for the blueprint.
// `rebuild_insert_sql` is the one exec-time deparse (batch-size re-expansion).

/// deparseReturningList: append a RETURNING clause (if any), collecting the
/// attnums retrieved by WITH CHECK OPTION or RETURNING into `retrieved_attrs`.
/// `trig_after_row` = the target relation has an AFTER ROW trigger for the op,
/// which forces a whole-row retrieval (C: bms_make_singleton).
#[allow(clippy::too_many_arguments)]
pub fn deparse_returning_list<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    trig_after_row: bool,
    wco_list: &[Node<'mcx>],
    returning_list: &[Node<'mcx>],
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<()> {
    let mut attrs_used = types_nodes::Bitmapset::empty();
    if trig_after_row {
        // whole-row reference acquires all non-system columns.
        attrs_used.add_member(mcx, 0 - FIRST_LOW_INVALID_HEAP_ATTNUM)?;
    }
    for &node in wco_list {
        vars::pull_varattnos(mcx, node, rtindex, &mut attrs_used)?;
    }
    for &node in returning_list {
        vars::pull_varattnos(mcx, node, rtindex, &mut attrs_used)?;
    }
    if !attrs_used.is_empty() {
        deparse_target_list(
            buf, mcx, run, rtindex, rel, rte, true, &attrs_used, false, retrieved_attrs,
        )?;
    }
    // else: *retrieved_attrs stays NIL (empty), matching C.
    Ok(())
}

/// deparseInsertSql. Appends
///   INSERT INTO rel (cols...) VALUES ($1, $2, DEFAULT, ...) [ON CONFLICT DO
///   NOTHING] [RETURNING ...]
/// Generated columns emit DEFAULT (no param). Returns `values_end_len`, the
/// byte offset of the end of the first row's VALUES clause (batch-insert reuses
/// it via `rebuild_insert_sql`).
#[allow(clippy::too_many_arguments)]
pub fn deparse_insert_sql<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    target_attrs: &[i32],
    do_nothing: bool,
    trig_after_row: bool,
    wco_list: &[Node<'mcx>],
    returning_list: &[Node<'mcx>],
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<i32> {
    let tupdesc = &rel.rd_att;
    buf.push_str("INSERT INTO ");
    deparse_relation(buf, mcx, rel)?;

    if !target_attrs.is_empty() {
        buf.push('(');
        let mut first = true;
        for &attnum in target_attrs {
            if !first {
                buf.push_str(", ");
            }
            first = false;
            deparse_column_ref_buf(buf, mcx, rtindex, attnum as i16, rte, false)?;
        }
        buf.push_str(") VALUES (");

        let mut pindex = 1;
        let mut first = true;
        for &attnum in target_attrs {
            let attr = tupdesc.attr(attnum as usize - 1);
            if !first {
                buf.push_str(", ");
            }
            first = false;
            if attr.attgenerated != 0 {
                buf.push_str("DEFAULT");
            } else {
                let _ = write!(buf, "${pindex}");
                pindex += 1;
            }
        }
        buf.push(')');
    } else {
        buf.push_str(" DEFAULT VALUES");
    }
    let values_end_len = buf.as_str().len() as i32;

    if do_nothing {
        buf.push_str(" ON CONFLICT DO NOTHING");
    }

    deparse_returning_list(
        buf, mcx, run, rte, rtindex, rel, trig_after_row, wco_list, returning_list,
        retrieved_attrs,
    )?;
    Ok(values_end_len)
}

/// rebuildInsertSql: given a single-row INSERT template (`orig_query`) and its
/// `values_end_len`, rebuild an INSERT with `num_rows` VALUES tuples for batch
/// insert. `num_params` = params already emitted for the first row. Exec-time.
pub fn rebuild_insert_sql<'mcx>(
    buf: &mut PgString<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    orig_query: &str,
    target_attrs: &[i32],
    values_end_len: i32,
    num_params: i32,
    num_rows: i32,
) {
    let tupdesc = &rel.rd_att;
    let end = values_end_len as usize;
    debug_assert!(end > 0 && end <= orig_query.len());
    // Copy up to the end of the first record from the original query.
    buf.push_str(&orig_query[..end]);

    // Add the extra rows; params for the first row already exist, so continue.
    let mut pindex = num_params + 1;
    for _ in 0..num_rows {
        buf.push_str(", (");
        let mut first = true;
        for &attnum in target_attrs {
            let attr = tupdesc.attr(attnum as usize - 1);
            if !first {
                buf.push_str(", ");
            }
            first = false;
            if attr.attgenerated != 0 {
                buf.push_str("DEFAULT");
            } else {
                let _ = write!(buf, "${pindex}");
                pindex += 1;
            }
        }
        buf.push(')');
    }
    // Copy the stuff after the VALUES clause (RETURNING, ON CONFLICT, ...).
    buf.push_str(&orig_query[end..]);
}

/// deparseUpdateSql: UPDATE rel SET col = $n, gen = DEFAULT, ... WHERE ctid = $1
/// [RETURNING ...]. ctid is always param $1; SET params start at $2.
#[allow(clippy::too_many_arguments)]
pub fn deparse_update_sql<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    target_attrs: &[i32],
    trig_after_row: bool,
    wco_list: &[Node<'mcx>],
    returning_list: &[Node<'mcx>],
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<()> {
    let tupdesc = &rel.rd_att;
    buf.push_str("UPDATE ");
    deparse_relation(buf, mcx, rel)?;
    buf.push_str(" SET ");

    let mut pindex = 2; // ctid is always the first param
    let mut first = true;
    for &attnum in target_attrs {
        let attr = tupdesc.attr(attnum as usize - 1);
        if !first {
            buf.push_str(", ");
        }
        first = false;
        deparse_column_ref_buf(buf, mcx, rtindex, attnum as i16, rte, false)?;
        if attr.attgenerated != 0 {
            buf.push_str(" = DEFAULT");
        } else {
            let _ = write!(buf, " = ${pindex}");
            pindex += 1;
        }
    }
    buf.push_str(" WHERE ctid = $1");

    deparse_returning_list(
        buf, mcx, run, rte, rtindex, rel, trig_after_row, wco_list, returning_list,
        retrieved_attrs,
    )
}

/// deparseDeleteSql: DELETE FROM rel WHERE ctid = $1 [RETURNING ...].
#[allow(clippy::too_many_arguments)]
pub fn deparse_delete_sql<'mcx>(
    buf: &mut PgString<'mcx>,
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    trig_after_row: bool,
    returning_list: &[Node<'mcx>],
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<()> {
    buf.push_str("DELETE FROM ");
    deparse_relation(buf, mcx, rel)?;
    buf.push_str(" WHERE ctid = $1");

    deparse_returning_list(
        buf, mcx, run, rte, rtindex, rel, trig_after_row, &[], returning_list, retrieved_attrs,
    )
}

/// deparseDirectUpdateSql, base-relation case only. The join case
/// (foreignrel is a RELOPT_JOINREL, needing deparseFromExprForRel + the
/// FROM/alias machinery) is phase-4 join pushdown; reaching it raises a loud
/// FEATURE_NOT_SUPPORTED.
#[allow(clippy::too_many_arguments)]
pub fn deparse_direct_update_sql<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    targetlist: &[Node<'mcx>],
    target_attrs: &[i32],
    remote_conds: &[types_pathnodes::RinfoId],
    returning_list: &[Node<'mcx>],
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<()> {
    if ctx.run.root.rel(ctx.foreignrel).reloptkind == types_pathnodes::RELOPT_JOINREL {
        return Err(direct_modify_join_unported());
    }
    let mcx = ctx.mcx;
    ctx.buf.push_str("UPDATE ");
    deparse_relation(&mut ctx.buf, mcx, rel)?;
    ctx.buf.push_str(" SET ");

    // Make sure any constants in the exprs are printed portably.
    let nestlevel = crate::transmission::set_transmission_modes();
    let mut first = true;
    for (i, &tle_node) in targetlist.iter().enumerate() {
        let tle = tle_node.as_target_entry().expect("direct-update tlist is TargetEntry");
        let attnum = target_attrs[i];
        debug_assert!(!tle.resjunk);
        if !first {
            ctx.buf.push_str(", ");
        }
        first = false;
        deparse_column_ref_buf(&mut ctx.buf, mcx, rtindex, attnum as i16, rte, false)?;
        ctx.buf.push_str(" = ");
        deparse_expr(ctx, tle.expr)?;
    }
    crate::transmission::reset_transmission_modes(nestlevel);

    // base-rel: no FROM clause, additional_conds is NIL.
    append_where_clause(ctx, remote_conds)?;

    deparse_returning_list(
        &mut ctx.buf, mcx, ctx.run, rte, rtindex, rel, false, &[], returning_list,
        retrieved_attrs,
    )
}

/// deparseDirectDeleteSql, base-relation case only (join case is phase-4).
#[allow(clippy::too_many_arguments)]
pub fn deparse_direct_delete_sql<'mcx>(
    ctx: &mut DeparseCtx<'_, 'mcx>,
    rtindex: i32,
    rel: &types_rel::Relation<'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    remote_conds: &[types_pathnodes::RinfoId],
    returning_list: &[Node<'mcx>],
    retrieved_attrs: &mut PgVec<'mcx, i32>,
) -> PgResult<()> {
    if ctx.run.root.rel(ctx.foreignrel).reloptkind == types_pathnodes::RELOPT_JOINREL {
        return Err(direct_modify_join_unported());
    }
    let mcx = ctx.mcx;
    ctx.buf.push_str("DELETE FROM ");
    deparse_relation(&mut ctx.buf, mcx, rel)?;

    // base-rel: no USING clause, additional_conds is NIL.
    append_where_clause(ctx, remote_conds)?;

    deparse_returning_list(
        &mut ctx.buf, mcx, ctx.run, rte, rtindex, rel, false, &[], returning_list,
        retrieved_attrs,
    )
}

#[track_caller]
#[cold]
fn direct_modify_join_unported() -> Box<PgError> {
    Box::new(
        PgError::error(
            "postgres_fdw: direct modify over a foreign join is phase-4 join pushdown",
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literal_escaping() {
        let mcx = mcx::MemoryContext::new("t");
        let m = mcx.mcx();
        let mut b = PgString::new_in(m);
        deparse_string_literal(&mut b, "abc");
        assert_eq!(b.as_str(), "'abc'");

        let mut b = PgString::new_in(m);
        deparse_string_literal(&mut b, "a'b");
        assert_eq!(b.as_str(), "'a''b'");

        let mut b = PgString::new_in(m);
        deparse_string_literal(&mut b, "a\\b");
        assert_eq!(b.as_str(), "E'a\\\\b'");
    }

    #[test]
    fn jointype_names() {
        assert_eq!(get_jointype_name(JoinType::JOIN_INNER), "INNER");
        assert_eq!(get_jointype_name(JoinType::JOIN_LEFT), "LEFT");
        assert_eq!(get_jointype_name(JoinType::JOIN_RIGHT), "RIGHT");
        assert_eq!(get_jointype_name(JoinType::JOIN_FULL), "FULL");
        assert_eq!(get_jointype_name(JoinType::JOIN_SEMI), "SEMI");
    }
}
