//! `_read<Type>` readers for the read_plan_family node arms (the plannodes.h
//! plan / scan / join node family). Each reader reads its fields in the EXACT
//! order the OUT side (`out_plan_family`) wrote them, keeping the byte-stable
//! round-trip property. `try_read` returns `Some(result)` iff this family owns
//! `label`.
//!
//! Symmetry with `out_plan_family`: a node the OUT side `mirror-pg-and-panic`s
//! (ModifyTable / WindowAgg / TableFuncScan / SampleScan / CustomScan — a
//! trimmed supertype field or a child sub-struct not reachable as a `Node`) is
//! likewise not read here (those labels fall through to the C
//! `elog(ERROR, "badly formatted node string ...")` tail, which is the explicit
//! signal). Every node the OUT side serializes is read back here.

use alloc::vec::Vec;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_error::PgResult;
use ::nodes::jointype::{Join, JoinType};
use ::nodes::nodeindexscan::{Plan, Scan};
use ::nodes::nodes::Node;
use ::nodes::primnodes::{Expr, TargetEntry};

use nodes_core::read::{self, Token};

use crate::{
    elog_error, read_bitmapset_opt_field, read_bool_field, read_enum_field, read_float_field,
    read_int64_field, read_int_field, read_oid_field, read_uint64_field, read_uint_field, tok_str,
};

/// `READ_LONG_FIELD` — `atol` (mirrors lib.rs's `read_long_field`).
fn read_long_field() -> PgResult<i64> {
    read_int64_field()
}

// ---------------------------------------------------------------------------
// Local low-level helpers (the lib.rs `read_string_field` / `read_expr_list_field`
// / `read_opt_expr_field` are private; this family re-derives the few it needs,
// plus the scalar-array readers, off the shared `pg_strtok` cursor).
// ---------------------------------------------------------------------------

fn next_tok<'a>() -> PgResult<Token<'a>> {
    read::pg_strtok().ok_or_else(|| elog_error("unexpected end of node string"))
}

/// `READ_STRING_FIELD` via `nullable_string` (`<>` = NULL/None; `""` = empty;
/// else debackslash).
fn read_string<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let _label = next_tok()?;
    let v = next_tok()?;
    if v.bytes.is_empty() {
        return Ok(None);
    }
    if v.bytes == b"\"\"" {
        return Ok(Some(mcx::PgString::from_str_in("", mcx)?));
    }
    let s = read::debackslash(v.bytes);
    Ok(Some(mcx::PgString::from_str_in(&s, mcx)?))
}

/// `READ_NODE_FIELD` over a single child `Node *` (`<>` = NULL/None).
fn read_node_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    let _label = next_tok()?;
    read::node_read(mcx, None)
}

/// `READ_NODE_FIELD` over a `List *` of `Node`, returning the elements as a
/// `Vec<Node>` (the repo carries some plan-child lists as a bare `Vec<Node>`).
/// `<>` (NIL) → empty Vec.
fn read_node_vec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<Node<'mcx>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(Vec::new()),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = Vec::with_capacity(elems.len());
                for c in elems {
                    out.push(PgBox::into_inner(c));
                }
                Ok(out)
            },
                None => Err(elog_error(alloc::format!(
                "expected List for node-list field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` over a `List *` of `Node`, as an `Option<PgVec<Node>>`
/// (NIL → None).
fn read_node_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    out.push(PgBox::into_inner(c));
                }
                Ok(Some(out))
            },
                None => Err(elog_error(alloc::format!(
                "expected List for node-list field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` over a `List *` of `Node`, as `Option<PgVec<NodePtr>>`
/// (each cell a boxed `Node`). NIL → None.
fn read_node_box_pgvec_opt<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    out.push(c);
                }
                Ok(Some(out))
            },
                None => Err(elog_error(alloc::format!(
                "expected List for node-list field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` over a `List *` of `Expr`, as an `Option<PgVec<Expr>>`
/// (NIL → None). Mirrors a `WRITE_NODE_FIELD` of an expr list.
fn read_expr_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    {
            let __n = PgBox::into_inner(c);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(e),
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected Expr in expr list, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Ok(Some(out))
            },
                None => Err(elog_error(alloc::format!(
                "expected List for expr-list field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// A non-optional `PgVec<Expr>` (the repo carries a few clause lists this way).
fn read_expr_pgvec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, Expr>> {
    Ok(read_expr_pgvec_opt(mcx)?.unwrap_or_else(|| PgVec::new_in(mcx)))
}

/// A non-optional plain `alloc::Vec<Expr>` (e.g. `MergeJoin.mergeclauses`).
fn read_expr_alloc_vec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<Expr>> {
    let v = read_expr_pgvec_opt(mcx)?;
    Ok(match v {
        None => Vec::new(),
        Some(pv) => pv.into_iter().collect(),
    })
}

/// `READ_NODE_FIELD` over a `List *` of `TargetEntry`, as `Option<PgVec<TargetEntry>>`
/// (NIL → None).
fn read_te_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    {
            let __n = PgBox::into_inner(c);
            let __tag = __n.node_tag();
            match __n.into_targetentry() {
                Some(te) => out.push(te),
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected TargetEntry in tlist, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Ok(Some(out))
            },
                None => Err(elog_error(alloc::format!(
                "expected List for tlist field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// Single optional child `Expr` (`<>` → None), boxed (`Option<PgBox<Expr>>`).
fn read_expr_box_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgBox<'mcx, Expr>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => Ok(Some(alloc_in(mcx, e)?)),
                None => Err(elog_error(alloc::format!(
                "expected Expr child, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_INT_LIST` returning a `PgVec<i32>` (`(i v0 v1 ...)` form; `<>`/empty →
/// empty Vec). Mirrors `read_expr_family::read_int_list_field` but allocates the
/// repo's `PgVec<i32>` SubPlan carrier directly.
fn read_int_list_pgvec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, i32>> {
    let _label = next_tok()?;
    let open = next_tok()?;
    let mut out = PgVec::new_in(mcx);
    if open.bytes.is_empty() {
        return Ok(out);
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for int list"));
    }
    let tag = next_tok()?;
    if tag.bytes != b"i" && tag.bytes != b")" {
        return Err(elog_error("expected 'i' for int list"));
    }
    if tag.bytes == b")" {
        return Ok(out);
    }
    loop {
        let t = next_tok()?;
        if t.bytes == b")" {
            break;
        }
        out.push(crate::atoi_i64(&tok_str(&t)) as i32);
    }
    Ok(out)
}

/// `READ_NODE_FIELD` over the `:args` list of `Expr` written by
/// `write_pgbox_expr_list_field` (`(child child ...)`; empty → `()`); returns
/// `PgVec<PgBox<Expr>>`.
fn read_expr_box_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, Expr>>> {
    let _label = next_tok()?;
    let mut out = PgVec::new_in(mcx);
    match read::node_read(mcx, None)? {
        None => Ok(out),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                for c in elems {
                    {
            let __n = PgBox::into_inner(c);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(alloc_in(mcx, e)?),
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected Expr in args list, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Ok(out)
            },
                None => Err(elog_error(alloc::format!(
                "expected List for args field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `_readSubPlan` (readfuncs.funcs.c) — reads the `{SUBPLAN ...}` body fields in
/// the exact order `out_expr_family::out_subplan` writes them, reconstructing the
/// `'mcx`-carrying `SubPlan<'mcx>` (the `Plan.initPlan` element type; distinct
/// from the lifetime-free `Expr::SubPlan` carrier). The opening `{`/LABEL is
/// consumed by the caller.
pub(crate) fn read_subplan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::primnodes::SubPlan<'mcx>> {
    let subLinkType = crate::read_expr_family::sublink_type_from(read_enum_field()?);
    let testexpr = read_expr_box_opt(mcx)?;
    let paramIds = read_int_list_pgvec(mcx)?;
    let plan_id = read_int_field()?;
    let plan_name = read_string(mcx)?;
    let firstColType = read_oid_field()?;
    let firstColTypmod = read_int_field()?;
    let firstColCollation = read_oid_field()?;
    let useHashTable = read_bool_field()?;
    let unknownEqFalse = read_bool_field()?;
    let parallel_safe = read_bool_field()?;
    let setParam = read_int_list_pgvec(mcx)?;
    let parParam = read_int_list_pgvec(mcx)?;
    let args = read_expr_box_pgvec(mcx)?;
    let startup_cost = read_float_field()?;
    let per_call_cost = read_float_field()?;
    Ok(::nodes::primnodes::SubPlan {
        subLinkType,
        testexpr,
        paramIds,
        plan_id,
        plan_name,
        firstColType,
        firstColTypmod,
        firstColCollation,
        useHashTable,
        unknownEqFalse,
        parallel_safe,
        setParam,
        parParam,
        args,
        startup_cost,
        per_call_cost,
    })
}

/// Read the `:initPlan` field: the `<>` NULL form → `None`, or a
/// `({SUBPLAN ...} ...)` list of framed SubPlan bodies → `Some(PgVec<SubPlan>)`.
/// Mirrors `out_plan`'s initPlan emission exactly (each element framed by `{`/`}`
/// and read by `read_subplan`).
fn read_initplan_list<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, ::nodes::primnodes::SubPlan<'mcx>>>> {
    let _label = next_tok()?; // :initPlan
    let open = next_tok()?;
    if open.bytes.is_empty() {
        // `<>` — C NIL.
        return Ok(None);
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' or '<>' for initPlan list"));
    }
    let mut out = PgVec::new_in(mcx);
    loop {
        let t = next_tok()?;
        if t.bytes == b")" {
            break;
        }
        // Each element is a framed `{SUBPLAN ...}`. `t` is the opening `{`.
        if t.bytes.first() != Some(&b'{') {
            return Err(elog_error("expected '{' opening a SubPlan in initPlan"));
        }
        let label = next_tok()?;
        if label.bytes != b"SUBPLAN" {
            return Err(elog_error("expected SUBPLAN label in initPlan list"));
        }
        out.push(read_subplan(mcx)?);
        let close = next_tok()?;
        if close.bytes.first() != Some(&b'}') {
            return Err(elog_error("did not find '}' at end of SubPlan in initPlan"));
        }
    }
    Ok(Some(out))
}

/// `_readAlternativeSubPlan` (readfuncs.funcs.c) — reads the `{ALTERNATIVESUBPLAN
/// ...}` body. `out_alternative_subplan` writes a single `:subplans (` list of
/// framed `{SUBPLAN ...}` bodies `)`. The opening `{`/LABEL is consumed by the
/// caller; this reads the `:subplans` field into a `Vec<PgBox<SubPlan>>`.
pub(crate) fn read_alternative_subplan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::primnodes::AlternativeSubPlan<'mcx>> {
    let _label = next_tok()?; // :subplans
    let open = next_tok()?;
    let mut subplans: alloc::vec::Vec<PgBox<'mcx, ::nodes::primnodes::SubPlan<'mcx>>> =
        alloc::vec::Vec::new();
    if open.bytes.is_empty() {
        // `<>` — C NIL (an empty AlternativeSubPlan is not expected, but mirror
        // the NIL form defensively).
        return Ok(::nodes::primnodes::AlternativeSubPlan { subplans });
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for AlternativeSubPlan subplans list"));
    }
    loop {
        let t = next_tok()?;
        if t.bytes == b")" {
            break;
        }
        // Each element is a framed `{SUBPLAN ...}`. `t` is the opening `{`.
        if t.bytes.first() != Some(&b'{') {
            return Err(elog_error("expected '{' opening a SubPlan in AlternativeSubPlan"));
        }
        let label = next_tok()?;
        if label.bytes != b"SUBPLAN" {
            return Err(elog_error("expected SUBPLAN label in AlternativeSubPlan list"));
        }
        let sp = read_subplan(mcx)?;
        subplans.push(alloc_in(mcx, sp)?);
        let close = next_tok()?;
        if close.bytes.first() != Some(&b'}') {
            return Err(elog_error("did not find '}' at end of SubPlan in AlternativeSubPlan"));
        }
    }
    Ok(::nodes::primnodes::AlternativeSubPlan { subplans })
}

/// `readXxxCols`: read the `:fldname` label, then a `( v0 v1 ...)` token run of
/// `n` values (or `<>` for a NULL array, returning an empty Vec). `parse` maps
/// the token text to `T`.
fn read_scalar_cols<T>(n: usize, mut parse: impl FnMut(&str) -> T) -> PgResult<Vec<T>> {
    let _label = next_tok()?;
    let open = next_tok()?;
    if open.bytes.is_empty() {
        // it was "<>" → NULL → empty.
        return Ok(Vec::new());
    }
    if open.bytes != b"(" {
        return Err(elog_error("unrecognized token: expected '(' for scalar array"));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let t = next_tok()?;
        if t.bytes == b")" {
            return Err(elog_error("incomplete scalar array"));
        }
        out.push(parse(&tok_str(&t)));
    }
    let close = next_tok()?;
    if close.bytes != b")" {
        return Err(elog_error("incomplete scalar array (missing ')')"));
    }
    Ok(out)
}

fn read_attrnumber_cols(n: usize) -> PgResult<Vec<i16>> {
    read_scalar_cols(n, |s| crate::atoi_i64(s) as i16)
}
fn read_oid_cols(n: usize) -> PgResult<Vec<u32>> {
    read_scalar_cols(n, |s| crate::atoui_u64(s) as u32)
}
fn read_bool_cols(n: usize) -> PgResult<Vec<bool>> {
    read_scalar_cols(n, |s| s.as_bytes().first() == Some(&b't'))
}

/// `WRITE_NODE_FIELD` of a `List *` of `Oid` (the `(o v1 v2 ...)` OidList form).
/// `read.c`'s `node_read` rejects a top-level scalar `(o ...)` list (it is a
/// typed sub-field), so parse it directly off the shared cursor like C's
/// `_readOidList`: `<>` → None, else `(`, `o`, values until `)`.
fn read_oidlist_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, u32>>> {
    let _label = next_tok()?;
    let open = next_tok()?;
    if open.bytes.is_empty() {
        return Ok(None); // it was `<>` → NIL
    }
    if open.bytes != b"(" {
        return Err(elog_error("unrecognized token: expected '(' for OidList"));
    }
    let disc = next_tok()?;
    if disc.bytes != b"o" {
        return Err(elog_error("unrecognized token: expected 'o' for OidList"));
    }
    let mut out = PgVec::new_in(mcx);
    loop {
        let t = next_tok()?;
        if t.bytes == b")" {
            break;
        }
        out.push(crate::atoui_u64(&tok_str(&t)) as u32);
    }
    Ok(Some(out))
}

/// A non-optional `PgVec<Oid>` OidList (`(o ...)`; NIL → empty).
fn read_oidlist<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u32>> {
    Ok(read_oidlist_opt(mcx)?.unwrap_or_else(|| PgVec::new_in(mcx)))
}

// ---------------------------------------------------------------------------
// Flattened supertype readers (read the fields the out_plan_family emitters
// wrote, in order).
// ---------------------------------------------------------------------------

/// Read the flattened `Plan` supertype (the fields `out_plan_fields` wrote).
fn read_plan_fields<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Plan<'mcx>> {
    let disabled_nodes = read_int_field()?;
    let startup_cost = read_float_field()?;
    let total_cost = read_float_field()?;
    let plan_rows = read_float_field()?;
    let plan_width = read_int_field()?;
    let parallel_aware = read_bool_field()?;
    let parallel_safe = read_bool_field()?;
    let async_capable = read_bool_field()?;
    let plan_node_id = read_int_field()?;
    let targetlist = read_te_pgvec_opt(mcx)?;
    let qual = read_expr_pgvec_opt(mcx)?;
    let lefttree = read_node_opt(mcx)?;
    let righttree = read_node_opt(mcx)?;
    // initPlan: a `List *` of `SubPlan`, written by out_plan as `<>` (None) or
    // `({SUBPLAN ...} {SUBPLAN ...} ...)`. Each element is a framed `{SUBPLAN}`
    // body read by `read_subplan` into a `'mcx`-carrying `SubPlan<'mcx>` (this is
    // NOT routed through the central `node_read`/Expr::SubPlan dispatch, whose
    // `'static` SubPlanExpr carrier is a separate seam-blocked path).
    let initPlan = read_initplan_list(mcx)?;
    let extParam = read_bitmapset_opt_field(mcx)?;
    let allParam = read_bitmapset_opt_field(mcx)?;
    Ok(Plan {
        disabled_nodes,
        startup_cost,
        total_cost,
        plan_rows,
        plan_width,
        parallel_aware,
        parallel_safe,
        async_capable,
        plan_node_id,
        targetlist,
        qual,
        initPlan,
        lefttree,
        righttree,
        extParam,
        allParam,
    })
}

/// Read the flattened `Scan` supertype: the plan fields then `scanrelid`.
fn read_scan_fields<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Scan<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let scanrelid = read_uint_field()?;
    Ok(Scan { plan, scanrelid })
}

/// Read the flattened `Join` supertype: the plan fields then
/// `jointype`/`inner_unique`/`joinqual`.
fn read_join_fields<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Join<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let jointype = join_type_from(read_enum_field()?);
    let inner_unique = read_bool_field()?;
    let joinqual = read_expr_pgvec_opt(mcx)?;
    Ok(Join {
        plan,
        jointype,
        inner_unique,
        joinqual,
    })
}

fn join_type_from(code: i32) -> JoinType {
    match code {
        0 => JoinType::JOIN_INNER,
        1 => JoinType::JOIN_LEFT,
        2 => JoinType::JOIN_FULL,
        3 => JoinType::JOIN_RIGHT,
        4 => JoinType::JOIN_SEMI,
        5 => JoinType::JOIN_ANTI,
        6 => JoinType::JOIN_RIGHT_SEMI,
        7 => JoinType::JOIN_RIGHT_ANTI,
        8 => JoinType::JOIN_UNIQUE_OUTER,
        _ => JoinType::JOIN_UNIQUE_INNER,
    }
}

fn scan_dir_from(code: i32) -> ::nodes::execnodes::ScanDirection {
    use ::nodes::execnodes::ScanDirection;
    match code {
        -1 => ScanDirection::BackwardScanDirection,
        1 => ScanDirection::ForwardScanDirection,
        _ => ScanDirection::NoMovementScanDirection,
    }
}

fn agg_strategy_from(code: i32) -> ::nodes::nodeagg::AggStrategy {
    use ::nodes::nodeagg::AggStrategy;
    match code {
        1 => AggStrategy::AggSorted,
        2 => AggStrategy::AggHashed,
        3 => AggStrategy::AggMixed,
        _ => AggStrategy::AggPlain,
    }
}

fn cmd_type_from(code: i32) -> ::nodes::nodes::CmdType {
    use ::nodes::nodes::CmdType;
    match code {
        1 => CmdType::CMD_SELECT,
        2 => CmdType::CMD_UPDATE,
        3 => CmdType::CMD_INSERT,
        4 => CmdType::CMD_DELETE,
        5 => CmdType::CMD_MERGE,
        6 => CmdType::CMD_UTILITY,
        7 => CmdType::CMD_NOTHING,
        _ => CmdType::CMD_UNKNOWN,
    }
}

fn limit_option_from(code: i32) -> ::nodes::nodelimit::LimitOption {
    use ::nodes::nodelimit::LimitOption;
    match code {
        1 => LimitOption::LIMIT_OPTION_WITH_TIES,
        _ => LimitOption::LIMIT_OPTION_COUNT,
    }
}

fn subquery_scan_status_from(code: i32) -> ::nodes::nodeindexscan::SubqueryScanStatus {
    use ::nodes::nodeindexscan::SubqueryScanStatus;
    match code {
        1 => SubqueryScanStatus::Trivial,
        2 => SubqueryScanStatus::Nontrivial,
        _ => SubqueryScanStatus::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Per-node readers (fields in the exact order the OUT side wrote them).
// ---------------------------------------------------------------------------

fn read_seqscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeseqscan::SeqScan<'mcx>> {
    Ok(::nodes::nodeseqscan::SeqScan {
        scan: read_scan_fields(mcx)?,
    })
}

/// `_readSampleScan` (readfuncs.funcs.c): `READ_SCAN_FIELDS()`, then
/// `READ_NODE_FIELD(tablesample)` over the `TableSampleClause *` (read back via
/// `node_read` → `Node::TableSampleClause`, unwrapped to the typed boxed
/// carrier; a `NULL` field gives `None`). Reads in the exact order
/// `_outSampleScan` wrote.
/// `_readTableFuncScan` — scan fields then the framed `tablefunc` child.
fn read_tablefuncscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodetablefuncscan::TableFuncScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // READ_NODE_FIELD(tablefunc): a framed TABLEFUNC node (never NULL in a
    // serialized plan).
    let _label = next_tok()?;
    let tablefunc = match read::node_read(mcx, None)? {
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_tablefunc() {
                Some(tf) => alloc_in(mcx, tf)?,
                None => {
                return Err(elog_error(alloc::format!(
                    "_readTableFuncScan: expected TableFunc for tablefunc, got {:?}",
                    __tag
                )))
            },
            }
        },
        None => {
            return Err(elog_error(
                "_readTableFuncScan: tablefunc is NULL (unexpected for a TableFuncScan)",
            ))
        }
    };
    Ok(::nodes::nodetablefuncscan::TableFuncScan { scan, tablefunc })
}

fn read_samplescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodesamplescan::SampleScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // READ_NODE_FIELD(tablesample): a framed TABLESAMPLECLAUSE, or `<>` → None.
    let _label = next_tok()?;
    let tablesample = match read::node_read(mcx, None)? {
        None => None,
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_tablesampleclause() {
                Some(ts) => Some(alloc::boxed::Box::new(ts)),
                None => {
                return Err(elog_error(alloc::format!(
                    "_readSampleScan: expected TableSampleClause for tablesample, got {:?}",
                    __tag
                )))
            },
            }
        },
    };
    Ok(::nodes::nodesamplescan::SampleScan { scan, tablesample })
}

/// `_readFunctionScan` (readfuncs.funcs.c): `READ_SCAN_FIELDS()`, the
/// `functions` node list (each cell a framed `RANGETBLFUNCTION`, read back via
/// `node_read` → `Node::RangeTblFunction` and unwrapped into the typed Vec), and
/// the `funcordinality` flag. Reads in the exact order `_outFunctionScan` wrote.
fn read_functionscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodefunctionscan::FunctionScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // READ_NODE_FIELD(functions): a List of RangeTblFunction, or NIL → None.
    let _label = next_tok()?;
    let functions = match read::node_read(mcx, None)? {
        None => None,
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    {
            let __n = PgBox::into_inner(c);
            let __tag = __n.node_tag();
            match __n.into_rangetblfunction() {
                Some(rtf) => out.push(rtf),
                None => {
                            return Err(elog_error(alloc::format!(
                                "_readFunctionScan: expected RangeTblFunction in \
                                 functions list, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Some(out)
            },
                None => {
                return Err(elog_error(alloc::format!(
                    "_readFunctionScan: expected List for functions, got {:?}",
                    __tag
                )))
            },
            }
        },
    };
    let funcordinality = read_bool_field()?;
    Ok(::nodes::nodefunctionscan::FunctionScan {
        scan,
        functions,
        funcordinality,
    })
}

fn read_material<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeforeigncustom::Material<'mcx>> {
    Ok(::nodes::nodeforeigncustom::Material {
        plan: read_plan_fields(mcx)?,
    })
}

fn read_projectset<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeprojectset::ProjectSet<'mcx>> {
    Ok(::nodes::nodeprojectset::ProjectSet {
        plan: read_plan_fields(mcx)?,
    })
}

fn read_result<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::noderesult::Result<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let resconstantqual = read_expr_pgvec_opt(mcx)?;
    Ok(::nodes::noderesult::Result {
        plan,
        resconstantqual,
    })
}

fn read_append<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeappend::Append<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let apprelids = read_bitmapset_opt_field(mcx)?;
    let appendplans = read_node_vec(mcx)?;
    let nasyncplans = read_int_field()?;
    let first_partial_plan = read_int_field()?;
    let part_prune_index = read_int_field()?;
    Ok(::nodes::nodeappend::Append {
        plan,
        apprelids,
        appendplans,
        nasyncplans,
        first_partial_plan,
        part_prune_index,
    })
}

fn read_bitmapand<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodebitmapand::BitmapAnd<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let bitmapplans = read_node_vec(mcx)?;
    Ok(::nodes::nodebitmapand::BitmapAnd { plan, bitmapplans })
}

fn read_gather<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodegather::Gather<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let num_workers = read_int_field()?;
    let rescan_param = read_int_field()?;
    let single_copy = read_bool_field()?;
    let invisible = read_bool_field()?;
    let initParam = read_bitmapset_opt_field(mcx)?;
    Ok(::nodes::nodegather::Gather {
        plan,
        num_workers,
        rescan_param,
        single_copy,
        invisible,
        initParam,
    })
}

fn read_gathermerge<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodegathermerge::GatherMerge<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let num_workers = read_int_field()?;
    let rescan_param = read_int_field()?;
    let numCols = read_int_field()?;
    let sortColIdx = read_attrnumber_cols(numCols as usize)?;
    let sortOperators = read_oid_cols(numCols as usize)?;
    let collations = read_oid_cols(numCols as usize)?;
    let nullsFirst = read_bool_cols(numCols as usize)?;
    let initParam = read_bitmapset_opt_field(mcx)?;
    Ok(::nodes::nodegathermerge::GatherMerge {
        plan,
        num_workers,
        rescan_param,
        numCols,
        sortColIdx,
        sortOperators,
        collations,
        nullsFirst,
        initParam,
    })
}

fn read_mergeappend<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodemergeappend::MergeAppend<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let apprelids = read_bitmapset_opt_field(mcx)?;
    let mergeplans = read_node_vec(mcx)?;
    let numCols = read_int_field()?;
    let sortColIdx = read_attrnumber_cols(numCols as usize)?;
    let sortOperators = read_oid_cols(numCols as usize)?;
    let collations = read_oid_cols(numCols as usize)?;
    let nullsFirst = read_bool_cols(numCols as usize)?;
    let part_prune_index = read_int_field()?;
    Ok(::nodes::nodemergeappend::MergeAppend {
        plan,
        apprelids,
        mergeplans,
        numCols,
        sortColIdx,
        sortOperators,
        collations,
        nullsFirst,
        part_prune_index,
    })
}

fn read_recursiveunion<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::noderecursiveunion::RecursiveUnion<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let wtParam = read_int_field()?;
    let numCols = read_int_field()?;
    let dupColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let dupOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let dupCollations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let numGroups = read_long_field()?;
    Ok(::nodes::noderecursiveunion::RecursiveUnion {
        plan,
        wtParam,
        numCols,
        dupColIdx,
        dupOperators,
        dupCollations,
        numGroups,
    })
}

fn read_group<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodegroup::Group<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let grpColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let grpOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let grpCollations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    Ok(::nodes::nodegroup::Group {
        plan,
        numCols,
        grpColIdx,
        grpOperators,
        grpCollations,
    })
}

fn read_setop<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodesetop::SetOp<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let cmd = read_enum_field()?;
    let strategy = read_enum_field()?;
    let numCols = read_int_field()?;
    let cmpColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let cmpOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let cmpCollations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let cmpNullsFirst = into_pgvec(mcx, read_bool_cols(numCols as usize)?)?;
    let numGroups = read_long_field()?;
    Ok(::nodes::nodesetop::SetOp {
        plan,
        cmd,
        strategy,
        numCols,
        cmpColIdx,
        cmpOperators,
        cmpCollations,
        cmpNullsFirst,
        numGroups,
    })
}

fn read_unique<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeunique::Unique<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let uniqColIdx = into_pgvec_opt(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let uniqOperators = into_pgvec_opt(mcx, read_oid_cols(numCols as usize)?)?;
    let uniqCollations = into_pgvec_opt(mcx, read_oid_cols(numCols as usize)?)?;
    Ok(::nodes::nodeunique::Unique {
        plan,
        numCols,
        uniqColIdx,
        uniqOperators,
        uniqCollations,
    })
}

fn read_sort<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodesort::Sort<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let sortColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let sortOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let collations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let nullsFirst = into_pgvec(mcx, read_bool_cols(numCols as usize)?)?;
    Ok(::nodes::nodesort::Sort {
        plan,
        numCols,
        sortColIdx,
        sortOperators,
        collations,
        nullsFirst,
    })
}

fn read_incrementalsort<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodeincrementalsort::IncrementalSort<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let sortColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let sortOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let collations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let nullsFirst = into_pgvec(mcx, read_bool_cols(numCols as usize)?)?;
    let nPresortedCols = read_int_field()?;
    Ok(::nodes::nodeincrementalsort::IncrementalSort {
        sort: ::nodes::nodesort::Sort {
            plan,
            numCols,
            sortColIdx,
            sortOperators,
            collations,
            nullsFirst,
        },
        nPresortedCols,
    })
}

fn read_limit<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodelimit::Limit<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let limitOffset = read_expr_box_opt(mcx)?;
    let limitCount = read_expr_box_opt(mcx)?;
    let limitOption = limit_option_from(read_enum_field()?);
    let uniqNumCols = read_int_field()?;
    let uniqColIdx = into_pgvec_opt(mcx, read_attrnumber_cols(uniqNumCols as usize)?)?;
    let uniqOperators = into_pgvec_opt(mcx, read_oid_cols(uniqNumCols as usize)?)?;
    let uniqCollations = into_pgvec_opt(mcx, read_oid_cols(uniqNumCols as usize)?)?;
    Ok(::nodes::nodelimit::Limit {
        plan,
        limitOffset,
        limitCount,
        limitOption,
        uniqNumCols,
        uniqColIdx,
        uniqOperators,
        uniqCollations,
    })
}

fn read_agg<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeagg::Agg<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let aggstrategy = agg_strategy_from(read_enum_field()?);
    let aggsplit = read_enum_field()?; // AggSplit = i32
    let num_cols = read_int_field()?;
    let grp_col_idx = into_pgvec_opt(mcx, read_attrnumber_cols(num_cols as usize)?)?;
    let grp_operators = into_pgvec_opt(mcx, read_oid_cols(num_cols as usize)?)?;
    let grp_collations = into_pgvec_opt(mcx, read_oid_cols(num_cols as usize)?)?;
    let num_groups = read_long_field()?;
    let transition_space = read_uint64_field()?;
    let agg_params = read_bitmapset_opt_field(mcx)?;
    // groupingSets: `List *` of IntList — `(` then per-set `(i v...)` then `)`,
    // or `<>` for NIL. `node_read` rejects the inner scalar `(i ...)`, so parse
    // directly off the cursor.
    let _label = next_tok()?; // skip :groupingSets
    let open = next_tok()?;
    let grouping_sets = if open.bytes.is_empty() {
        None // `<>` → NIL
    } else if open.bytes != b"(" {
        return Err(elog_error("unrecognized token: expected '(' for groupingSets"));
    } else {
        let mut out: PgVec<'mcx, PgVec<'mcx, i32>> = PgVec::new_in(mcx);
        loop {
            let t = next_tok()?;
            if t.bytes == b")" {
                break; // end of outer list
            }
            // `t` is the inner sublist's opening `(`.
            if t.bytes != b"(" {
                return Err(elog_error("expected '(' for groupingSets IntList"));
            }
            let disc = next_tok()?;
            if disc.bytes != b"i" {
                return Err(elog_error("expected 'i' for groupingSets IntList"));
            }
            let mut iv = PgVec::new_in(mcx);
            loop {
                let it = next_tok()?;
                if it.bytes == b")" {
                    break;
                }
                iv.push(crate::atoi_i64(&tok_str(&it)) as i32);
            }
            out.push(iv);
        }
        Some(out)
    };
    // chain: `List *` of Agg. NIL → None.
    let _label = next_tok()?; // skip :chain
    let chain = match read::node_read(mcx, None)? {
        None => None,
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    {
            let __n = PgBox::into_inner(c);
            let __tag = __n.node_tag();
            match __n.into_agg() {
                Some(a) => out.push(alloc_in(mcx, a)?),
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected Agg in chain, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Some(out)
            },
                None => Err(elog_error(alloc::format!(
                "expected List for chain, got {:?}",
                __tag
            )))?,
            }
        },
    };
    Ok(::nodes::nodeagg::Agg {
        plan,
        aggstrategy,
        aggsplit,
        num_cols,
        grp_col_idx,
        grp_operators,
        grp_collations,
        num_groups,
        transition_space,
        agg_params,
        grouping_sets,
        chain,
    })
}

/// `_readWindowAgg` — reads the fields `out_windowagg` wrote, in order.
fn read_windowagg<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodewindowagg::WindowAgg<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let winname = read_string(mcx)?;
    let winref = read_uint_field()?;
    let partNumCols = read_int_field()?;
    let partColIdx = into_pgvec_opt(mcx, read_attrnumber_cols(partNumCols as usize)?)?;
    let partOperators = into_pgvec_opt(mcx, read_oid_cols(partNumCols as usize)?)?;
    let partCollations = into_pgvec_opt(mcx, read_oid_cols(partNumCols as usize)?)?;
    let ordNumCols = read_int_field()?;
    let ordColIdx = into_pgvec_opt(mcx, read_attrnumber_cols(ordNumCols as usize)?)?;
    let ordOperators = into_pgvec_opt(mcx, read_oid_cols(ordNumCols as usize)?)?;
    let ordCollations = into_pgvec_opt(mcx, read_oid_cols(ordNumCols as usize)?)?;
    let frameOptions = read_int_field()?;
    let startOffset = read_expr_box_opt(mcx)?;
    let endOffset = read_expr_box_opt(mcx)?;
    let runCondition = read_expr_pgvec_opt(mcx)?;
    let runConditionOrig = read_expr_pgvec_opt(mcx)?;
    let startInRangeFunc = read_oid_field()?;
    let endInRangeFunc = read_oid_field()?;
    let inRangeColl = read_oid_field()?;
    let inRangeAsc = read_bool_field()?;
    let inRangeNullsFirst = read_bool_field()?;
    let topWindow = read_bool_field()?;
    Ok(::nodes::nodewindowagg::WindowAgg {
        plan,
        winname,
        winref,
        partNumCols,
        partColIdx,
        partOperators,
        partCollations,
        ordNumCols,
        ordColIdx,
        ordOperators,
        ordCollations,
        frameOptions,
        startOffset,
        endOffset,
        runCondition,
        runConditionOrig,
        startInRangeFunc,
        endInRangeFunc,
        inRangeColl,
        inRangeAsc,
        inRangeNullsFirst,
        topWindow,
    })
}

/// `_readNestLoopParam` (readfuncs.funcs.c) — read the body of a framed
/// `{NESTLOOPPARAM ...}` node (the `{` and `NESTLOOPPARAM` label already
/// consumed by the caller). `NestLoopParam` is a typed struct, not a `Node`
/// arm, so it is read directly here (mirroring the OUT side's hand-emitted
/// framed list element).
fn read_nestloopparam_body<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodenestloop::NestLoopParam> {
    let paramno = read_int_field()?;
    // READ_NODE_FIELD(paramval): typed `Var *` but may be a framed `{VAR ...}`
    // or, for lateral-PHV nestloops, a `{PLACEHOLDERVAR ...}`. Read the generic
    // node and accept either as an `Expr` (the value model is lifetime-free).
    let _label = next_tok()?; // skip :paramval
    let paramval = match read::node_read(mcx, None)? {
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => e,
                None => {
                    return Err(elog_error(alloc::format!(
                        "readNestLoopParam: paramval is not an Expr node, got {:?}",
                        __tag
                    )))
                }
            }
        }
        None => {
            return Err(elog_error("readNestLoopParam: paramval is NULL"));
        }
    };
    Ok(::nodes::nodenestloop::NestLoopParam { paramno, paramval })
}

fn read_nestloop<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodenestloop::NestLoop<'mcx>> {
    let join = read_join_fields(mcx)?;
    // READ_NODE_FIELD(nestParams): `List *` of `NestLoopParam`. `<>` (NIL) is the
    // empty list; otherwise the bare `({NESTLOOPPARAM ...} ...)` list form. Each
    // element is read directly (NestLoopParam is not a `Node` arm), mirroring the
    // OUT side's hand-emitted list.
    let _label = next_tok()?; // skip :nestParams
    let mut nestParams = Vec::new();
    let first = next_tok()?;
    if !first.bytes.is_empty() {
        // Not `<>`: must be `(`.
        if first.bytes != b"(" {
            return Err(elog_error(
                "readNestLoop: expected '(' or '<>' for nestParams list",
            ));
        }
        loop {
            let t = next_tok()?;
            if t.bytes == b")" {
                break;
            }
            if t.bytes != b"{" {
                return Err(elog_error(
                    "readNestLoop: expected '{' or ')' in nestParams list",
                ));
            }
            let label = next_tok()?;
            if label.bytes != b"NESTLOOPPARAM" {
                return Err(elog_error(
                    "readNestLoop: nestParams element is not a NESTLOOPPARAM node",
                ));
            }
            let p = read_nestloopparam_body(mcx)?;
            let close = next_tok()?;
            if close.bytes != b"}" {
                return Err(elog_error(
                    "readNestLoop: expected '}' after NESTLOOPPARAM body",
                ));
            }
            nestParams.push(p);
        }
    }
    Ok(::nodes::nodenestloop::NestLoop { join, nestParams })
}

fn read_mergejoin<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodemergejoin::MergeJoin<'mcx>> {
    let join = read_join_fields(mcx)?;
    let skip_mark_restore = read_bool_field()?;
    let mergeclauses = read_expr_alloc_vec(mcx)?;
    let nclauses = mergeclauses.len();
    let mergeFamilies = read_oid_cols(nclauses)?;
    let mergeCollations = read_oid_cols(nclauses)?;
    let mergeReversals = read_bool_cols(nclauses)?;
    let mergeNullsFirst = read_bool_cols(nclauses)?;
    Ok(::nodes::nodemergejoin::MergeJoin {
        join,
        skip_mark_restore,
        mergeclauses,
        mergeFamilies,
        mergeCollations,
        mergeReversals,
        mergeNullsFirst,
    })
}

fn read_hashjoin<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodehashjoin::HashJoin<'mcx>> {
    let join = read_join_fields(mcx)?;
    let hashclauses = read_node_pgvec_opt(mcx)?;
    let hashoperators = read_oidlist(mcx)?;
    let hashcollations = read_oidlist(mcx)?;
    let hashkeys = read_node_pgvec_opt(mcx)?;
    Ok(::nodes::nodehashjoin::HashJoin {
        join,
        hashclauses,
        hashoperators,
        hashcollations,
        hashkeys,
    })
}

fn read_hash<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodehashjoin::Hash<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let hashkeys = read_node_pgvec_opt(mcx)?;
    let skewTable = read_oid_field()?;
    let skewColumn = read_int_field()? as i16;
    let skewInherit = read_bool_field()?;
    let rows_total = read_float_field()?;
    Ok(::nodes::nodehashjoin::Hash {
        plan,
        hashkeys,
        skewTable,
        skewColumn,
        skewInherit,
        rows_total,
    })
}

fn read_memoize<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodememoize::Memoize<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numKeys = read_int_field()?;
    let hashOperators = into_pgvec(mcx, read_oid_cols(numKeys as usize)?)?;
    let collations = into_pgvec(mcx, read_oid_cols(numKeys as usize)?)?;
    let param_exprs = read_expr_pgvec(mcx)?;
    let singlerow = read_bool_field()?;
    let binary_mode = read_bool_field()?;
    let est_entries = read_uint_field()?;
    let keyparamids = read_bitmapset_opt_field(mcx)?;
    // `plan_node_id` is a separate field in the repo Memoize struct (a copy of
    // plan.plan_node_id maintained by the executor); the wire format does not
    // carry it (C's Memoize has no such field), so mirror plan.plan_node_id.
    let plan_node_id = plan.plan_node_id;
    Ok(::nodes::nodememoize::Memoize {
        plan,
        plan_node_id,
        numKeys,
        hashOperators,
        collations,
        param_exprs,
        singlerow,
        binary_mode,
        est_entries,
        keyparamids,
    })
}

fn read_indexscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeindexscan::IndexScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let indexid = read_oid_field()?;
    let indexqual = read_expr_pgvec_opt(mcx)?;
    let indexqualorig = read_expr_pgvec_opt(mcx)?;
    let indexorderby = read_expr_pgvec_opt(mcx)?;
    let indexorderbyorig = read_expr_pgvec_opt(mcx)?;
    let indexorderbyops = read_oidlist_opt(mcx)?;
    let indexorderdir = scan_dir_from(read_enum_field()?);
    Ok(::nodes::nodeindexscan::IndexScan {
        scan,
        indexid,
        indexqual,
        indexqualorig,
        indexorderby,
        indexorderbyorig,
        indexorderbyops,
        indexorderdir,
    })
}

fn read_indexonlyscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodeindexonlyscan::IndexOnlyScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let indexid = read_oid_field()?;
    let indexqual = read_expr_pgvec_opt(mcx)?;
    let recheckqual = read_expr_pgvec_opt(mcx)?;
    let indexorderby = read_expr_pgvec_opt(mcx)?;
    let indextlist = read_te_pgvec_opt(mcx)?;
    let indexorderdir = scan_dir_from(read_enum_field()?);
    Ok(::nodes::nodeindexonlyscan::IndexOnlyScan {
        scan,
        indexid,
        indexqual,
        recheckqual,
        indexorderby,
        indextlist,
        indexorderdir,
    })
}

fn read_bitmapindexscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodebitmapindexscan::BitmapIndexScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let indexid = read_oid_field()?;
    let isshared = read_bool_field()?;
    let indexqual = read_expr_pgvec_opt(mcx)?;
    let indexqualorig = read_expr_pgvec_opt(mcx)?;
    Ok(::nodes::nodebitmapindexscan::BitmapIndexScan {
        scan,
        indexid,
        isshared,
        indexqual,
        indexqualorig,
    })
}

fn read_bitmapheapscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodebitmapheapscan::BitmapHeapScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let bitmapqualorig = read_expr_pgvec(mcx)?;
    Ok(::nodes::nodebitmapheapscan::BitmapHeapScan {
        scan,
        bitmapqualorig,
    })
}

fn read_tidscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodeindexscan::TidScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let tidquals = read_expr_pgvec_opt(mcx)?;
    Ok(::nodes::nodeindexscan::TidScan { scan, tidquals })
}

fn read_tidrangescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodetidrangescan::TidRangeScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let tidrangequals = read_expr_pgvec_opt(mcx)?;
    Ok(::nodes::nodetidrangescan::TidRangeScan {
        scan,
        tidrangequals,
    })
}

fn read_subqueryscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodeindexscan::SubqueryScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let subplan = read_node_opt(mcx)?;
    let scanstatus = subquery_scan_status_from(read_enum_field()?);
    Ok(::nodes::nodeindexscan::SubqueryScan {
        scan,
        subplan,
        scanstatus,
    })
}

fn read_worktablescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodeworktablescan::WorkTableScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let wtParam = read_int_field()?;
    Ok(::nodes::nodeworktablescan::WorkTableScan { scan, wtParam })
}

fn read_ctescan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodectescan::CteScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let ctePlanId = read_int_field()?;
    let cteParam = read_int_field()?;
    Ok(::nodes::nodectescan::CteScan {
        scan,
        ctePlanId,
        cteParam,
    })
}

fn read_namedtuplestorescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodenamedtuplestorescan::NamedTuplestoreScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let enrname = read_string(mcx)?;
    Ok(::nodes::nodenamedtuplestorescan::NamedTuplestoreScan { scan, enrname })
}

fn read_valuesscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::nodevaluesscan::ValuesScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // values_lists: `List *` of (`List *` of Expr) — `node_read` of `(` ... `)`
    // where each element is a `(` {..} `)` expr sublist. The sublists are
    // `Node::List` of `Node::Expr`; the outer is a `Node::List` of those.
    let _label = next_tok()?; // skip :values_lists
    let outer = read::node_read(mcx, None)?;
    let mut values_lists: PgVec<'mcx, PgVec<'mcx, Expr>> = PgVec::new_in(mcx);
    if let Some(n) = outer {
        {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(subs) => {
                for sub in subs {
                    {
            let __n = PgBox::into_inner(sub);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(exprs) => {
                            let mut row = PgVec::new_in(mcx);
                            for e in exprs {
                                {
            let __n = PgBox::into_inner(e);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(x) => row.push(x),
                None => {
                                        return Err(elog_error(alloc::format!(
                                            "expected Expr in values_lists row, got {:?}",
                                            __tag
                                        )))
                                    },
            }
        }
                            }
                            values_lists.push(row);
                        },
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected sublist in values_lists, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
            },
                None => {
                return Err(elog_error(alloc::format!(
                    "expected List for values_lists, got {:?}",
                    __tag
                )))
            },
            }
        }
    }
    Ok(::nodes::nodevaluesscan::ValuesScan { scan, values_lists })
}

fn read_foreignscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<::nodes::nodeforeigncustom::ForeignScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let operation = cmd_type_from(read_enum_field()?);
    let resultRelation = read_uint_field()?;
    let checkAsUser = read_oid_field()?;
    let fs_server = read_oid_field()?;
    let fdw_exprs = read_expr_pgvec_opt(mcx)?;
    let fdw_private = read_node_box_pgvec_opt(mcx)?;
    let fdw_scan_tlist = read_te_pgvec_opt(mcx)?;
    let fdw_recheck_quals = read_expr_pgvec_opt(mcx)?;
    let fs_relids = read_bitmapset_opt_field(mcx)?;
    let fs_base_relids = read_bitmapset_opt_field(mcx)?;
    let fsSystemCol = read_bool_field()?;
    Ok(::nodes::nodeforeigncustom::ForeignScan {
        scan,
        operation,
        resultRelation,
        checkAsUser,
        fs_server,
        fdw_exprs,
        fdw_private,
        fdw_scan_tlist,
        fdw_recheck_quals,
        fs_relids,
        fs_base_relids,
        fsSystemCol,
    })
}

fn into_pgvec<'mcx, T>(mcx: Mcx<'mcx>, v: Vec<T>) -> PgResult<PgVec<'mcx, T>> {
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for x in v {
        out.push(x);
    }
    Ok(out)
}

fn into_pgvec_opt<'mcx, T>(mcx: Mcx<'mcx>, v: Vec<T>) -> PgResult<Option<PgVec<'mcx, T>>> {
    if v.is_empty() {
        return Ok(None);
    }
    Ok(Some(into_pgvec(mcx, v)?))
}

/// Dispatch the read_plan_family LABELs this module owns.
pub(crate) fn try_read<'mcx>(mcx: Mcx<'mcx>, label: &[u8]) -> Option<PgResult<Node<'mcx>>> {
    let r: PgResult<Node<'mcx>> = match label {
        b"SEQSCAN" => read_seqscan(mcx).and_then(|p| Node::mk_seq_scan(mcx, p)),
        b"MATERIAL" => read_material(mcx).and_then(|p| Node::mk_material(mcx, p)),
        b"PROJECTSET" => read_projectset(mcx).and_then(|p| Node::mk_project_set(mcx, p)),
        b"RESULT" => read_result(mcx).and_then(|p| Node::mk_result(mcx, p)),
        b"APPEND" => read_append(mcx).and_then(|p| Node::mk_append(mcx, p)),
        b"BITMAPAND" => read_bitmapand(mcx).and_then(|p| Node::mk_bitmap_and(mcx, p)),
        b"GATHER" => read_gather(mcx).and_then(|p| Node::mk_gather(mcx, p)),
        b"GATHERMERGE" => read_gathermerge(mcx).and_then(|p| Node::mk_gather_merge(mcx, p)),
        b"MERGEAPPEND" => read_mergeappend(mcx).and_then(|p| Node::mk_merge_append(mcx, p)),
        b"RECURSIVEUNION" => read_recursiveunion(mcx).and_then(|p| Node::mk_recursive_union(mcx, p)),
        b"GROUP" => read_group(mcx).and_then(|p| Node::mk_group(mcx, p)),
        b"SETOP" => read_setop(mcx).and_then(|p| Node::mk_set_op(mcx, p)),
        b"UNIQUE" => read_unique(mcx).and_then(|p| Node::mk_unique(mcx, p)),
        b"SORT" => read_sort(mcx).and_then(|p| Node::mk_sort(mcx, p)),
        b"INCREMENTALSORT" => read_incrementalsort(mcx).and_then(|p| Node::mk_incremental_sort(mcx, p)),
        b"LIMIT" => read_limit(mcx).and_then(|p| Node::mk_limit(mcx, p)),
        b"AGG" => read_agg(mcx).and_then(|p| Node::mk_agg(mcx, p)),
        b"NESTLOOP" => read_nestloop(mcx).and_then(|p| Node::mk_nest_loop(mcx, p)),
        b"MERGEJOIN" => read_mergejoin(mcx).and_then(|p| Node::mk_merge_join(mcx, p)),
        b"HASHJOIN" => read_hashjoin(mcx).and_then(|p| Node::mk_hash_join(mcx, p)),
        b"HASH" => read_hash(mcx).and_then(|p| Node::mk_hash(mcx, p)),
        b"MEMOIZE" => read_memoize(mcx).and_then(|p| Node::mk_memoize(mcx, p)),
        b"INDEXSCAN" => read_indexscan(mcx).and_then(|p| Node::mk_index_scan(mcx, p)),
        b"INDEXONLYSCAN" => read_indexonlyscan(mcx).and_then(|p| Node::mk_index_only_scan(mcx, p)),
        b"BITMAPINDEXSCAN" => read_bitmapindexscan(mcx).and_then(|p| Node::mk_bitmap_index_scan(mcx, p)),
        b"BITMAPHEAPSCAN" => read_bitmapheapscan(mcx).and_then(|p| Node::mk_bitmap_heap_scan(mcx, p)),
        b"TIDSCAN" => read_tidscan(mcx).and_then(|p| Node::mk_tid_scan(mcx, p)),
        b"TIDRANGESCAN" => read_tidrangescan(mcx).and_then(|p| Node::mk_tid_range_scan(mcx, p)),
        b"SUBQUERYSCAN" => read_subqueryscan(mcx).and_then(|p| Node::mk_subquery_scan(mcx, p)),
        b"WORKTABLESCAN" => read_worktablescan(mcx).and_then(|p| Node::mk_work_table_scan(mcx, p)),
        b"CTESCAN" => read_ctescan(mcx).and_then(|p| Node::mk_cte_scan(mcx, p)),
        b"NAMEDTUPLESTORESCAN" => read_namedtuplestorescan(mcx).and_then(|p| Node::mk_named_tuplestore_scan(mcx, p)),
        b"VALUESSCAN" => read_valuesscan(mcx).and_then(|p| Node::mk_values_scan(mcx, p)),
        b"FOREIGNSCAN" => read_foreignscan(mcx).and_then(|p| Node::mk_foreign_scan(mcx, p)),
        b"FUNCTIONSCAN" => read_functionscan(mcx).and_then(|p| Node::mk_function_scan(mcx, p)),
        b"SAMPLESCAN" => read_samplescan(mcx).and_then(|p| Node::mk_sample_scan(mcx, p)),
        b"WINDOWAGG" => read_windowagg(mcx).and_then(|p| Node::mk_window_agg(mcx, p)),
        b"TABLEFUNCSCAN" => read_tablefuncscan(mcx).and_then(|p| Node::mk_table_func_scan(mcx, p)),
        _ => return None,
    };
    Some(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodes_core::read::string_to_node;
    use outfuncs::nodeToString;
    use mcx::MemoryContext;

    extern crate std;

    use crate::ensure_seams_for_tests as ensure_seams;

    /// OUT a framed plan node, READ it back through `string_to_node`, and assert
    /// byte-stable re-serialization (a strong round-trip / token-parity check).
    fn assert_framed_round_trip(node: &Node<'_>) -> std::string::String {
        ensure_seams();
        let ctx = MemoryContext::new("plan-roundtrip");
        let mcx = ctx.mcx();
        let text = nodeToString(mcx, node).expect("nodeToString");
        let parsed = string_to_node(mcx, text.as_str()).expect("string_to_node");
        let text2 = nodeToString(mcx, &parsed).expect("re-serialize");
        assert_eq!(text.as_str(), text2.as_str(), "round-trip not byte-stable");
        std::string::ToString::to_string(text.as_str())
    }

    #[test]
    fn seqscan_round_trips() {
        let ctx = MemoryContext::new("seqscan");
        let mcx = ctx.mcx();
        // An empty-child SeqScan: all Plan fields default, scanrelid set.
        let mut s = ::nodes::nodeseqscan::SeqScan {
            scan: ::nodes::nodeindexscan::Scan::default(),
        };
        s.scan.scanrelid = 3;
        let text = assert_framed_round_trip(&Node::mk_seq_scan(mcx, s)?);
        assert!(text.starts_with("{SEQSCAN :scan.plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":scan.plan.targetlist <>"), "{text}");
        assert!(text.ends_with(":scan.scanrelid 3}"), "{text}");
    }

    #[test]
    fn windowagg_round_trips() {
        // A WindowAgg with winname set and the part/ord count-paired arrays —
        // exercises the now-filled WRITE_STRING_FIELD(winname) and the arrays.
        let ctx = MemoryContext::new("windowagg");
        let mcx = ctx.mcx();
        let mut w = ::nodes::nodewindowagg::WindowAgg::default();
        w.winname = Some(mcx::PgString::from_str_in("w", mcx).unwrap());
        w.winref = 1;
        w.partNumCols = 1;
        let mut pc = PgVec::new_in(mcx);
        pc.push(2i16);
        w.partColIdx = Some(pc);
        let mut po = PgVec::new_in(mcx);
        po.push(96u32);
        w.partOperators = Some(po);
        let mut pco = PgVec::new_in(mcx);
        pco.push(0u32);
        w.partCollations = Some(pco);
        w.topWindow = true;
        let text = assert_framed_round_trip(&Node::mk_window_agg(mcx, w)?);
        assert!(text.starts_with("{WINDOWAGG :plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":winname w "), "{text}");
        assert!(text.ends_with(":topWindow true}"), "{text}");
    }

    #[test]
    fn tablefuncscan_round_trips() {
        // A TableFuncScan whose tablefunc carries colnames/coltypes and the
        // now-modeled plan(NULL)+location fields — exercises the framed
        // {TABLEFUNC ...} child round-trip through both families.
        let ctx = MemoryContext::new("tablefuncscan");
        let mcx = ctx.mcx();
        let mut tf = ::nodes::primnodes::TableFunc::default();
        tf.functype = ::nodes::primnodes::TableFuncType::TFT_XMLTABLE;
        let mut names = PgVec::new_in(mcx);
        names.push(mcx::PgString::from_str_in("c1", mcx).unwrap());
        tf.colnames = Some(names);
        let mut types = PgVec::new_in(mcx);
        types.push(23u32);
        tf.coltypes = Some(types);
        tf.ordinalitycol = -1;
        tf.location = 7;
        let mut s = ::nodes::nodetablefuncscan::TableFuncScan {
            scan: ::nodes::nodeindexscan::Scan::default(),
            tablefunc: alloc_in(mcx, tf).unwrap(),
        };
        s.scan.scanrelid = 4;
        let text = assert_framed_round_trip(&Node::mk_table_func_scan(mcx, s)?);
        assert!(text.starts_with("{TABLEFUNCSCAN :scan.plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":tablefunc {TABLEFUNC :functype 0"), "{text}");
        assert!(text.contains(":colnames (\"c1\")"), "{text}");
        assert!(text.contains(":plan <>"), "{text}");
        // nodeToString writes locations as -1 (write_location_fields off).
        assert!(text.contains(":location -1"), "{text}");
    }

    #[test]
    fn initplan_subplan_round_trips() {
        // A SeqScan whose Plan.initPlan carries one SubPlan — exercises the
        // `({SUBPLAN ...})` list emission (out) and `read_subplan`/`read_initplan_list`
        // (read), and the byte-stable round-trip across both.
        let ctx = std::boxed::Box::leak(std::boxed::Box::new(MemoryContext::new("initplan")));
        let mcx = ctx.mcx();
        let sp = ::nodes::primnodes::SubPlan {
            subLinkType: ::nodes::primnodes::SubLinkType::Exists,
            testexpr: None,
            paramIds: PgVec::new_in(mcx),
            plan_id: 7,
            plan_name: Some(mcx::PgString::from_str_in("InitPlan 1", mcx).unwrap()),
            firstColType: 23,
            firstColTypmod: -1,
            firstColCollation: 0,
            useHashTable: false,
            unknownEqFalse: false,
            parallel_safe: true,
            setParam: PgVec::new_in(mcx),
            parParam: PgVec::new_in(mcx),
            args: PgVec::new_in(mcx),
            startup_cost: 0.0,
            per_call_cost: 0.0,
        };
        let mut init = PgVec::new_in(mcx);
        init.push(sp);
        let mut s = ::nodes::nodeseqscan::SeqScan {
            scan: ::nodes::nodeindexscan::Scan::default(),
        };
        s.scan.plan.initPlan = Some(init);
        s.scan.scanrelid = 1;
        let text = assert_framed_round_trip(&Node::mk_seq_scan(mcx, s)?);
        assert!(text.contains("initPlan ({SUBPLAN :subLinkType"), "{text}");
        assert!(text.contains(":plan_id 7"), "{text}");

        // Read it back and confirm the SubPlan survived.
        ensure_seams();
        let parsed = string_to_node(mcx, &text).expect("read");
        {
            let __n = PgBox::into_inner(parsed);
            let __tag = __n.node_tag();
            match __n.into_seqscan() {
                Some(s) => {
                let ip = s.scan.plan.initPlan.expect("initPlan lost");
                assert_eq!(ip.len(), 1);
                assert_eq!(ip[0].plan_id, 7);
                assert!(ip[0].parallel_safe);
            },
                None => panic!("expected SeqScan, got {:?}", __tag),
            }
        }
    }

    #[test]
    fn functionscan_round_trips() {
        // A FunctionScan with one RangeTblFunction (funccolcount set, lists empty)
        // and funcordinality — exercises the framed RANGETBLFUNCTION list bridge.
        let ctx = MemoryContext::new("funcscan");
        let mcx = ctx.mcx();
        let rtf = ::nodes::rawnodes::RangeTblFunction {
            funcexpr: None,
            funccolcount: 2,
            funccolnames: PgVec::new_in(mcx),
            funccoltypes: PgVec::new_in(mcx),
            funccoltypmods: PgVec::new_in(mcx),
            funccolcollations: PgVec::new_in(mcx),
            funcparams: None,
        };
        let mut funcs = PgVec::new_in(mcx);
        funcs.push(rtf);
        let mut fs = ::nodes::nodefunctionscan::FunctionScan {
            scan: ::nodes::nodeindexscan::Scan::default(),
            functions: Some(funcs),
            funcordinality: true,
        };
        fs.scan.scanrelid = 5;
        let text = assert_framed_round_trip(&Node::mk_function_scan(mcx, fs)?);
        assert!(text.starts_with("{FUNCTIONSCAN :scan.plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":functions ({RANGETBLFUNCTION"), "{text}");
        assert!(text.ends_with(":funcordinality true}"), "{text}");
    }

    #[test]
    fn material_round_trips() {
        // A Plan-only node (no extra fields) with a child SeqScan in lefttree.
        let ctx = MemoryContext::new("mat");
        let mcx = ctx.mcx();
        let child = Node::mk_seq_scan(mcx, ::nodes::nodeseqscan::SeqScan {
            scan: ::nodes::nodeindexscan::Scan::default(),
        });
        let mut m = ::nodes::nodeforeigncustom::Material {
            plan: ::nodes::nodeindexscan::Plan::default(),
        };
        m.plan.lefttree = Some(mcx::alloc_in(mcx, child).unwrap());
        let text = assert_framed_round_trip(&Node::mk_material(mcx, m)?);
        assert!(text.starts_with("{MATERIAL :plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":plan.lefttree {SEQSCAN"), "{text}");
    }

    #[test]
    fn sort_round_trips() {
        // A Sort with two sort columns (exercises the ATTRNUMBER/OID/BOOL arrays).
        let ctx = MemoryContext::new("sort");
        let mcx = ctx.mcx();
        let mk_i16 = |v: &[i16]| {
            let mut o = PgVec::new_in(mcx);
            for x in v {
                o.push(*x);
            }
            o
        };
        let mk_u32 = |v: &[u32]| {
            let mut o = PgVec::new_in(mcx);
            for x in v {
                o.push(*x);
            }
            o
        };
        let mk_bool = |v: &[bool]| {
            let mut o = PgVec::new_in(mcx);
            for x in v {
                o.push(*x);
            }
            o
        };
        let s = ::nodes::nodesort::Sort {
            plan: ::nodes::nodeindexscan::Plan::default(),
            numCols: 2,
            sortColIdx: mk_i16(&[1, 2])?,
            sortOperators: mk_u32(&[97, 521])?,
            collations: mk_u32(&[0, 100])?,
            nullsFirst: mk_bool(&[false, true])?,
        };
        let text = assert_framed_round_trip(&Node::mk_sort(mcx, s)?);
        assert!(text.contains(":numCols 2"), "{text}");
        assert!(text.contains(":sortColIdx ( 1 2)"), "{text}");
        assert!(text.contains(":sortOperators ( 97 521)"), "{text}");
        assert!(text.contains(":nullsFirst ( false true)"), "{text}");
    }

    #[test]
    fn nestloop_empty_nestparams_round_trips() {
        let ctx = MemoryContext::new("nestloop");
        let mcx = ctx.mcx();
        // NIL nestParams → `<>`.
        let mut nl = ::nodes::nodenestloop::NestLoop::default();
        nl.join.jointype = JoinType::JOIN_INNER;
        let text = assert_framed_round_trip(&Node::mk_nest_loop(mcx, nl)?);
        assert!(text.starts_with("{NESTLOOP :join.plan.disabled_nodes 0"), "{text}");
        assert!(text.ends_with(":nestParams <>}"), "{text}");
    }

    #[test]
    fn nestloop_with_nestparams_round_trips() {
        // A NestLoop with two NestLoopParam entries, each carrying a Var paramval.
        // Exercises the framed `({NESTLOOPPARAM ...} ...)` list (out) and the
        // hand-rolled list reader (read), byte-stable across both.
        let ctx = std::boxed::Box::leak(std::boxed::Box::new(MemoryContext::new("nestloop")));
        let mcx = ctx.mcx();
        let mut v1 = ::nodes::primnodes::Var::default();
        v1.varno = 1;
        v1.varattno = 2;
        v1.vartype = 23;
        let mut v2 = ::nodes::primnodes::Var::default();
        v2.varno = 1;
        v2.varattno = 5;
        v2.vartype = 25;
        let mut nl = ::nodes::nodenestloop::NestLoop::default();
        nl.join.jointype = JoinType::JOIN_LEFT;
        nl.nestParams = std::vec![
            ::nodes::nodenestloop::NestLoopParam {
                paramno: 0,
                paramval: ::nodes::primnodes::Expr::Var(v1),
            },
            ::nodes::nodenestloop::NestLoopParam {
                paramno: 1,
                paramval: ::nodes::primnodes::Expr::Var(v2),
            },
        ];
        let text = assert_framed_round_trip(&Node::mk_nest_loop(mcx, nl)?);
        assert!(
            text.contains(":nestParams ({NESTLOOPPARAM :paramno 0 :paramval {VAR :varno 1"),
            "{text}"
        );
        assert!(text.contains("{NESTLOOPPARAM :paramno 1 :paramval {VAR :varno 1"), "{text}");

        // Read it back and confirm the params survived.
        ensure_seams();
        let parsed = string_to_node(mcx, &text).expect("read");
        {
            let __n = PgBox::into_inner(parsed);
            let __tag = __n.node_tag();
            match __n.into_nestloop() {
                Some(nl) => {
                assert_eq!(nl.nestParams.len(), 2);
                assert_eq!(nl.nestParams[0].paramno, 0);
                assert_eq!(nl.nestParams[1].paramno, 1);
                let pv0 = match &nl.nestParams[0].paramval {
                    ::nodes::primnodes::Expr::Var(v) => v,
                    _ => panic!("expected Var paramval"),
                };
                assert_eq!(pv0.varattno, 2);
                let pv1 = match &nl.nestParams[1].paramval {
                    ::nodes::primnodes::Expr::Var(v) => v,
                    _ => panic!("expected Var paramval"),
                };
                assert_eq!(pv1.vartype, 25);
            },
                None => panic!("expected NestLoop, got {:?}", __tag),
            }
        }
    }
}
