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
use types_nodes::jointype::{Join, JoinType};
use types_nodes::nodeindexscan::{Plan, Scan};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, TargetEntry};

use backend_nodes_core::read::{self, Token};

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
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = Vec::with_capacity(elems.len());
                for c in elems {
                    out.push(PgBox::into_inner(c));
                }
                Ok(out)
            }
            other => Err(elog_error(alloc::format!(
                "expected List for node-list field, got {:?}",
                other.node_tag()
            ))),
        },
    }
}

/// `READ_NODE_FIELD` over a `List *` of `Node`, as an `Option<PgVec<Node>>`
/// (NIL → None).
fn read_node_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    out.push(PgBox::into_inner(c));
                }
                Ok(Some(out))
            }
            other => Err(elog_error(alloc::format!(
                "expected List for node-list field, got {:?}",
                other.node_tag()
            ))),
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
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    out.push(c);
                }
                Ok(Some(out))
            }
            other => Err(elog_error(alloc::format!(
                "expected List for node-list field, got {:?}",
                other.node_tag()
            ))),
        },
    }
}

/// `READ_NODE_FIELD` over a `List *` of `Expr`, as an `Option<PgVec<Expr>>`
/// (NIL → None). Mirrors a `WRITE_NODE_FIELD` of an expr list.
fn read_expr_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    match PgBox::into_inner(c) {
                        Node::Expr(e) => out.push(e),
                        other => {
                            return Err(elog_error(alloc::format!(
                                "expected Expr in expr list, got {:?}",
                                other.node_tag()
                            )))
                        }
                    }
                }
                Ok(Some(out))
            }
            other => Err(elog_error(alloc::format!(
                "expected List for expr-list field, got {:?}",
                other.node_tag()
            ))),
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
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    match PgBox::into_inner(c) {
                        Node::TargetEntry(te) => out.push(te),
                        other => {
                            return Err(elog_error(alloc::format!(
                                "expected TargetEntry in tlist, got {:?}",
                                other.node_tag()
                            )))
                        }
                    }
                }
                Ok(Some(out))
            }
            other => Err(elog_error(alloc::format!(
                "expected List for tlist field, got {:?}",
                other.node_tag()
            ))),
        },
    }
}

/// Single optional child `Expr` (`<>` → None), boxed (`Option<PgBox<Expr>>`).
fn read_expr_box_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgBox<'mcx, Expr>>> {
    let _label = next_tok()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => match PgBox::into_inner(n) {
            Node::Expr(e) => Ok(Some(alloc_in(mcx, e)?)),
            other => Err(elog_error(alloc::format!(
                "expected Expr child, got {:?}",
                other.node_tag()
            ))),
        },
    }
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
    // initPlan: the OUT side emits `<>` for the empty/None case (SubPlan list
    // emission is unported); read it back as None.
    let _label = next_tok()?; // skip :...initPlan
    let initPlan = match read::node_read(mcx, None)? {
        None => None,
        Some(_) => {
            return Err(elog_error(
                "readPlan: non-empty initPlan SubPlan list is unmodeled in this \
                 serialization stage (out side panics on non-empty initPlan)",
            ))
        }
    };
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

fn scan_dir_from(code: i32) -> types_nodes::execnodes::ScanDirection {
    use types_nodes::execnodes::ScanDirection;
    match code {
        -1 => ScanDirection::BackwardScanDirection,
        1 => ScanDirection::ForwardScanDirection,
        _ => ScanDirection::NoMovementScanDirection,
    }
}

fn agg_strategy_from(code: i32) -> types_nodes::nodeagg::AggStrategy {
    use types_nodes::nodeagg::AggStrategy;
    match code {
        1 => AggStrategy::AggSorted,
        2 => AggStrategy::AggHashed,
        3 => AggStrategy::AggMixed,
        _ => AggStrategy::AggPlain,
    }
}

fn cmd_type_from(code: i32) -> types_nodes::nodes::CmdType {
    use types_nodes::nodes::CmdType;
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

fn limit_option_from(code: i32) -> types_nodes::nodelimit::LimitOption {
    use types_nodes::nodelimit::LimitOption;
    match code {
        1 => LimitOption::LIMIT_OPTION_WITH_TIES,
        _ => LimitOption::LIMIT_OPTION_COUNT,
    }
}

fn subquery_scan_status_from(code: i32) -> types_nodes::nodeindexscan::SubqueryScanStatus {
    use types_nodes::nodeindexscan::SubqueryScanStatus;
    match code {
        1 => SubqueryScanStatus::Trivial,
        2 => SubqueryScanStatus::Nontrivial,
        _ => SubqueryScanStatus::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Per-node readers (fields in the exact order the OUT side wrote them).
// ---------------------------------------------------------------------------

fn read_seqscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeseqscan::SeqScan<'mcx>> {
    Ok(types_nodes::nodeseqscan::SeqScan {
        scan: read_scan_fields(mcx)?,
    })
}

/// `_readSampleScan` (readfuncs.funcs.c): `READ_SCAN_FIELDS()`, then
/// `READ_NODE_FIELD(tablesample)` over the `TableSampleClause *` (read back via
/// `node_read` → `Node::TableSampleClause`, unwrapped to the typed boxed
/// carrier; a `NULL` field gives `None`). Reads in the exact order
/// `_outSampleScan` wrote.
fn read_samplescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodesamplescan::SampleScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // READ_NODE_FIELD(tablesample): a framed TABLESAMPLECLAUSE, or `<>` → None.
    let _label = next_tok()?;
    let tablesample = match read::node_read(mcx, None)? {
        None => None,
        Some(n) => match PgBox::into_inner(n) {
            Node::TableSampleClause(ts) => Some(alloc::boxed::Box::new(ts)),
            other => {
                return Err(elog_error(alloc::format!(
                    "_readSampleScan: expected TableSampleClause for tablesample, got {:?}",
                    other.node_tag()
                )))
            }
        },
    };
    Ok(types_nodes::nodesamplescan::SampleScan { scan, tablesample })
}

/// `_readFunctionScan` (readfuncs.funcs.c): `READ_SCAN_FIELDS()`, the
/// `functions` node list (each cell a framed `RANGETBLFUNCTION`, read back via
/// `node_read` → `Node::RangeTblFunction` and unwrapped into the typed Vec), and
/// the `funcordinality` flag. Reads in the exact order `_outFunctionScan` wrote.
fn read_functionscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodefunctionscan::FunctionScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // READ_NODE_FIELD(functions): a List of RangeTblFunction, or NIL → None.
    let _label = next_tok()?;
    let functions = match read::node_read(mcx, None)? {
        None => None,
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    match PgBox::into_inner(c) {
                        Node::RangeTblFunction(rtf) => out.push(rtf),
                        other => {
                            return Err(elog_error(alloc::format!(
                                "_readFunctionScan: expected RangeTblFunction in \
                                 functions list, got {:?}",
                                other.node_tag()
                            )))
                        }
                    }
                }
                Some(out)
            }
            other => {
                return Err(elog_error(alloc::format!(
                    "_readFunctionScan: expected List for functions, got {:?}",
                    other.node_tag()
                )))
            }
        },
    };
    let funcordinality = read_bool_field()?;
    Ok(types_nodes::nodefunctionscan::FunctionScan {
        scan,
        functions,
        funcordinality,
    })
}

fn read_material<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeforeigncustom::Material<'mcx>> {
    Ok(types_nodes::nodeforeigncustom::Material {
        plan: read_plan_fields(mcx)?,
    })
}

fn read_projectset<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeprojectset::ProjectSet<'mcx>> {
    Ok(types_nodes::nodeprojectset::ProjectSet {
        plan: read_plan_fields(mcx)?,
    })
}

fn read_result<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::noderesult::Result<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let resconstantqual = read_expr_pgvec_opt(mcx)?;
    Ok(types_nodes::noderesult::Result {
        plan,
        resconstantqual,
    })
}

fn read_append<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeappend::Append<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let apprelids = read_bitmapset_opt_field(mcx)?;
    let appendplans = read_node_vec(mcx)?;
    let nasyncplans = read_int_field()?;
    let first_partial_plan = read_int_field()?;
    let part_prune_index = read_int_field()?;
    Ok(types_nodes::nodeappend::Append {
        plan,
        apprelids,
        appendplans,
        nasyncplans,
        first_partial_plan,
        part_prune_index,
    })
}

fn read_bitmapand<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodebitmapand::BitmapAnd<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let bitmapplans = read_node_vec(mcx)?;
    Ok(types_nodes::nodebitmapand::BitmapAnd { plan, bitmapplans })
}

fn read_gather<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodegather::Gather<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let num_workers = read_int_field()?;
    let rescan_param = read_int_field()?;
    let single_copy = read_bool_field()?;
    let invisible = read_bool_field()?;
    let initParam = read_bitmapset_opt_field(mcx)?;
    Ok(types_nodes::nodegather::Gather {
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
) -> PgResult<types_nodes::nodegathermerge::GatherMerge<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let num_workers = read_int_field()?;
    let rescan_param = read_int_field()?;
    let numCols = read_int_field()?;
    let sortColIdx = read_attrnumber_cols(numCols as usize)?;
    let sortOperators = read_oid_cols(numCols as usize)?;
    let collations = read_oid_cols(numCols as usize)?;
    let nullsFirst = read_bool_cols(numCols as usize)?;
    let initParam = read_bitmapset_opt_field(mcx)?;
    Ok(types_nodes::nodegathermerge::GatherMerge {
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
) -> PgResult<types_nodes::nodemergeappend::MergeAppend<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let apprelids = read_bitmapset_opt_field(mcx)?;
    let mergeplans = read_node_vec(mcx)?;
    let numCols = read_int_field()?;
    let sortColIdx = read_attrnumber_cols(numCols as usize)?;
    let sortOperators = read_oid_cols(numCols as usize)?;
    let collations = read_oid_cols(numCols as usize)?;
    let nullsFirst = read_bool_cols(numCols as usize)?;
    let part_prune_index = read_int_field()?;
    Ok(types_nodes::nodemergeappend::MergeAppend {
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
) -> PgResult<types_nodes::noderecursiveunion::RecursiveUnion<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let wtParam = read_int_field()?;
    let numCols = read_int_field()?;
    let dupColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let dupOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let dupCollations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let numGroups = read_long_field()?;
    Ok(types_nodes::noderecursiveunion::RecursiveUnion {
        plan,
        wtParam,
        numCols,
        dupColIdx,
        dupOperators,
        dupCollations,
        numGroups,
    })
}

fn read_group<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodegroup::Group<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let grpColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let grpOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let grpCollations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    Ok(types_nodes::nodegroup::Group {
        plan,
        numCols,
        grpColIdx,
        grpOperators,
        grpCollations,
    })
}

fn read_setop<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodesetop::SetOp<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let cmd = read_enum_field()?;
    let strategy = read_enum_field()?;
    let numCols = read_int_field()?;
    let cmpColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let cmpOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let cmpCollations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let cmpNullsFirst = into_pgvec(mcx, read_bool_cols(numCols as usize)?)?;
    let numGroups = read_long_field()?;
    Ok(types_nodes::nodesetop::SetOp {
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

fn read_unique<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeunique::Unique<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let uniqColIdx = into_pgvec_opt(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let uniqOperators = into_pgvec_opt(mcx, read_oid_cols(numCols as usize)?)?;
    let uniqCollations = into_pgvec_opt(mcx, read_oid_cols(numCols as usize)?)?;
    Ok(types_nodes::nodeunique::Unique {
        plan,
        numCols,
        uniqColIdx,
        uniqOperators,
        uniqCollations,
    })
}

fn read_sort<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodesort::Sort<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let sortColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let sortOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let collations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let nullsFirst = into_pgvec(mcx, read_bool_cols(numCols as usize)?)?;
    Ok(types_nodes::nodesort::Sort {
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
) -> PgResult<types_nodes::nodeincrementalsort::IncrementalSort<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let numCols = read_int_field()?;
    let sortColIdx = into_pgvec(mcx, read_attrnumber_cols(numCols as usize)?)?;
    let sortOperators = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let collations = into_pgvec(mcx, read_oid_cols(numCols as usize)?)?;
    let nullsFirst = into_pgvec(mcx, read_bool_cols(numCols as usize)?)?;
    let nPresortedCols = read_int_field()?;
    Ok(types_nodes::nodeincrementalsort::IncrementalSort {
        sort: types_nodes::nodesort::Sort {
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

fn read_limit<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodelimit::Limit<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let limitOffset = read_expr_box_opt(mcx)?;
    let limitCount = read_expr_box_opt(mcx)?;
    let limitOption = limit_option_from(read_enum_field()?);
    let uniqNumCols = read_int_field()?;
    let uniqColIdx = into_pgvec_opt(mcx, read_attrnumber_cols(uniqNumCols as usize)?)?;
    let uniqOperators = into_pgvec_opt(mcx, read_oid_cols(uniqNumCols as usize)?)?;
    let uniqCollations = into_pgvec_opt(mcx, read_oid_cols(uniqNumCols as usize)?)?;
    Ok(types_nodes::nodelimit::Limit {
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

fn read_agg<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeagg::Agg<'mcx>> {
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
        Some(n) => match PgBox::into_inner(n) {
            Node::List(elems) => {
                let mut out = vec_with_capacity_in(mcx, elems.len())?;
                for c in elems {
                    match PgBox::into_inner(c) {
                        Node::Agg(a) => out.push(alloc_in(mcx, a)?),
                        other => {
                            return Err(elog_error(alloc::format!(
                                "expected Agg in chain, got {:?}",
                                other.node_tag()
                            )))
                        }
                    }
                }
                Some(out)
            }
            other => Err(elog_error(alloc::format!(
                "expected List for chain, got {:?}",
                other.node_tag()
            )))?,
        },
    };
    Ok(types_nodes::nodeagg::Agg {
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

fn read_nestloop<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodenestloop::NestLoop<'mcx>> {
    let join = read_join_fields(mcx)?;
    // nestParams: the OUT side emits `<>` only (NestLoopParam unmodeled);
    // read it back as the empty list.
    let _label = next_tok()?; // skip :nestParams
    match read::node_read(mcx, None)? {
        None => {}
        Some(_) => {
            return Err(elog_error(
                "readNestLoop: non-empty nestParams (NestLoopParam) is unmodeled \
                 in this serialization stage (out side panics on a non-empty list)",
            ))
        }
    }
    Ok(types_nodes::nodenestloop::NestLoop {
        join,
        nestParams: Vec::new(),
    })
}

fn read_mergejoin<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodemergejoin::MergeJoin<'mcx>> {
    let join = read_join_fields(mcx)?;
    let skip_mark_restore = read_bool_field()?;
    let mergeclauses = read_expr_alloc_vec(mcx)?;
    let nclauses = mergeclauses.len();
    let mergeFamilies = read_oid_cols(nclauses)?;
    let mergeCollations = read_oid_cols(nclauses)?;
    let mergeReversals = read_bool_cols(nclauses)?;
    let mergeNullsFirst = read_bool_cols(nclauses)?;
    Ok(types_nodes::nodemergejoin::MergeJoin {
        join,
        skip_mark_restore,
        mergeclauses,
        mergeFamilies,
        mergeCollations,
        mergeReversals,
        mergeNullsFirst,
    })
}

fn read_hashjoin<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodehashjoin::HashJoin<'mcx>> {
    let join = read_join_fields(mcx)?;
    let hashclauses = read_node_pgvec_opt(mcx)?;
    let hashoperators = read_oidlist(mcx)?;
    let hashcollations = read_oidlist(mcx)?;
    let hashkeys = read_node_pgvec_opt(mcx)?;
    Ok(types_nodes::nodehashjoin::HashJoin {
        join,
        hashclauses,
        hashoperators,
        hashcollations,
        hashkeys,
    })
}

fn read_hash<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodehashjoin::Hash<'mcx>> {
    let plan = read_plan_fields(mcx)?;
    let hashkeys = read_node_pgvec_opt(mcx)?;
    let skewTable = read_oid_field()?;
    let skewColumn = read_int_field()? as i16;
    let skewInherit = read_bool_field()?;
    let rows_total = read_float_field()?;
    Ok(types_nodes::nodehashjoin::Hash {
        plan,
        hashkeys,
        skewTable,
        skewColumn,
        skewInherit,
        rows_total,
    })
}

fn read_memoize<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodememoize::Memoize<'mcx>> {
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
    Ok(types_nodes::nodememoize::Memoize {
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

fn read_indexscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeindexscan::IndexScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let indexid = read_oid_field()?;
    let indexqual = read_expr_pgvec_opt(mcx)?;
    let indexqualorig = read_expr_pgvec_opt(mcx)?;
    let indexorderby = read_expr_pgvec_opt(mcx)?;
    let indexorderbyorig = read_expr_pgvec_opt(mcx)?;
    let indexorderbyops = read_oidlist_opt(mcx)?;
    let indexorderdir = scan_dir_from(read_enum_field()?);
    Ok(types_nodes::nodeindexscan::IndexScan {
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
) -> PgResult<types_nodes::nodeindexonlyscan::IndexOnlyScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let indexid = read_oid_field()?;
    let indexqual = read_expr_pgvec_opt(mcx)?;
    let recheckqual = read_expr_pgvec_opt(mcx)?;
    let indexorderby = read_expr_pgvec_opt(mcx)?;
    let indextlist = read_te_pgvec_opt(mcx)?;
    let indexorderdir = scan_dir_from(read_enum_field()?);
    Ok(types_nodes::nodeindexonlyscan::IndexOnlyScan {
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
) -> PgResult<types_nodes::nodebitmapindexscan::BitmapIndexScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let indexid = read_oid_field()?;
    let isshared = read_bool_field()?;
    let indexqual = read_expr_pgvec_opt(mcx)?;
    let indexqualorig = read_expr_pgvec_opt(mcx)?;
    Ok(types_nodes::nodebitmapindexscan::BitmapIndexScan {
        scan,
        indexid,
        isshared,
        indexqual,
        indexqualorig,
    })
}

fn read_tidscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodeindexscan::TidScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let tidquals = read_expr_pgvec_opt(mcx)?;
    Ok(types_nodes::nodeindexscan::TidScan { scan, tidquals })
}

fn read_tidrangescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodetidrangescan::TidRangeScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let tidrangequals = read_expr_pgvec_opt(mcx)?;
    Ok(types_nodes::nodetidrangescan::TidRangeScan {
        scan,
        tidrangequals,
    })
}

fn read_subqueryscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodeindexscan::SubqueryScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let subplan = read_node_opt(mcx)?;
    let scanstatus = subquery_scan_status_from(read_enum_field()?);
    Ok(types_nodes::nodeindexscan::SubqueryScan {
        scan,
        subplan,
        scanstatus,
    })
}

fn read_worktablescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodeworktablescan::WorkTableScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let wtParam = read_int_field()?;
    Ok(types_nodes::nodeworktablescan::WorkTableScan { scan, wtParam })
}

fn read_ctescan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodectescan::CteScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let ctePlanId = read_int_field()?;
    let cteParam = read_int_field()?;
    Ok(types_nodes::nodectescan::CteScan {
        scan,
        ctePlanId,
        cteParam,
    })
}

fn read_namedtuplestorescan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodenamedtuplestorescan::NamedTuplestoreScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    let enrname = read_string(mcx)?;
    Ok(types_nodes::nodenamedtuplestorescan::NamedTuplestoreScan { scan, enrname })
}

fn read_valuesscan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::nodevaluesscan::ValuesScan<'mcx>> {
    let scan = read_scan_fields(mcx)?;
    // values_lists: `List *` of (`List *` of Expr) — `node_read` of `(` ... `)`
    // where each element is a `(` {..} `)` expr sublist. The sublists are
    // `Node::List` of `Node::Expr`; the outer is a `Node::List` of those.
    let _label = next_tok()?; // skip :values_lists
    let outer = read::node_read(mcx, None)?;
    let mut values_lists: PgVec<'mcx, PgVec<'mcx, Expr>> = PgVec::new_in(mcx);
    if let Some(n) = outer {
        match PgBox::into_inner(n) {
            Node::List(subs) => {
                for sub in subs {
                    match PgBox::into_inner(sub) {
                        Node::List(exprs) => {
                            let mut row = PgVec::new_in(mcx);
                            for e in exprs {
                                match PgBox::into_inner(e) {
                                    Node::Expr(x) => row.push(x),
                                    other => {
                                        return Err(elog_error(alloc::format!(
                                            "expected Expr in values_lists row, got {:?}",
                                            other.node_tag()
                                        )))
                                    }
                                }
                            }
                            values_lists.push(row);
                        }
                        other => {
                            return Err(elog_error(alloc::format!(
                                "expected sublist in values_lists, got {:?}",
                                other.node_tag()
                            )))
                        }
                    }
                }
            }
            other => {
                return Err(elog_error(alloc::format!(
                    "expected List for values_lists, got {:?}",
                    other.node_tag()
                )))
            }
        }
    }
    Ok(types_nodes::nodevaluesscan::ValuesScan { scan, values_lists })
}

fn read_foreignscan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_nodes::nodeforeigncustom::ForeignScan<'mcx>> {
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
    Ok(types_nodes::nodeforeigncustom::ForeignScan {
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
        b"SEQSCAN" => read_seqscan(mcx).map(Node::SeqScan),
        b"MATERIAL" => read_material(mcx).map(Node::Material),
        b"PROJECTSET" => read_projectset(mcx).map(Node::ProjectSet),
        b"RESULT" => read_result(mcx).map(Node::Result),
        b"APPEND" => read_append(mcx).map(Node::Append),
        b"BITMAPAND" => read_bitmapand(mcx).map(Node::BitmapAnd),
        b"GATHER" => read_gather(mcx).map(Node::Gather),
        b"GATHERMERGE" => read_gathermerge(mcx).map(Node::GatherMerge),
        b"MERGEAPPEND" => read_mergeappend(mcx).map(Node::MergeAppend),
        b"RECURSIVEUNION" => read_recursiveunion(mcx).map(Node::RecursiveUnion),
        b"GROUP" => read_group(mcx).map(Node::Group),
        b"SETOP" => read_setop(mcx).map(Node::SetOp),
        b"UNIQUE" => read_unique(mcx).map(Node::Unique),
        b"SORT" => read_sort(mcx).map(Node::Sort),
        b"INCREMENTALSORT" => read_incrementalsort(mcx).map(Node::IncrementalSort),
        b"LIMIT" => read_limit(mcx).map(Node::Limit),
        b"AGG" => read_agg(mcx).map(Node::Agg),
        b"NESTLOOP" => read_nestloop(mcx).map(Node::NestLoop),
        b"MERGEJOIN" => read_mergejoin(mcx).map(Node::MergeJoin),
        b"HASHJOIN" => read_hashjoin(mcx).map(Node::HashJoin),
        b"HASH" => read_hash(mcx).map(Node::Hash),
        b"MEMOIZE" => read_memoize(mcx).map(Node::Memoize),
        b"INDEXSCAN" => read_indexscan(mcx).map(Node::IndexScan),
        b"INDEXONLYSCAN" => read_indexonlyscan(mcx).map(Node::IndexOnlyScan),
        b"BITMAPINDEXSCAN" => read_bitmapindexscan(mcx).map(Node::BitmapIndexScan),
        b"TIDSCAN" => read_tidscan(mcx).map(Node::TidScan),
        b"TIDRANGESCAN" => read_tidrangescan(mcx).map(Node::TidRangeScan),
        b"SUBQUERYSCAN" => read_subqueryscan(mcx).map(Node::SubqueryScan),
        b"WORKTABLESCAN" => read_worktablescan(mcx).map(Node::WorkTableScan),
        b"CTESCAN" => read_ctescan(mcx).map(Node::CteScan),
        b"NAMEDTUPLESTORESCAN" => read_namedtuplestorescan(mcx).map(Node::NamedTuplestoreScan),
        b"VALUESSCAN" => read_valuesscan(mcx).map(Node::ValuesScan),
        b"FOREIGNSCAN" => read_foreignscan(mcx).map(Node::ForeignScan),
        b"FUNCTIONSCAN" => read_functionscan(mcx).map(Node::FunctionScan),
        b"SAMPLESCAN" => read_samplescan(mcx).map(Node::SampleScan),
        _ => return None,
    };
    Some(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use backend_nodes_core::read::string_to_node;
    use backend_nodes_outfuncs::nodeToString;
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
        // An empty-child SeqScan: all Plan fields default, scanrelid set.
        let mut s = types_nodes::nodeseqscan::SeqScan {
            scan: types_nodes::nodeindexscan::Scan::default(),
        };
        s.scan.scanrelid = 3;
        let text = assert_framed_round_trip(&Node::SeqScan(s));
        assert!(text.starts_with("{SEQSCAN :scan.plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":scan.plan.targetlist <>"), "{text}");
        assert!(text.ends_with(":scan.scanrelid 3}"), "{text}");
    }

    #[test]
    fn functionscan_round_trips() {
        // A FunctionScan with one RangeTblFunction (funccolcount set, lists empty)
        // and funcordinality — exercises the framed RANGETBLFUNCTION list bridge.
        let ctx = MemoryContext::new("funcscan");
        let mcx = ctx.mcx();
        let rtf = types_nodes::rawnodes::RangeTblFunction {
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
        let mut fs = types_nodes::nodefunctionscan::FunctionScan {
            scan: types_nodes::nodeindexscan::Scan::default(),
            functions: Some(funcs),
            funcordinality: true,
        };
        fs.scan.scanrelid = 5;
        let text = assert_framed_round_trip(&Node::FunctionScan(fs));
        assert!(text.starts_with("{FUNCTIONSCAN :scan.plan.disabled_nodes 0"), "{text}");
        assert!(text.contains(":functions ({RANGETBLFUNCTION"), "{text}");
        assert!(text.ends_with(":funcordinality true}"), "{text}");
    }

    #[test]
    fn material_round_trips() {
        // A Plan-only node (no extra fields) with a child SeqScan in lefttree.
        let ctx = MemoryContext::new("mat");
        let mcx = ctx.mcx();
        let child = Node::SeqScan(types_nodes::nodeseqscan::SeqScan {
            scan: types_nodes::nodeindexscan::Scan::default(),
        });
        let mut m = types_nodes::nodeforeigncustom::Material {
            plan: types_nodes::nodeindexscan::Plan::default(),
        };
        m.plan.lefttree = Some(mcx::alloc_in(mcx, child).unwrap());
        let text = assert_framed_round_trip(&Node::Material(m));
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
        let s = types_nodes::nodesort::Sort {
            plan: types_nodes::nodeindexscan::Plan::default(),
            numCols: 2,
            sortColIdx: mk_i16(&[1, 2]),
            sortOperators: mk_u32(&[97, 521]),
            collations: mk_u32(&[0, 100]),
            nullsFirst: mk_bool(&[false, true]),
        };
        let text = assert_framed_round_trip(&Node::Sort(s));
        assert!(text.contains(":numCols 2"), "{text}");
        assert!(text.contains(":sortColIdx ( 1 2)"), "{text}");
        assert!(text.contains(":sortOperators ( 97 521)"), "{text}");
        assert!(text.contains(":nullsFirst ( false true)"), "{text}");
    }
}
