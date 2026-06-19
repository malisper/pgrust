#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// Every fallible function returns the shared `types_error::PgResult` (==
// `Result<_, PgError>`); `PgError`'s size is fixed by `types-error`, so we accept
// the large-`Err` lint crate-wide, like every sibling parser crate.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/parser/parse_cte.c` (PostgreSQL 18.3) — handling of CTEs
//! (common table expressions) in the parser: the `WITH` / `WITH RECURSIVE`
//! clause transform, the recursive-CTE dependency graph + topological sort, the
//! recursion well-formedness checks, and the `SEARCH` / `CYCLE` clause
//! validation.
//!
//! Every `parse_cte.c` function is ported here with its original C name, branch
//! order, error message text and SQLSTATE preserved 1:1:
//!
//! * [`transformWithClause`] (parse_cte.c:110) — duplicate-name check,
//!   modifying-CTE flag, recursive vs non-recursive analysis paths.
//! * [`analyzeCTE`] (parse_cte.c:243) — cycle-mark typing, sub-analyze,
//!   recursive-term type/collation re-check, SEARCH/CYCLE validation.
//! * [`analyzeCTETargetList`] (parse_cte.c:571) — output column
//!   name/type/collation derivation; unknown→text for recursive CTEs.
//! * `makeDependencyGraph` / `makeDependencyGraphWalker` / `WalkInnerWith` /
//!   `TopologicalSort` — the cross-reference dependency graph + no-forward-ref
//!   ordering.
//! * `checkWellFormedRecursion` / `checkWellFormedRecursionWalker` /
//!   `checkWellFormedSelectStmt` — recursion well-formedness.
//!
//! # The owned node tree
//!
//! This repo carries the parse tree as owned `mcx`-allocated values
//! (`types_nodes::nodes::Node`, `PgBox`/`PgVec`/`PgString`), not raw `*mut`
//! pointers. So the C in-place pointer mutation of the CTE objects via the
//! `ctenamespace` (which aliases the very `CommonTableExpr`s `analyzeCTE`
//! mutates) is modelled here with an owned working collection ([`CteState`])
//! that holds the CTEs + per-CTE sort metadata, indexed in lockstep; the
//! topological sort reorders them, the walkers borrow the CTE subqueries
//! read-only via [`backend_nodes_core::node_walker::raw_expression_tree_walker`]
//! and mutate only the scratch state, and the `p_ctenamespace` clones are
//! refreshed after each `analyzeCTE` to preserve the forward-visibility-of-
//! analyzed-columns invariant ([`refresh_ctenamespace_entry`]).
//!
//! The `Bitmapset depends_on` of the C `CteItem` is a `BTreeSet<i32>` (only
//! add/delete/is-empty are used; identical semantics). The `innerwiths` list of
//! lists of `CommonTableExpr` is kept as a stack of name-frames (only the CTE
//! names are consulted).
//!
//! # The split Expr/Node model
//!
//! Analyzed expression nodes live in the single `Node::Expr(Expr)` arm. The
//! cycle-mark value/default are stored back as `Node::Expr` after
//! `transformExpr`; the node-level `exprType`/`exprTypmod`/`exprCollation`/
//! `exprLocation` reach the `Expr`-level [`backend_nodes_core::nodefuncs`]
//! accessors through that arm. The coerce / collate seam helpers take `Expr`
//! values, so the cycle-mark exprs are threaded as `Expr` through those calls
//! and re-wrapped into `Node::Expr` for storage.
//!
//! # Deps and seams
//!
//! Merged owners called directly (cycle-free): `transformExpr`
//! (backend-parser-parse-expr), `select_common_type` / `coerce_to_common_type` /
//! `select_common_typmod` (backend-parser-coerce), `select_common_collation`
//! (backend-parser-parse-collate), `format_type_be_owned` /
//! `format_type_with_typemod` (backend-utils-adt-format-type), `get_negator` /
//! `get_collation_name` (backend-utils-cache-lsyscache). Outward seams:
//! `parse_sub_analyze` (analyze.c, the CTE↔analyze recursion seam, unported),
//! `lookup_type_cache_eq_opr` (typcache, across the dep cycle), and
//! `parser_errposition` (parse_node.c, owned by parser-small1). Each seam
//! defaults to a loud panic until installed; this crate never silently stubs.
//!
//! This crate owns no inward seam: its public functions (`transformWithClause`,
//! `analyzeCTETargetList`) are consumed by analyze.c / parse_clause.c (both
//! unported), which will call them directly as a merged owner once landed
//! (cf. parse_collate / parse_oper). So `init_seams()` is empty and the crate is
//! not wired into `init_all`.

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgString, PgVec};

use types_core::primitive::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_COLLATION_MISMATCH, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_ALIAS, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_RECURSION, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION, ERROR,
};
use types_tuple::heaptuple::{DEFAULT_COLLATION_OID, TEXTOID, UNKNOWNOID};

use types_nodes::copy_query::Query;
use types_nodes::jointype::JoinType;
use types_nodes::nodes::{ntag, CmdType, Node};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{Expr, TargetEntry};
use types_nodes::rawnodes::{
    CTECycleClause, CommonTableExpr, SelectStmt, SetOperation, WithClause,
};
use types_nodes::value::StringNode;

use backend_nodes_core::nodefuncs::{expr_collation, expr_location, expr_type, expr_typmod};
use backend_nodes_core::node_walker::raw_expression_tree_walker;
use backend_utils_error::ereport;

/// `TYPECACHE_EQ_OPR` (typcache.h) — request the equality operator of a type.
/// (Consumed only inside the `lookup_type_cache_eq_opr` seam; kept here as
/// documentation of the C call.)
pub const TYPECACHE_EQ_OPR: i32 = 0x0001;

// ===========================================================================
// RecursionContext (parse_cte.c:31) and its message table (parse_cte.c:42).
// ===========================================================================

/// Enumeration of contexts in which a self-reference is disallowed.
/// (`RecursionContext`, parse_cte.c:31)
#[derive(Clone, Copy, PartialEq, Eq)]
enum RecursionContext {
    /// `RECURSION_OK`
    Ok,
    /// `RECURSION_NONRECURSIVETERM` — inside the left-hand term.
    NonRecursiveTerm,
    /// `RECURSION_SUBLINK` — inside a sublink.
    Sublink,
    /// `RECURSION_OUTERJOIN` — inside nullable side of an outer join.
    OuterJoin,
    /// `RECURSION_INTERSECT` — underneath INTERSECT (ALL).
    Intersect,
    /// `RECURSION_EXCEPT` — underneath EXCEPT (ALL).
    Except,
}

/// Associated error messages --- each must have one %s for CTE name.
/// (`recursion_errormsgs`, parse_cte.c:42)
fn recursion_errormsg(ctx: RecursionContext, ctename: &str) -> String {
    match ctx {
        // RECURSION_OK has no message in C (NULL); it is never used to format.
        RecursionContext::Ok => String::new(),
        RecursionContext::NonRecursiveTerm => format!(
            "recursive reference to query \"{ctename}\" must not appear within its non-recursive term"
        ),
        RecursionContext::Sublink => format!(
            "recursive reference to query \"{ctename}\" must not appear within a subquery"
        ),
        RecursionContext::OuterJoin => format!(
            "recursive reference to query \"{ctename}\" must not appear within an outer join"
        ),
        RecursionContext::Intersect => format!(
            "recursive reference to query \"{ctename}\" must not appear within INTERSECT"
        ),
        RecursionContext::Except => {
            format!("recursive reference to query \"{ctename}\" must not appear within EXCEPT")
        }
    }
}

// ===========================================================================
// Internal working structs (parse_cte.c:63, :71).
//
// Working state for the tree walkers; not ABI / on-disk structures.
// ===========================================================================

/// Per-CTE bookkeeping the topological sort needs. (`CteItem`, parse_cte.c:63)
///
/// The CTE itself lives in [`CteState::ctes`]; this carries only the sort
/// metadata indexed in lockstep with that vector.
struct CteItem {
    /// Index into [`CteState::ctes`] of the CTE this item describes.
    cte: usize,
    /// Its ID number for dependencies.
    id: i32,
    /// CTEs depended on (not including self). Mirrors the C `Bitmapset`; only
    /// add / delete / is-empty are performed, so a sorted set of member ids has
    /// identical semantics.
    depends_on: alloc::collections::BTreeSet<i32>,
}

/// What we need to pass around in the tree walkers. (`CteState`, parse_cte.c:71)
struct CteState<'mcx> {
    /// the owned CTEs being analyzed (the C `withClause->ctes` array elements)
    ctes: Vec<CommonTableExpr<'mcx>>,
    /// array of per-CTE sort metadata (the C `CteItem[]`)
    items: Vec<CteItem>,
    // working state during a tree walk:
    /// index into `items` of the item currently being examined
    curitem: usize,
    /// stack of inner-WITH visibility frames. Each frame is the list of CTE
    /// names visible from that inner WITH (the C `innerwiths`, a list of lists
    /// of `CommonTableExpr`; only the names are consulted).
    innerwiths: Vec<Vec<String>>,
    // working state for checkWellFormedRecursion walk only:
    /// number of self-references detected
    selfrefcount: i32,
    /// context to allow or disallow self-ref
    context: RecursionContext,
}

impl<'mcx> CteState<'mcx> {
    fn numitems(&self) -> usize {
        self.items.len()
    }

    /// The `CommonTableExpr` that `items[i]` refers to.
    fn item_cte(&self, i: usize) -> &CommonTableExpr<'mcx> {
        &self.ctes[self.items[i].cte]
    }
}

// ===========================================================================
// Small in-crate helpers.
// ===========================================================================

/// `errmsg("out of memory")` for a `palloc`/`lappend` failure path. C's
/// `palloc` never returns NULL (it `ereport(ERROR)`s on exhaustion); the
/// `try_reserve` model surfaces that as a recoverable `Err`.
fn out_of_memory() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}

/// `elog(ERROR, msg)` — build an internal-error `PgError` (errmsg_internal,
/// default XX000 SQLSTATE for ERROR) for the "shouldn't happen" cases.
fn elog_error(message: impl Into<String>) -> PgError {
    ereport(ERROR).errmsg_internal(message).into_error()
}

/// Push onto a `Vec`, surfacing allocator exhaustion as the C `palloc` ereport.
fn push_checked<T>(v: &mut Vec<T>, item: T) -> PgResult<()> {
    v.try_reserve(1).map_err(|_| out_of_memory())?;
    v.push(item);
    Ok(())
}

/// Push onto a `PgVec`, surfacing allocator exhaustion as the C `palloc`/
/// `lappend` ereport.
fn pgvec_push<T>(v: &mut PgVec<'_, T>, item: T) -> PgResult<()> {
    v.try_reserve(1).map_err(|_| out_of_memory())?;
    v.push(item);
    Ok(())
}

/// `parser_errposition(pstate, location)` (parse_node.c, owned by
/// parser-small1) — the cursor position for an error.
fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> PgResult<i32> {
    backend_parser_small1_seams::parser_errposition::call(pstate, location)
}

/// `format_type_be(type_oid)` for an error message (format_type.c).
fn format_type_be(mcx: Mcx<'_>, type_oid: Oid) -> PgResult<String> {
    let _ = mcx;
    Ok(backend_utils_adt_format_type::format_type_be_owned(type_oid)?)
}

/// `format_type_with_typemod(type_oid, typemod)` for an error message
/// (format_type.c).
fn format_type_with_typemod(mcx: Mcx<'_>, type_oid: Oid, typemod: i32) -> PgResult<String> {
    let s = backend_utils_adt_format_type::format_type_with_typemod(mcx, type_oid, typemod)?;
    Ok(s.as_str().to_string())
}

/// Render a collation name the way C's `errmsg("%s", get_collation_name(oid))`
/// does: a non-NULL name prints verbatim, while a NULL `char *` (an unknown /
/// `InvalidOid` collation, modelled as `None`) prints glibc's literal
/// `(null)`.
fn collation_name_or_null(mcx: Mcx<'_>, colloid: Oid) -> PgResult<String> {
    let name =
        backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_name(
            mcx, colloid,
        )?;
    Ok(name.map(|s| s.as_str().to_string()).unwrap_or_else(|| "(null)".to_string()))
}

/// The CTE name (`ctename`), or `""` for a (shouldn't-happen) NULL.
fn cte_name<'a>(cte: &'a CommonTableExpr<'_>) -> &'a str {
    cte.ctename.as_ref().map(|s| s.as_str()).unwrap_or("")
}

/// `makeString(pstrdup(s))` — a `Node::String` allocated for a colname list.
fn make_string_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    let node = Node::mk_string(mcx, StringNode {
        sval: PgString::from_str_in(s, mcx)?,
    });
    mcx::alloc_in(mcx, node)
}

/// `strVal(node)` — read a `String` node's contents (`""` for a non-String).
fn str_val<'a>(node: &'a Node<'_>) -> &'a str {
    match node.node_tag() {
        ntag::T_String => node.expect_string().sval.as_str(),
        _ => "",
    }
}

/// Is this `Node` a `Query`?
fn is_query(node: Option<&Node<'_>>) -> bool {
    node.is_some_and(|n| n.is_query())
}

/// Borrow a node as a `Query` if it is one.
fn query_of<'a, 'mcx>(node: Option<&'a Node<'mcx>>) -> Option<&'a Query<'mcx>> {
    node.and_then(|n| n.as_query())
}

/// Is the node a raw `SelectStmt`?
fn is_select_stmt(node: Option<&Node<'_>>) -> bool {
    node.is_some_and(|n| n.is_selectstmt())
}

/// Is the node a data-modifying raw statement (Insert/Update/Delete/Merge)?
fn is_data_modifying_stmt(node: Option<&Node<'_>>) -> bool {
    node.is_some_and(|n| {
        matches!(
            n.node_tag(),
            ntag::T_InsertStmt | ntag::T_UpdateStmt | ntag::T_DeleteStmt | ntag::T_MergeStmt
        )
    })
}

/// Is the node a `RangeTblRef`?
fn is_range_tbl_ref(node: Option<&Node<'_>>) -> bool {
    node.is_some_and(|n| n.is_rangetblref())
}

/// The `withClause` of a raw DML/SELECT statement node — `None` if the node is
/// not such a statement or has no WITH clause.
fn stmt_with_clause<'a, 'mcx>(node: &'a Node<'mcx>) -> Option<&'a WithClause<'mcx>> {
    match node.node_tag() {
        ntag::T_SelectStmt => node.expect_selectstmt().withClause.as_deref(),
        ntag::T_InsertStmt => node.expect_insertstmt().withClause.as_deref(),
        ntag::T_UpdateStmt => node.expect_updatestmt().withClause.as_deref(),
        ntag::T_DeleteStmt => node.expect_deletestmt().withClause.as_deref(),
        ntag::T_MergeStmt => node.expect_mergestmt().withClause.as_deref(),
        _ => None,
    }
}

/// The list of CTE names declared by a `WithClause`.
fn cte_names_of(wc: &WithClause<'_>) -> Vec<String> {
    wc.ctes
        .iter()
        .filter_map(|c| c.as_commontableexpr().map(|cte| cte_name(cte).to_string()))
        .collect()
}

/// `GetCTETargetList(cte)` (parsenodes.h): the SELECT targetlist, or the
/// RETURNING list for data-modifying CTEs. The ctequery must be a Query. Both
/// fields are `PgVec<TargetEntry>` here; this clones the relevant one.
fn get_cte_target_list<'mcx>(
    mcx: Mcx<'mcx>,
    cte: &CommonTableExpr<'mcx>,
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    let q = query_of(cte.ctequery.as_deref())
        .ok_or_else(|| elog_error("GetCTETargetList: ctequery is not a Query"))?;
    let src = if q.commandType == CmdType::CMD_SELECT {
        &q.targetList
    } else {
        &q.returningList
    };
    let mut out: Vec<TargetEntry<'mcx>> = Vec::new();
    out.try_reserve(src.len()).map_err(|_| out_of_memory())?;
    for te in src.iter() {
        out.push(te.clone_in(mcx)?);
    }
    Ok(out)
}

/// Return the analyzed CTE's non-recursive (leftmost) UNION term targetlist —
/// the subquery in the leftmost `RangeTblRef` of the query's `setOperations`
/// tree (or the query's own targetlist when there's no set-op).  Mirrors the
/// targetlist `determineRecursiveColTypes` (analyze.c) used to size the
/// recursive CTE's output columns.
fn nonrecursive_term_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    cte: &CommonTableExpr<'mcx>,
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    let q = query_of(cte.ctequery.as_deref())
        .ok_or_else(|| elog_error("nonrecursive_term_targetlist: ctequery is not a Query"))?;

    let src_tlist: &PgVec<'mcx, TargetEntry<'mcx>> = match q.setOperations.as_deref() {
        Some(setop_node) => {
            // Descend to the leftmost leaf RangeTblRef of the set-op tree.
            let mut node: &Node = setop_node;
            let leftmost_rti = loop {
                match node.node_tag() {
                    ntag::T_SetOperationStmt => {
                        node = node
                            .expect_setoperationstmt()
                            .larg
                            .as_deref()
                            .ok_or_else(|| elog_error("set-op tree has no left child"))?;
                    }
                    ntag::T_RangeTblRef => break node.expect_rangetblref().rtindex,
                    _ => return Err(elog_error("set-op leftmost is not a RangeTblRef")),
                }
            };
            let rte = &q.rtable[(leftmost_rti - 1) as usize];
            let leftq = rte
                .subquery
                .as_deref()
                .ok_or_else(|| elog_error("leftmost set-op member is not a subquery"))?;
            &leftq.targetList
        }
        None => &q.targetList,
    };

    let mut out: Vec<TargetEntry<'mcx>> = Vec::new();
    out.try_reserve(src_tlist.len()).map_err(|_| out_of_memory())?;
    for te in src_tlist.iter() {
        out.push(te.clone_in(mcx)?);
    }
    Ok(out)
}

/// `exprLocation((Node *) list)` over a list of raw nodes — the minimum
/// non-negative child location (-1 if empty / all unknown).
fn expr_location_of_list(list: &PgVec<'_, PgBox<'_, Node<'_>>>) -> PgResult<i32> {
    let mut result = -1i32;
    for n in list.iter() {
        let loc = expr_location_of_node(n)?;
        if loc < 0 {
            continue;
        }
        if result < 0 || loc < result {
            result = loc;
        }
    }
    Ok(result)
}

/// `exprLocation((Node *) n)` over a raw or analyzed node. Raw grammar nodes
/// carry no location in this model except via the `Node::Expr` arm; we read the
/// expression location where present and otherwise -1 (the trimmed-location
/// fallback used repo-wide).
fn expr_location_of_node(node: &Node<'_>) -> PgResult<i32> {
    match node.as_expr() {
        Some(e) => expr_location(Some(e)),
        None => Ok(-1),
    }
}

// ===========================================================================
// Public API (matches parser/parse_cte.h).
// ===========================================================================

/// transformWithClause -
///   Transform the list of WITH clause "common table expressions" into
///   Query nodes.
///
/// The result is the list of transformed CTEs to be put into the output Query.
/// (This is in fact the same as the ending value of `p_ctenamespace`, but it
/// seems cleaner to not expose that in the function's API.)
///
/// Port target: `transformWithClause` (parse_cte.c:110).
pub fn transformWithClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    withClause: WithClause<'mcx>,
) -> PgResult<PgVec<'mcx, CommonTableExpr<'mcx>>> {
    // Only one WITH clause per query level
    debug_assert!(pstate.p_ctenamespace.is_empty());
    debug_assert!(pstate.p_future_ctes.is_empty());

    // Move the CTEs out of the WithClause into an owned working vector.
    let mut ctes: Vec<CommonTableExpr<'mcx>> = Vec::new();
    ctes.try_reserve(withClause.ctes.len())
        .map_err(|_| out_of_memory())?;
    for node in withClause.ctes {
        let other = PgBox::into_inner(node);
        let other_tag = other.node_tag();
        match other.into_commontableexpr() {
            Some(cte) => ctes.push(cte),
            None => {
                return Err(elog_error(format!(
                    "WITH clause element is not a CommonTableExpr (node tag {:?})",
                    other_tag
                )));
            }
        }
    }

    // For either type of WITH, there must not be duplicate CTE names in the
    // list.  Check this right away so we needn't worry later.
    //
    // Also, tentatively mark each CTE as non-recursive, and initialize its
    // reference count to zero, and set pstate->p_hasModifyingCTE if needed.
    for i in 0..ctes.len() {
        // for_each_cell(rest, withClause->ctes, lnext(withClause->ctes, lc))
        for j in (i + 1)..ctes.len() {
            if cte_name(&ctes[i]) == cte_name(&ctes[j]) {
                let name = cte_name(&ctes[j]).to_string();
                let location = ctes[j].location;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_ALIAS)
                    .errmsg(format!(
                        "WITH query name \"{name}\" specified more than once"
                    ))
                    .errposition(parser_errposition(pstate, location)?)
                    .into_error());
            }
        }

        ctes[i].cterecursive = false;
        ctes[i].cterefcount = 0;

        if !is_select_stmt(ctes[i].ctequery.as_deref()) {
            // must be a data-modifying statement
            debug_assert!(is_data_modifying_stmt(ctes[i].ctequery.as_deref()));
            pstate.p_hasModifyingCTE = true;
        }
    }

    if withClause.recursive {
        // For WITH RECURSIVE, we rearrange the list elements if needed to
        // eliminate forward references.  First, build a work array and set up
        // the data structure needed by the tree walkers.
        let numitems = ctes.len();
        let mut items: Vec<CteItem> = Vec::new();
        items.try_reserve(numitems).map_err(|_| out_of_memory())?;
        for i in 0..numitems {
            items.push(CteItem {
                cte: i,
                id: i as i32,
                depends_on: alloc::collections::BTreeSet::new(),
            });
        }
        let mut cstate = CteState {
            ctes,
            items,
            curitem: 0,
            innerwiths: Vec::new(),
            selfrefcount: 0,
            context: RecursionContext::Ok,
        };

        // Find all the dependencies and sort the CteItems into a safe
        // processing order.  Also, mark CTEs that contain self-references.
        makeDependencyGraph(mcx, pstate, &mut cstate)?;

        // Check that recursive queries are well-formed.
        checkWellFormedRecursion(mcx, pstate, &mut cstate)?;

        // Set up the ctenamespace for parse analysis.  Per spec, all the WITH
        // items are visible to all others, so stuff them all in before parse
        // analysis.  We build the list in safe processing order so that the
        // planner can process the queries in sequence.
        //
        // Reorder the owned CTEs to match the topological item order.
        let order: Vec<usize> = cstate.items.iter().map(|it| it.cte).collect();
        let mut slots: Vec<Option<CommonTableExpr<'mcx>>> =
            cstate.ctes.into_iter().map(Some).collect();
        let mut ordered: Vec<CommonTableExpr<'mcx>> = Vec::new();
        ordered.try_reserve(order.len()).map_err(|_| out_of_memory())?;
        for &idx in &order {
            ordered.push(
                slots[idx]
                    .take()
                    .ok_or_else(|| elog_error("topological order references a CTE twice"))?,
            );
        }

        // Stuff them all into the ctenamespace before analysis (forward
        // visibility), then analyze in the topological order.
        //
        // In C the ctenamespace holds pointers to the very CTE objects that
        // `analyzeCTE` mutates in place, so a later CTE's analysis observes an
        // earlier CTE already carrying its analyzed `ctecolnames`/types. The
        // owned model holds clones, so after analyzing each CTE we refresh its
        // ctenamespace slot (by `ctename`) with the analyzed value — preserving
        // that same forward-visibility-of-analyzed-columns invariant.
        for cte in &ordered {
            let entry = cte.clone_in(mcx)?;
            pgvec_push(&mut pstate.p_ctenamespace, entry)?;
        }

        let mut analyzed: PgVec<'mcx, CommonTableExpr<'mcx>> = PgVec::new_in(mcx);
        analyzed.try_reserve(ordered.len()).map_err(|_| out_of_memory())?;
        for mut cte in ordered {
            analyzeCTE(mcx, pstate, &mut cte)?;
            refresh_ctenamespace_entry(mcx, pstate, &cte)?;
            analyzed.push(cte);
        }

        Ok(analyzed)
    } else {
        // For non-recursive WITH, just analyze each CTE in sequence and then
        // add it to the ctenamespace.  This corresponds to the spec's
        // definition of the scope of each WITH name.  However, to allow error
        // reports to be aware of the possibility of an erroneous reference,
        // we maintain a list in p_future_ctes of the not-yet-visible CTEs.
        pstate.p_future_ctes.clear();
        pstate
            .p_future_ctes
            .try_reserve(ctes.len())
            .map_err(|_| out_of_memory())?;
        for cte in &ctes {
            let entry = cte.clone_in(mcx)?;
            pstate.p_future_ctes.push(entry);
        }

        let mut analyzed: PgVec<'mcx, CommonTableExpr<'mcx>> = PgVec::new_in(mcx);
        analyzed.try_reserve(ctes.len()).map_err(|_| out_of_memory())?;
        for mut cte in ctes {
            analyzeCTE(mcx, pstate, &mut cte)?;
            let entry = cte.clone_in(mcx)?;
            pgvec_push(&mut pstate.p_ctenamespace, entry)?;
            analyzed.push(cte);

            // list_delete_first(p_future_ctes)
            if !pstate.p_future_ctes.is_empty() {
                pstate.p_future_ctes.remove(0);
            }
        }

        Ok(analyzed)
    }
}

/// Perform the actual parse analysis transformation of one CTE.  All CTEs it
/// depends on have already been loaded into pstate->p_ctenamespace, and have
/// been marked with the correct output column names/types.
///
/// Port target: `analyzeCTE` (parse_cte.c:243).
fn analyzeCTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cte: &mut CommonTableExpr<'mcx>,
) -> PgResult<()> {
    // Analysis not done already
    debug_assert!(!is_query(cte.ctequery.as_deref()));

    // Before analyzing the CTE's query, we'd better identify the data type of
    // the cycle mark column if any, since the query could refer to that.
    // Other validity checks on the cycle clause will be done afterwards.
    if let Some(cycle_box) = cte.cycle_clause.take() {
        let mut cycle_clause: CTECycleClause<'mcx> = PgBox::into_inner(cycle_box)
            .into_cte_cycle_clause()
            .ok_or_else(|| elog_error("cycle_clause is not a CTECycleClause node"))?;

        // cycle_mark_value = transformExpr(EXPR_KIND_CYCLE_MARK)
        let mut mark_value = transform_cycle_expr(pstate, cycle_clause.cycle_mark_value.take())?;
        let mut mark_default =
            transform_cycle_expr(pstate, cycle_clause.cycle_mark_default.take())?;

        // cycle_mark_type = select_common_type(list_make2(value, default))
        cycle_clause.cycle_mark_type = {
            let exprs = list_make2_exprs(&mark_value, &mark_default);
            backend_parser_coerce::select_common_type(Some(pstate), &exprs, Some("CYCLE"))?
        };

        // cycle_mark_value = coerce_to_common_type(value, type, "CYCLE/SET/TO")
        mark_value = Some(backend_parser_coerce::coerce_to_common_type(
            mcx,
            Some(pstate),
            expect_expr(mark_value)?,
            cycle_clause.cycle_mark_type,
            "CYCLE/SET/TO",
        )?);
        // cycle_mark_default = coerce_to_common_type(default, type, "CYCLE/SET/DEFAULT")
        mark_default = Some(backend_parser_coerce::coerce_to_common_type(
            mcx,
            Some(pstate),
            expect_expr(mark_default)?,
            cycle_clause.cycle_mark_type,
            "CYCLE/SET/DEFAULT",
        )?);

        // cycle_mark_typmod = select_common_typmod(list_make2(value, default), type)
        cycle_clause.cycle_mark_typmod = {
            let exprs = list_make2_exprs(&mark_value, &mark_default);
            backend_parser_coerce::select_common_typmod(&exprs, cycle_clause.cycle_mark_type)?
        };

        // cycle_mark_collation = select_common_collation(list_make2(value, default), true)
        cycle_clause.cycle_mark_collation = {
            let mut exprs = list_make2_exprs(&mark_value, &mark_default);
            let coll =
                backend_parser_parse_collate::select_common_collation(Some(pstate), &mut exprs, true)?;
            // select_common_collation mutates the exprs in place (exprSetCollation
            // through the walker); copy the (possibly updated) exprs back so the
            // stored cycle-mark nodes carry the assigned collations, mirroring
            // the C pointer aliasing.
            store_back_exprs(&mut mark_value, &mut mark_default, exprs);
            coll
        };

        // Might as well look up the relevant <> operator while we are at it
        let eq_opr = backend_utils_cache_typcache_seams::lookup_type_cache_eq_opr::call(
            cycle_clause.cycle_mark_type,
        )?;
        if eq_opr == InvalidOid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!(
                    "could not identify an equality operator for type {}",
                    format_type_be(mcx, cycle_clause.cycle_mark_type)?
                ))
                .into_error());
        }
        let op = backend_utils_cache_lsyscache::opfamily_operator::get_negator(eq_opr)?;
        if op == InvalidOid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!(
                    "could not identify an inequality operator for type {}",
                    format_type_be(mcx, cycle_clause.cycle_mark_type)?
                ))
                .into_error());
        }
        cycle_clause.cycle_mark_neop = op;

        // Re-wrap the analyzed exprs as Node::Expr for storage.
        cycle_clause.cycle_mark_value = wrap_expr_node(mcx, mark_value)?;
        cycle_clause.cycle_mark_default = wrap_expr_node(mcx, mark_default)?;

        cte.cycle_clause = Some(mcx::alloc_in(
            mcx,
            types_nodes::nodes::Node::mk_cte_cycle_clause(mcx, cycle_clause),
        )?);
    }

    // Now we can get on with analyzing the CTE's query
    let ctequery = cte
        .ctequery
        .take()
        .ok_or_else(|| elog_error("CTE has no query"))?;
    let query_node =
        backend_parser_analyze_seams::parse_sub_analyze::call(mcx, &ctequery, pstate, Some(&*cte), false, true)?;
    // cte->ctequery = (Node *) query;
    cte.ctequery = Some(query_node);

    // Check that we got something reasonable.  These first two cases should be
    // prevented by the grammar.
    if !is_query(cte.ctequery.as_deref()) {
        return Err(elog_error("unexpected non-Query statement in WITH"));
    }
    {
        let q = query_of(cte.ctequery.as_deref())
            .ok_or_else(|| elog_error("CTE query is not a Query"))?;
        if q.utilityStmt.is_some() {
            return Err(elog_error("unexpected utility statement in WITH"));
        }
        // We disallow data-modifying WITH except at the top level of a query,
        // because it's not clear when such a modification should be executed.
        if q.commandType != CmdType::CMD_SELECT && pstate.parentParseState.is_some() {
            let location = cte.location;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "WITH clause containing a data-modifying statement must be at the top level",
                )
                .errposition(parser_errposition(pstate, location)?)
                .into_error());
        }
    }

    // CTE queries are always marked not canSetTag.  (Currently this only
    // matters for data-modifying statements, for which the flag will be
    // propagated to the ModifyTable plan node.)
    if let Some(q) = cte.ctequery.as_deref_mut().and_then(|n| n.as_query_mut()) {
        q.canSetTag = false;
    }

    if !cte.cterecursive {
        // Compute the output column names/types if not done yet
        let tlist = get_cte_target_list(mcx, cte)?;
        analyzeCTETargetList(mcx, pstate, cte, &tlist)?;
    } else {
        // For a recursive CTE the output columns were determined by
        // `determineRecursiveColTypes` (analyze.c) while transforming the
        // non-recursive term.  In C that mutated this very `cte` (it aliases
        // `pstate->p_parent_cte`); the owned model handed the sub-analysis a
        // clone, so re-derive the columns here from the analyzed query's
        // non-recursive (leftmost) term before verifying — leaving `cte` in the
        // same state C would have at this point.
        if cte.ctecolnames.is_empty() {
            let nr_tlist = nonrecursive_term_targetlist(mcx, cte)?;
            analyzeCTETargetList(mcx, pstate, cte, &nr_tlist)?;
        }

        // Verify that the previously determined output column types and
        // collations match what the query really produced.  We have to check
        // this because the recursive term could have overridden the
        // non-recursive term, and we don't have any easy way to fix that.
        let tlist = get_cte_target_list(mcx, cte)?;
        let mut iter_typ = cte.ctecoltypes.iter();
        let mut iter_typmod = cte.ctecoltypmods.iter();
        let mut iter_coll = cte.ctecolcollations.iter();
        let mut varattno = 0;

        for te in &tlist {
            if te.resjunk {
                continue;
            }
            varattno += 1;
            debug_assert_eq!(varattno, te.resno as i32);

            let (Some(&typ_oid), Some(&typmod_val), Some(&coll_oid)) =
                (iter_typ.next(), iter_typmod.next(), iter_coll.next())
            else {
                // shouldn't happen
                return Err(elog_error("wrong number of output columns in WITH"));
            };

            let texpr = te.expr.as_deref();
            let actual_type = expr_type(texpr)?;
            let actual_typmod = expr_typmod(texpr)?;
            if actual_type != typ_oid || actual_typmod != typmod_val {
                let name = cte_name(cte).to_string();
                let loc = expr_location(texpr)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "recursive query \"{}\" column {} has type {} in non-recursive term but type {} overall",
                        name,
                        varattno,
                        format_type_with_typemod(mcx, typ_oid, typmod_val)?,
                        format_type_with_typemod(mcx, actual_type, actual_typmod)?,
                    ))
                    .errhint("Cast the output of the non-recursive term to the correct type.")
                    .errposition(parser_errposition(pstate, loc)?)
                    .into_error());
            }
            let actual_coll = expr_collation(texpr)?;
            if actual_coll != coll_oid {
                let name = cte_name(cte).to_string();
                let loc = expr_location(texpr)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_COLLATION_MISMATCH)
                    .errmsg(format!(
                        "recursive query \"{}\" column {} has collation \"{}\" in non-recursive term but collation \"{}\" overall",
                        name,
                        varattno,
                        collation_name_or_null(mcx, coll_oid)?,
                        collation_name_or_null(mcx, actual_coll)?,
                    ))
                    .errhint("Use the COLLATE clause to set the collation of the non-recursive term.")
                    .errposition(parser_errposition(pstate, loc)?)
                    .into_error());
            }
        }
        if iter_typ.next().is_some() || iter_typmod.next().is_some() || iter_coll.next().is_some() {
            // shouldn't happen
            return Err(elog_error("wrong number of output columns in WITH"));
        }
    }

    // Now make validity checks on the SEARCH and CYCLE clauses, if present.
    let has_search = cte.search_clause.is_some();
    let has_cycle = cte.cycle_clause.is_some();

    if has_search || has_cycle {
        if !cte.cterecursive {
            let location = cte.location;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("WITH query is not recursive")
                .errposition(parser_errposition(pstate, location)?)
                .into_error());
        }

        // SQL requires a WITH list element (CTE) to be "expandable" in order to
        // allow a search or cycle clause.  That is a stronger requirement than
        // just being recursive.  It basically means the query expression looks
        // like
        //
        // non-recursive query UNION [ALL] recursive query
        //
        // and that the recursive query is not itself a set operation.
        let q = query_of(cte.ctequery.as_deref())
            .ok_or_else(|| elog_error("CTE query is not a Query"))?;
        let sos = match q.setOperations.as_deref().and_then(|n| n.as_setoperationstmt()) {
            Some(s) => s,
            None => return Err(elog_error("CTE has no set operations")),
        };

        // This left side check is not required for expandability, but
        // rewriteSearchAndCycle() doesn't currently have support for it, so we
        // catch it here.
        if !is_range_tbl_ref(sos.larg.as_deref()) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT",
                )
                .into_error());
        }

        if !is_range_tbl_ref(sos.rarg.as_deref()) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(
                    "with a SEARCH or CYCLE clause, the right side of the UNION must be a SELECT",
                )
                .into_error());
        }
    }

    if let Some(search_clause) = cte.search_clause.as_deref() {
        let mut seen: Vec<String> = Vec::new();

        for colname_node in search_clause.search_col_list.iter() {
            let colname = str_val(colname_node);
            if !cte.ctecolnames.iter().any(|c| str_val(c) == colname) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "search column \"{colname}\" not in WITH query column list"
                    ))
                    .errposition(parser_errposition(pstate, search_clause.location)?)
                    .into_error());
            }

            if seen.iter().any(|s| s == colname) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!(
                        "search column \"{colname}\" specified more than once"
                    ))
                    .errposition(parser_errposition(pstate, search_clause.location)?)
                    .into_error());
            }
            push_checked(&mut seen, colname.to_string())?;
        }

        let seq = search_clause
            .search_seq_column
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("");
        if cte.ctecolnames.iter().any(|c| str_val(c) == seq) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "search sequence column name \"{seq}\" already used in WITH query column list"
                ))
                .errposition(parser_errposition(pstate, search_clause.location)?)
                .into_error());
        }
    }

    if let Some(cycle_clause) = cte.cycle_clause.as_deref().and_then(|n| n.as_cte_cycle_clause()) {
        let mut seen: Vec<String> = Vec::new();

        for colname_node in cycle_clause.cycle_col_list.iter() {
            let colname = str_val(colname_node);
            if !cte.ctecolnames.iter().any(|c| str_val(c) == colname) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "cycle column \"{colname}\" not in WITH query column list"
                    ))
                    .errposition(parser_errposition(pstate, cycle_clause.location)?)
                    .into_error());
            }

            if seen.iter().any(|s| s == colname) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!(
                        "cycle column \"{colname}\" specified more than once"
                    ))
                    .errposition(parser_errposition(pstate, cycle_clause.location)?)
                    .into_error());
            }
            push_checked(&mut seen, colname.to_string())?;
        }

        let mark = cycle_clause
            .cycle_mark_column
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("");
        if cte.ctecolnames.iter().any(|c| str_val(c) == mark) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "cycle mark column name \"{mark}\" already used in WITH query column list"
                ))
                .errposition(parser_errposition(pstate, cycle_clause.location)?)
                .into_error());
        }

        let path = cycle_clause
            .cycle_path_column
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("");
        if cte.ctecolnames.iter().any(|c| str_val(c) == path) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "cycle path column name \"{path}\" already used in WITH query column list"
                ))
                .errposition(parser_errposition(pstate, cycle_clause.location)?)
                .into_error());
        }

        if mark == path {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("cycle mark column name and cycle path column name are the same")
                .errposition(parser_errposition(pstate, cycle_clause.location)?)
                .into_error());
        }
    }

    if has_search && has_cycle {
        let search_clause = cte
            .search_clause
            .as_deref()
            .ok_or_else(|| elog_error("search_clause missing"))?;
        let cycle_clause = cte
            .cycle_clause
            .as_deref()
            .and_then(|n| n.as_cte_cycle_clause())
            .ok_or_else(|| elog_error("cycle_clause missing"))?;
        let seq = search_clause
            .search_seq_column
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("");
        let mark = cycle_clause
            .cycle_mark_column
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("");
        let path = cycle_clause
            .cycle_path_column
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("");

        if seq == mark {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("search sequence column name and cycle mark column name are the same")
                .errposition(parser_errposition(pstate, search_clause.location)?)
                .into_error());
        }

        if seq == path {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("search sequence column name and cycle path column name are the same")
                .errposition(parser_errposition(pstate, search_clause.location)?)
                .into_error());
        }
    }

    Ok(())
}

/// Compute derived fields of a CTE, given the transformed output targetlist.
///
/// For a nonrecursive CTE, this is called after transforming the CTE's query.
/// For a recursive CTE, we call it after transforming the non-recursive term,
/// and pass the targetlist emitted by the non-recursive term only.
///
/// Note: in the recursive case, the passed pstate is actually the one being used
/// to analyze the CTE's query, so it is one level lower down than in the
/// nonrecursive case.  This doesn't matter since we only use it for error
/// message context anyway.
///
/// Port target: `analyzeCTETargetList` (parse_cte.c:571).
pub fn analyzeCTETargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cte: &mut CommonTableExpr<'mcx>,
    tlist: &[TargetEntry<'mcx>],
) -> PgResult<()> {
    // Not done already ...
    debug_assert!(cte.ctecolnames.is_empty());

    // We need to determine column names, types, and collations.  The alias
    // column names override anything coming from the query itself.  (Note: the
    // SQL spec says that the alias list must be empty or exactly as long as the
    // output column set; but we allow it to be shorter for consistency with
    // Alias handling.)
    //
    // cte->ctecolnames = copyObject(cte->aliascolnames)
    let mut ctecolnames: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
    ctecolnames
        .try_reserve(cte.aliascolnames.len())
        .map_err(|_| out_of_memory())?;
    for n in cte.aliascolnames.iter() {
        ctecolnames.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    cte.ctecolnames = ctecolnames;
    cte.ctecoltypes.clear();
    cte.ctecoltypmods.clear();
    cte.ctecolcollations.clear();
    let numaliases = cte.aliascolnames.len() as i32;
    let mut varattno: i32 = 0;

    for te in tlist {
        if te.resjunk {
            continue;
        }
        varattno += 1;
        debug_assert_eq!(varattno, te.resno as i32);
        if varattno > numaliases {
            let attrname = te.resname.as_ref().map(|s| s.as_str()).unwrap_or("");
            let node = make_string_node(mcx, attrname)?;
            pgvec_push(&mut cte.ctecolnames, node)?;
        }
        let texpr = te.expr.as_deref();
        let mut coltype = expr_type(texpr)?;
        let mut coltypmod = expr_typmod(texpr)?;
        let mut colcoll = expr_collation(texpr)?;

        // If the CTE is recursive, force the exposed column type of any
        // "unknown" column to "text".  We must deal with this here because we're
        // called on the non-recursive term before there's been any attempt to
        // force unknown output columns to some other type.  We have to resolve
        // unknowns before looking at the recursive term.
        //
        // The column might contain 'foo' COLLATE "bar", so don't override
        // collation if it's already set.
        if cte.cterecursive && coltype == UNKNOWNOID {
            coltype = TEXTOID;
            coltypmod = -1; // should be -1 already, but be sure
            if colcoll == InvalidOid {
                colcoll = DEFAULT_COLLATION_OID;
            }
        }
        pgvec_push(&mut cte.ctecoltypes, coltype)?;
        pgvec_push(&mut cte.ctecoltypmods, coltypmod)?;
        pgvec_push(&mut cte.ctecolcollations, colcoll)?;
    }
    if varattno < numaliases {
        let name = cte_name(cte).to_string();
        let location = cte.location;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "WITH query \"{name}\" has {varattno} columns available but {numaliases} columns specified"
            ))
            .errposition(parser_errposition(pstate, location)?)
            .into_error());
    }

    Ok(())
}

// ===========================================================================
// Cycle-mark expression helpers (the split Expr/Node bridge).
// ===========================================================================

/// `transformExpr(pstate, node, EXPR_KIND_CYCLE_MARK)` over an optional raw node
/// (the cycle-mark value / default). Returns the analyzed `Expr` (or `None`).
fn transform_cycle_expr<'mcx>(
    pstate: &mut ParseState<'mcx>,
    node: Option<PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<Option<Expr>> {
    let input = node.map(PgBox::into_inner);
    backend_parser_parse_expr::transformExpr(pstate, input, ParseExprKind::EXPR_KIND_CYCLE_MARK)
}

/// `list_make2(a, b)` of the cycle-mark value/default exprs, for the
/// `select_common_*` calls (which take `&[Expr]`). The C `list_make2` always
/// builds a two-element list; a `None` (a NULL cell post-transformExpr) is
/// dropped here, matching the coercion routines' NULL-cell handling (they treat
/// an absent element as UNKNOWN). Both are always `Some` at these call sites.
fn list_make2_exprs(a: &Option<Expr>, b: &Option<Expr>) -> Vec<Expr> {
    let mut v: Vec<Expr> = Vec::new();
    if let Some(x) = a {
        v.push(x.clone());
    }
    if let Some(y) = b {
        v.push(y.clone());
    }
    v
}

/// Copy the (possibly collation-updated) `select_common_collation` exprs back
/// into the value/default slots, mirroring the C in-place `exprSetCollation`
/// over the aliased pointers.
fn store_back_exprs(a: &mut Option<Expr>, b: &mut Option<Expr>, mut exprs: Vec<Expr>) {
    let mut it = exprs.drain(..);
    if a.is_some() {
        if let Some(x) = it.next() {
            *a = Some(x);
        }
    }
    if b.is_some() {
        if let Some(y) = it.next() {
            *b = Some(y);
        }
    }
}

/// Unwrap a `Some(Expr)` (the cycle-mark exprs are always present at the coerce
/// call sites; C dereferences the non-NULL transformed node).
fn expect_expr(e: Option<Expr>) -> PgResult<Expr> {
    e.ok_or_else(|| elog_error("cycle-mark expression is unexpectedly NULL"))
}

/// Re-wrap an analyzed `Expr` as a stored `Node::Expr` (the cycle-mark slots are
/// `Option<NodePtr>`).
fn wrap_expr_node<'mcx>(
    mcx: Mcx<'mcx>,
    e: Option<Expr>,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    match e {
        Some(expr) => Ok(Some(mcx::alloc_in(mcx, Node::Expr(expr))?)),
        None => Ok(None),
    }
}

// ===========================================================================
// Dependency processing functions.
// ===========================================================================

/// Identify the cross-references of a list of WITH RECURSIVE items, and sort
/// into an order that has no forward references.
///
/// Port target: `makeDependencyGraph` (parse_cte.c:648).
fn makeDependencyGraph<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<()> {
    for i in 0..cstate.numitems() {
        cstate.curitem = i;
        cstate.innerwiths = Vec::new();
        // Walk this CTE's query (read-only) updating the dependency graph.
        // Clone the subquery so the walk does not hold a borrow into `cstate`.
        let query = match cstate.item_cte(i).ctequery.as_deref() {
            Some(n) => Some(n.clone_in(mcx)?),
            None => None,
        };
        if let Some(q) = &query {
            makeDependencyGraphWalker(mcx, q, cstate)?;
        }
        debug_assert!(cstate.innerwiths.is_empty());
    }

    TopologicalSort(pstate, cstate)
}

/// Recurse into the children of `node` using the shared raw walker, propagating
/// the first error a child walk raises. Returns the C walker's `bool` (whether
/// the walk was aborted early). The children are cloned so the captured closure
/// can own the `&mut CteState` recursion.
fn raw_walk_children_dep<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    let children = collect_children(mcx, node)?;
    for child in &children {
        if makeDependencyGraphWalker(mcx, child, cstate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Collect the immediate children the raw walker would visit (cloned), so the
/// recursion can thread `&mut`-state through them.
fn collect_children<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
) -> PgResult<Vec<Node<'mcx>>> {
    let mut children: Vec<Node<'mcx>> = Vec::new();
    let mut collect_err: PgResult<()> = Ok(());
    raw_expression_tree_walker(node, &mut |child| {
        if collect_err.is_err() {
            return true;
        }
        match child.clone_in(mcx) {
            Ok(c) => {
                if children.try_reserve(1).is_err() {
                    collect_err = Err(out_of_memory());
                    return true;
                }
                children.push(c);
                false
            }
            Err(e) => {
                collect_err = Err(e);
                true
            }
        }
    });
    collect_err?;
    Ok(children)
}

/// Tree walker function to detect cross-references and self-references of the
/// CTEs in a WITH RECURSIVE list.
///
/// Port target: `makeDependencyGraphWalker` (parse_cte.c:670).
fn makeDependencyGraphWalker<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    match node.node_tag() {
        ntag::T_RangeVar => {
            let rv = node.expect_rangevar();
            // If unqualified name, might be a CTE reference
            if rv.schemaname.is_none() {
                let relname = rv.relname.as_ref().map(|s| s.as_str()).unwrap_or("");

                // ... but first see if it's captured by an inner WITH
                for withlist in &cstate.innerwiths {
                    for name in withlist {
                        if relname == name {
                            return Ok(false); // yes, so bail out
                        }
                    }
                }

                // No, could be a reference to the query level we are working on
                for i in 0..cstate.numitems() {
                    if relname == cte_name(cstate.item_cte(i)) {
                        let myindex = cstate.curitem;
                        if i != myindex {
                            // Add cross-item dependency
                            let dep_id = cstate.items[i].id;
                            cstate.items[myindex].depends_on.insert(dep_id);
                        } else {
                            // Found out this one is self-referential
                            let cte_idx = cstate.items[i].cte;
                            cstate.ctes[cte_idx].cterecursive = true;
                        }
                        break;
                    }
                }
            }
            Ok(false)
        }
        ntag::T_SelectStmt => {
            let stmt = node.expect_selectstmt();
            if stmt.withClause.is_some() {
                // Examine the WITH clause and the SelectStmt
                WalkInnerWith(mcx, node, cstate, WalkerKind::Dependency)?;
                // We're done examining the SelectStmt
                return Ok(false);
            }
            // if no WITH clause, just fall through for normal processing
            raw_walk_children_dep(mcx, node, cstate)
        }
        ntag::T_InsertStmt => {
            let stmt = node.expect_insertstmt();
            if stmt.withClause.is_some() {
                WalkInnerWith(mcx, node, cstate, WalkerKind::Dependency)?;
                return Ok(false);
            }
            raw_walk_children_dep(mcx, node, cstate)
        }
        ntag::T_DeleteStmt => {
            let stmt = node.expect_deletestmt();
            if stmt.withClause.is_some() {
                WalkInnerWith(mcx, node, cstate, WalkerKind::Dependency)?;
                return Ok(false);
            }
            raw_walk_children_dep(mcx, node, cstate)
        }
        ntag::T_UpdateStmt => {
            let stmt = node.expect_updatestmt();
            if stmt.withClause.is_some() {
                WalkInnerWith(mcx, node, cstate, WalkerKind::Dependency)?;
                return Ok(false);
            }
            raw_walk_children_dep(mcx, node, cstate)
        }
        ntag::T_MergeStmt => {
            let stmt = node.expect_mergestmt();
            if stmt.withClause.is_some() {
                WalkInnerWith(mcx, node, cstate, WalkerKind::Dependency)?;
                return Ok(false);
            }
            raw_walk_children_dep(mcx, node, cstate)
        }
        ntag::T_WithClause => {
            // Prevent raw_expression_tree_walker from recursing directly into a
            // WITH clause.  We need that to happen only under the control of the
            // code above.
            Ok(false)
        }
        _ => raw_walk_children_dep(mcx, node, cstate),
    }
}

/// Which walker the [`WalkInnerWith`] recursion should drive: the dependency
/// graph walker or the well-formedness recursion walker. (The C code passes the
/// walker fn pointer; here the two walkers have different `&mut ParseState`
/// needs, so we dispatch by kind.)
#[derive(Clone, Copy)]
enum WalkerKind {
    Dependency,
}

/// makeDependencyGraphWalker's recursion into a statement having a WITH clause.
///
/// This subroutine is concerned with updating the innerwiths list correctly
/// based on the visibility rules for CTE names.
///
/// Port target: `WalkInnerWith` (parse_cte.c:812).
fn WalkInnerWith<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
    cstate: &mut CteState<'mcx>,
    kind: WalkerKind,
) -> PgResult<()> {
    let wc = stmt_with_clause(stmt).ok_or_else(|| elog_error("WalkInnerWith: no WITH clause"))?;
    let recursive = wc.recursive;
    // Snapshot the inner CTEs (their names + queries) up front; the borrow of
    // `stmt` ends here so the recursive walks can take `&mut cstate`.
    let inner_names: Vec<String> = cte_names_of(wc);
    let mut inner_queries: Vec<Option<Node<'mcx>>> = Vec::new();
    inner_queries
        .try_reserve(wc.ctes.len())
        .map_err(|_| out_of_memory())?;
    for c in wc.ctes.iter() {
        let q = match c.as_commontableexpr() {
            Some(cte) => match cte.ctequery.as_deref() {
                Some(n) => Some(n.clone_in(mcx)?),
                None => None,
            },
            None => None,
        };
        inner_queries.push(q);
    }
    let stmt_owned = stmt.clone_in(mcx)?;

    let WalkerKind::Dependency = kind;

    if recursive {
        // In the RECURSIVE case, all query names of the WITH are visible to all
        // WITH items as well as the main query.  So push them all on, process,
        // pop them all off.
        cstate.innerwiths.insert(0, inner_names);
        for q in inner_queries.iter().flatten() {
            makeDependencyGraphWalker(mcx, q, cstate)?;
        }
        raw_walk_children_dep(mcx, &stmt_owned, cstate)?;
        cstate.innerwiths.remove(0);
    } else {
        // In the non-RECURSIVE case, query names are visible to the WITH items
        // after them and to the main query.
        cstate.innerwiths.insert(0, Vec::new());
        for (idx, q) in inner_queries.iter().enumerate() {
            if let Some(q) = q {
                makeDependencyGraphWalker(mcx, q, cstate)?;
            }
            // note that recursion could mutate innerwiths list
            // cell1 = list_head(cstate->innerwiths);
            // lfirst(cell1) = lappend((List *) lfirst(cell1), cte);
            if let Some(name) = inner_names.get(idx) {
                let head = &mut cstate.innerwiths[0];
                push_checked(head, name.clone())?;
            }
        }
        raw_walk_children_dep(mcx, &stmt_owned, cstate)?;
        cstate.innerwiths.remove(0);
    }
    Ok(())
}

/// Sort by dependencies, using a standard topological sort operation.
///
/// Port target: `TopologicalSort` (parse_cte.c:863).
fn TopologicalSort<'mcx>(
    pstate: &mut ParseState<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<()> {
    let numitems = cstate.numitems();

    // for each position in sequence ...
    for i in 0..numitems {
        // ... scan the remaining items to find one that has no dependencies
        let mut j = i;
        while j < numitems {
            if cstate.items[j].depends_on.is_empty() {
                break;
            }
            j += 1;
        }

        // if we didn't find one, the dependency graph has a cycle
        if j >= numitems {
            let location = cstate.item_cte(i).location;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("mutual recursion between WITH items is not implemented")
                .errposition(parser_errposition(pstate, location)?)
                .into_error());
        }

        // Found one.  Move it to front and remove it from every other item's
        // dependencies.
        if i != j {
            cstate.items.swap(i, j);
        }

        // Items up through i are known to have no dependencies left, so we can
        // skip them in this loop.
        let removed_id = cstate.items[i].id;
        for k in (i + 1)..numitems {
            cstate.items[k].depends_on.remove(&removed_id);
        }
    }

    Ok(())
}

// ===========================================================================
// Recursion validity checker functions.
// ===========================================================================

/// Check that recursive queries are well-formed.
///
/// Port target: `checkWellFormedRecursion` (parse_cte.c:915).
fn checkWellFormedRecursion<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<()> {
    for i in 0..cstate.numitems() {
        // not analyzed yet
        debug_assert!(!is_query(cstate.item_cte(i).ctequery.as_deref()));

        // Ignore items that weren't found to be recursive
        if !cstate.item_cte(i).cterecursive {
            continue;
        }

        // Must be a SELECT statement
        let ctequery = match cstate.item_cte(i).ctequery.as_deref() {
            Some(n) => n.clone_in(mcx)?,
            None => {
                return Err(elog_error("recursive CTE has no query"));
            }
        };
        let stmt: SelectStmt<'mcx> = match ctequery.node_tag() {
            ntag::T_SelectStmt => ctequery.expect_selectstmt().clone_in(mcx)?,
            _ => {
                let name = cte_name(cstate.item_cte(i)).to_string();
                let location = cstate.item_cte(i).location;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_RECURSION)
                    .errmsg(format!(
                        "recursive query \"{name}\" must not contain data-modifying statements"
                    ))
                    .errposition(parser_errposition(pstate, location)?)
                    .into_error());
            }
        };

        // Must have top-level UNION
        if stmt.op != SetOperation::SETOP_UNION {
            let name = cte_name(cstate.item_cte(i)).to_string();
            let location = cstate.item_cte(i).location;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_RECURSION)
                .errmsg(format!(
                    "recursive query \"{name}\" does not have the form non-recursive-term UNION [ALL] recursive-term"
                ))
                .errposition(parser_errposition(pstate, location)?)
                .into_error());
        }

        // Really, we should insist that there not be a top-level WITH, since
        // syntactically that would enclose the UNION.  However, we've not done
        // so in the past and it's probably too late to change.  Settle for
        // insisting that WITH not contain a self-reference.  Test this before
        // examining the UNION arms, to avoid issuing confusing errors in such
        // cases.
        if let Some(w) = stmt.withClause.as_deref() {
            cstate.curitem = i;
            cstate.innerwiths = Vec::new();
            cstate.selfrefcount = 0;
            cstate.context = RecursionContext::Sublink;
            // Walk the WITH's ctes list (a list of CommonTableExprs).
            for cte_node in w.ctes.iter() {
                let cloned = cte_node.clone_in(mcx)?;
                checkWellFormedRecursionWalker(mcx, pstate, &cloned, cstate)?;
            }
            debug_assert!(cstate.innerwiths.is_empty());
        }

        // Disallow ORDER BY and similar decoration atop the UNION.
        if !stmt.sortClause.is_empty() {
            let loc = expr_location_of_list(&stmt.sortClause)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("ORDER BY in a recursive query is not implemented")
                .errposition(parser_errposition(pstate, loc)?)
                .into_error());
        }
        if let Some(off) = stmt.limitOffset.as_deref() {
            let loc = expr_location_of_node(off)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("OFFSET in a recursive query is not implemented")
                .errposition(parser_errposition(pstate, loc)?)
                .into_error());
        }
        if let Some(cnt) = stmt.limitCount.as_deref() {
            let loc = expr_location_of_node(cnt)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("LIMIT in a recursive query is not implemented")
                .errposition(parser_errposition(pstate, loc)?)
                .into_error());
        }
        if !stmt.lockingClause.is_empty() {
            let loc = expr_location_of_list(&stmt.lockingClause)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("FOR UPDATE/SHARE in a recursive query is not implemented")
                .errposition(parser_errposition(pstate, loc)?)
                .into_error());
        }

        // Now we can get on with checking the UNION operands themselves.
        //
        // The left-hand operand mustn't contain a self-reference at all.
        cstate.curitem = i;
        cstate.innerwiths = Vec::new();
        cstate.selfrefcount = 0;
        cstate.context = RecursionContext::NonRecursiveTerm;
        if let Some(larg) = stmt.larg.as_deref() {
            let node = Node::mk_select_stmt(mcx, larg.clone_in(mcx)?);
            checkWellFormedRecursionWalker(mcx, pstate, &node, cstate)?;
        }
        debug_assert!(cstate.innerwiths.is_empty());

        // Right-hand operand should contain one reference in a valid place
        cstate.curitem = i;
        cstate.innerwiths = Vec::new();
        cstate.selfrefcount = 0;
        cstate.context = RecursionContext::Ok;
        if let Some(rarg) = stmt.rarg.as_deref() {
            let node = Node::mk_select_stmt(mcx, rarg.clone_in(mcx)?);
            checkWellFormedRecursionWalker(mcx, pstate, &node, cstate)?;
        }
        debug_assert!(cstate.innerwiths.is_empty());
        if cstate.selfrefcount != 1 {
            // shouldn't happen
            return Err(elog_error("missing recursive reference"));
        }
    }

    Ok(())
}

/// Tree walker function to detect invalid self-references in a recursive query.
///
/// Port target: `checkWellFormedRecursionWalker` (parse_cte.c:1027).
fn checkWellFormedRecursionWalker<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    node: &Node<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    let save_context = cstate.context;

    match node.node_tag() {
        ntag::T_RangeVar => {
            let rv = node.expect_rangevar();
            // If unqualified name, might be a CTE reference
            if rv.schemaname.is_none() {
                let relname = rv.relname.as_ref().map(|s| s.as_str()).unwrap_or("");

                // ... but first see if it's captured by an inner WITH
                for withlist in &cstate.innerwiths {
                    for name in withlist {
                        if relname == name {
                            return Ok(false); // yes, so bail out
                        }
                    }
                }

                // No, could be a reference to the query level we are working on
                let mycte_name = cte_name(cstate.item_cte(cstate.curitem)).to_string();
                if relname == mycte_name {
                    // Found a recursive reference to the active query
                    if cstate.context != RecursionContext::Ok {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_RECURSION)
                            .errmsg(recursion_errormsg(cstate.context, &mycte_name))
                            .errposition(parser_errposition(pstate, rv.location)?)
                            .into_error());
                    }
                    // Count references
                    cstate.selfrefcount += 1;
                    if cstate.selfrefcount > 1 {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_RECURSION)
                            .errmsg(format!(
                                "recursive reference to query \"{mycte_name}\" must not appear more than once"
                            ))
                            .errposition(parser_errposition(pstate, rv.location)?)
                            .into_error());
                    }
                }
            }
            Ok(false)
        }
        ntag::T_SelectStmt => {
            let stmt = node.expect_selectstmt();
            if let Some(w) = stmt.withClause.as_deref() {
                let inner_names: Vec<String> = cte_names_of(w);
                let mut inner_queries: Vec<Option<Node<'mcx>>> = Vec::new();
                inner_queries
                    .try_reserve(w.ctes.len())
                    .map_err(|_| out_of_memory())?;
                for c in w.ctes.iter() {
                    let q = match c.as_commontableexpr() {
                        Some(cte) => match cte.ctequery.as_deref() {
                            Some(n) => Some(n.clone_in(mcx)?),
                            None => None,
                        },
                        None => None,
                    };
                    inner_queries.push(q);
                }
                // Snapshot the SelectStmt-without-WITH for the subroutine.
                let stmt_clone = stmt.clone_in(mcx)?;
                if w.recursive {
                    // RECURSIVE: all WITH names visible to all items + main query.
                    cstate.innerwiths.insert(0, inner_names);
                    for q in inner_queries.iter().flatten() {
                        checkWellFormedRecursionWalker(mcx, pstate, q, cstate)?;
                    }
                    checkWellFormedSelectStmt(mcx, pstate, &stmt_clone, cstate)?;
                    cstate.innerwiths.remove(0);
                } else {
                    // non-RECURSIVE: names visible to later items + main query.
                    cstate.innerwiths.insert(0, Vec::new());
                    for (idx, q) in inner_queries.iter().enumerate() {
                        if let Some(q) = q {
                            checkWellFormedRecursionWalker(mcx, pstate, q, cstate)?;
                        }
                        if let Some(name) = inner_names.get(idx) {
                            let head = &mut cstate.innerwiths[0];
                            push_checked(head, name.clone())?;
                        }
                    }
                    checkWellFormedSelectStmt(mcx, pstate, &stmt_clone, cstate)?;
                    cstate.innerwiths.remove(0);
                }
            } else {
                let stmt_clone = stmt.clone_in(mcx)?;
                checkWellFormedSelectStmt(mcx, pstate, &stmt_clone, cstate)?;
            }
            // We're done examining the SelectStmt
            Ok(false)
        }
        ntag::T_WithClause => {
            // Prevent raw_expression_tree_walker from recursing directly into a
            // WITH clause.  We need that to happen only under the control of the
            // code above.
            Ok(false)
        }
        ntag::T_JoinExpr => {
            let j = node.expect_joinexpr();
            match j.jointype {
                JoinType::JOIN_INNER => {
                    walk_opt(mcx, pstate, j.larg.as_deref(), cstate)?;
                    walk_opt(mcx, pstate, j.rarg.as_deref(), cstate)?;
                    walk_opt(mcx, pstate, j.quals.as_deref(), cstate)?;
                }
                JoinType::JOIN_LEFT => {
                    walk_opt(mcx, pstate, j.larg.as_deref(), cstate)?;
                    if save_context == RecursionContext::Ok {
                        cstate.context = RecursionContext::OuterJoin;
                    }
                    walk_opt(mcx, pstate, j.rarg.as_deref(), cstate)?;
                    cstate.context = save_context;
                    walk_opt(mcx, pstate, j.quals.as_deref(), cstate)?;
                }
                JoinType::JOIN_FULL => {
                    if save_context == RecursionContext::Ok {
                        cstate.context = RecursionContext::OuterJoin;
                    }
                    walk_opt(mcx, pstate, j.larg.as_deref(), cstate)?;
                    walk_opt(mcx, pstate, j.rarg.as_deref(), cstate)?;
                    cstate.context = save_context;
                    walk_opt(mcx, pstate, j.quals.as_deref(), cstate)?;
                }
                JoinType::JOIN_RIGHT => {
                    if save_context == RecursionContext::Ok {
                        cstate.context = RecursionContext::OuterJoin;
                    }
                    walk_opt(mcx, pstate, j.larg.as_deref(), cstate)?;
                    cstate.context = save_context;
                    walk_opt(mcx, pstate, j.rarg.as_deref(), cstate)?;
                    walk_opt(mcx, pstate, j.quals.as_deref(), cstate)?;
                }
                other => {
                    return Err(elog_error(format!("unrecognized join type: {}", other as i32)));
                }
            }
            Ok(false)
        }
        ntag::T_SubLink => {
            let sl = node.expect_sublink();
            // we intentionally override outer context, since subquery is
            // independent
            cstate.context = RecursionContext::Sublink;
            walk_opt(mcx, pstate, sl.subselect.as_deref(), cstate)?;
            cstate.context = save_context;
            walk_opt(mcx, pstate, sl.testexpr.as_deref(), cstate)?;
            Ok(false)
        }
        _ => raw_walk_children_recursion(mcx, pstate, node, cstate),
    }
}

/// subroutine for checkWellFormedRecursionWalker: process a SelectStmt without
/// worrying about its WITH clause.
///
/// Port target: `checkWellFormedSelectStmt` (parse_cte.c:1207).
fn checkWellFormedSelectStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &SelectStmt<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<()> {
    let save_context = cstate.context;

    if save_context != RecursionContext::Ok {
        // just recurse without changing state
        let node = Node::mk_select_stmt(mcx, stmt.clone_in(mcx)?);
        raw_walk_children_recursion(mcx, pstate, &node, cstate)?;
    } else {
        match stmt.op {
            SetOperation::SETOP_NONE | SetOperation::SETOP_UNION => {
                let node = Node::mk_select_stmt(mcx, stmt.clone_in(mcx)?);
                raw_walk_children_recursion(mcx, pstate, &node, cstate)?;
            }
            SetOperation::SETOP_INTERSECT => {
                if stmt.all {
                    cstate.context = RecursionContext::Intersect;
                }
                walk_opt_select(mcx, pstate, stmt.larg.as_deref(), cstate)?;
                walk_opt_select(mcx, pstate, stmt.rarg.as_deref(), cstate)?;
                cstate.context = save_context;
                walk_list(mcx, pstate, &stmt.sortClause, cstate)?;
                walk_opt(mcx, pstate, stmt.limitOffset.as_deref(), cstate)?;
                walk_opt(mcx, pstate, stmt.limitCount.as_deref(), cstate)?;
                walk_list(mcx, pstate, &stmt.lockingClause, cstate)?;
                // stmt->withClause is intentionally ignored here
            }
            SetOperation::SETOP_EXCEPT => {
                if stmt.all {
                    cstate.context = RecursionContext::Except;
                }
                walk_opt_select(mcx, pstate, stmt.larg.as_deref(), cstate)?;
                cstate.context = RecursionContext::Except;
                walk_opt_select(mcx, pstate, stmt.rarg.as_deref(), cstate)?;
                cstate.context = save_context;
                walk_list(mcx, pstate, &stmt.sortClause, cstate)?;
                walk_opt(mcx, pstate, stmt.limitOffset.as_deref(), cstate)?;
                walk_opt(mcx, pstate, stmt.limitCount.as_deref(), cstate)?;
                walk_list(mcx, pstate, &stmt.lockingClause, cstate)?;
                // stmt->withClause is intentionally ignored here
            }
        }
    }

    Ok(())
}

// ===========================================================================
// Walker helpers over the owned tree (the `&mut ParseState`-threaded variants).
// ===========================================================================

/// Visit an optional child with the recursion walker (the C
/// `checkWellFormedRecursionWalker(child, cstate)` over a possibly-NULL node).
fn walk_opt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    child: Option<&Node<'mcx>>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    match child {
        Some(n) => checkWellFormedRecursionWalker(mcx, pstate, n, cstate),
        None => Ok(false),
    }
}

/// Visit an optional `SelectStmt` child (the set-op tree larg/rarg are typed
/// `SelectStmt`) with the recursion walker.
fn walk_opt_select<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    child: Option<&SelectStmt<'mcx>>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    match child {
        Some(s) => {
            let node = Node::mk_select_stmt(mcx, s.clone_in(mcx)?);
            checkWellFormedRecursionWalker(mcx, pstate, &node, cstate)
        }
        None => Ok(false),
    }
}

/// Visit each element of a node list with the recursion walker (the C walker
/// recursion into a `List *`).
fn walk_list<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    list: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    for n in list.iter() {
        if checkWellFormedRecursionWalker(mcx, pstate, n, cstate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `raw_walk_children` variant for the `&mut ParseState`-threaded recursion
/// walker (clones each visited child so the captured closure can own the
/// `&mut ParseState`/`&mut CteState` recursion).
fn raw_walk_children_recursion<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    node: &Node<'mcx>,
    cstate: &mut CteState<'mcx>,
) -> PgResult<bool> {
    let children = collect_children(mcx, node)?;
    for child in &children {
        if checkWellFormedRecursionWalker(mcx, pstate, child, cstate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// ctenamespace forward-visibility refresh.
// ===========================================================================

/// Replace the `p_ctenamespace` entry matching `cte` (by `ctename`) with the
/// (now-analyzed) value. The C ctenamespace aliases the CTE objects mutated in
/// place by `analyzeCTE`; the owned model holds clones, so this keeps a later
/// CTE's analysis seeing the analyzed columns of an earlier one.
fn refresh_ctenamespace_entry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cte: &CommonTableExpr<'mcx>,
) -> PgResult<()> {
    let name = cte_name(cte).to_string();
    for entry in pstate.p_ctenamespace.iter_mut() {
        if cte_name(entry) == name {
            *entry = cte.clone_in(mcx)?;
            break;
        }
    }
    Ok(())
}

/// This crate owns no inward seam (its public functions are called directly by
/// the analyze.c / parse_clause.c merged owners once they land). Empty, and not
/// wired into `init_all` (cf. parse_collate / parse_oper / functioncmds).
pub fn init_seams() {}

#[cfg(test)]
mod tests;
