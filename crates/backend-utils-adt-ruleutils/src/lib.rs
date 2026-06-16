//! `utils/adt/ruleutils.c` — the SQL deparser, **F0a: the deparse
//! name-resolution engine** (the foundation the rest of ruleutils builds on).
//!
//! This is the first family of the ruleutils.c port (the file is 13.7k LOC and
//! cannot land in one pass). F0a delivers:
//!
//! 1. The four ruleutils-private types — [`DeparseContext`],
//!    [`DeparseNamespace`], [`DeparseColumns`], [`NameHashEntry`] — modeled
//!    field-for-field against `ruleutils.c` (110-313). They are *crate-private*
//!    (not in `types-*`): nothing outside ruleutils.c reads them.
//!
//! 2. The relation/column alias name-resolution engine (`ruleutils.c`
//!    3870-5139): [`set_rtable_names`], [`set_deparse_for_query`],
//!    [`set_simple_column_names`], [`has_dangerous_join_using`],
//!    [`set_using_names`], [`set_relation_column_names`],
//!    [`set_join_column_names`], [`colname_is_unique`], [`make_colname_unique`],
//!    [`expand_colnames_array_to`], the `*_names_hash` helpers,
//!    [`identify_join_columns`], [`get_rtable_name`], [`deparse_columns_fetch`],
//!    plus the pure frontends [`deparse_context_for`] and
//!    [`select_rtable_names_for_explain`].
//!
//! 3. The plan-navigation half ([`set_deparse_plan`] and friends) is gated on
//!    the planner producing owned `Plan` trees (issue #159) — those read `Plan`
//!    fields a real producer does not yet supply, so they are seam-and-panic
//!    (mirror-PG-and-panic) until the plan layer lands as ruleutils F0b.
//!
//! **F1 (the expression deparser) is now landed** in the [`expr_deparse`]
//! module: the precedence-aware `get_rule_expr` tree-walker and its per-node
//! deparsers (operators, functions, aggregates, window functions, constants,
//! coercions, CASE, ARRAY/ROW/COALESCE/MIN-MAX, NULL/boolean tests, sub-links,
//! subscripts, the `isSimpleNode` precedence oracle, and the Query-side
//! `get_variable`). F1 introduces the output buffer (`DeparseContext::buf`) the
//! engine renders into. The plan-tree-navigation arms (#159), the F2 query-tree
//! deparsers (`get_rule_orderby` / `get_rule_windowspec` / `get_query_def`), and
//! the catalog name generators (`generate_operator_name` /
//! `generate_function_name`) it reaches are seam-and-panic until those families
//! land.
//!
//! The query-tree deparsers (F2) and the catalog definition builders (F3) build
//! on F1 and are NOT in this family.
//!
//! C source: `src/backend/utils/adt/ruleutils.c`.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::format;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::fmgr::NAMEDATALEN;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{
    RangeTblEntry, RTE_FUNCTION, RTE_JOIN, RTE_RELATION, RTE_TABLEFUNC,
};
use types_nodes::rawnodes::{Alias, FromExpr, JoinExpr};

mod seams;
pub use seams::init_seams;

mod expr_deparse;
pub use expr_deparse::{
    get_coercion_expr, get_const_expr, get_func_expr, get_oper_expr, get_parameter,
    get_rule_expr, get_rule_expr_funccall, get_rule_expr_paren, get_rule_expr_toplevel,
    get_rule_list_toplevel, get_sublink_expr, get_variable, isSimpleNode,
};

/// `AccessShareLock` (`storage/lockdefs.h`) — the lock `deparse_context_for`
/// takes on its synthetic relation RTE.
const AccessShareLock: i32 = 1;

/// `RELKIND_RELATION` (`catalog/pg_class.h`) — ordinary table.
const RELKIND_RELATION: i8 = b'r' as i8;

/* -------------------------------------------------------------------------- *
 * Small helpers (errors, strings, list access).
 * -------------------------------------------------------------------------- */

/// `elog(ERROR, ...)` inside a deparse routine — produce an error `PgError`.
fn elog_error(msg: alloc::string::String) -> PgError {
    PgError::error(msg)
}

/// `strVal(node)` for a `Node::String` (the WITH/USING/colnames lists carry
/// their `char *` members as `String` value nodes). Returns the `&str`, or an
/// error if the node is not a `String` (which cannot happen for these lists
/// after parse analysis).
fn str_val<'a>(node: &'a Node<'_>) -> PgResult<&'a str> {
    match node {
        Node::String(s) => Ok(s.sval.as_str()),
        other => Err(elog_error(format!(
            "expected String value node, got tag {}",
            other.tag().0
        ))),
    }
}

/// `list_nth(list, n)` for a list of `String` value nodes (`char *` list) — the
/// `n`-th name, or an error if out of range. The list elements are boxed
/// (`List *` of `Node *`), so deref the box.
fn list_nth_str<'a, 'mcx>(
    list: &'a PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    n: usize,
) -> PgResult<&'a str> {
    match list.get(n) {
        Some(node) => str_val(node),
        None => Err(elog_error(format!("string list index {n} out of range"))),
    }
}

/// `rt_fetch(index, rtable)` — borrow the 1-based RTE. Mirrors the C macro
/// `rt_fetch`, which is `list_nth(rtable, index-1)`.
fn rt_fetch<'a, 'mcx>(
    index: i32,
    rtable: &'a [RangeTblEntry<'mcx>],
) -> PgResult<&'a RangeTblEntry<'mcx>> {
    let i = (index - 1) as usize;
    rtable.get(i).ok_or_else(|| {
        elog_error(format!(
            "rt_fetch: range-table index {index} out of range (len {})",
            rtable.len()
        ))
    })
}

/* -------------------------------------------------------------------------- *
 * The four ruleutils-private types (ruleutils.c 110-313).
 * -------------------------------------------------------------------------- */

/// `typedef struct { ... } deparse_context` (`ruleutils.c` 110-127).
///
/// The context info threaded through the recursive deparse routines. F0a uses
/// only `namespaces` (for [`get_rtable_name`]); the rest of the fields are
/// modeled field-for-field so F1/F2 fill them in without re-shaping the struct.
/// The `buf`/`StringInfo` output sink is deliberately not modeled in F0a (it is
/// introduced with the F2 query deparsers that actually emit text).
pub struct DeparseContext<'mcx> {
    /// `StringInfo buf` — output buffer to append to. F0a never emitted text, so
    /// the buffer is introduced with F1 (the expression deparser is the first
    /// family that renders SQL). Modeled as the owned [`types_stringinfo::StringInfo`]
    /// (the C `appendStringInfo*` family lives in `stringinfo.c`; the deparser's
    /// thin append helpers in `expr_deparse` wrap the byte buffer directly).
    pub buf: types_stringinfo::StringInfo<'mcx>,
    /// `List *namespaces` — list of `deparse_namespace` nodes.
    pub namespaces: PgVec<'mcx, DeparseNamespace<'mcx>>,
    /// `TupleDesc resultDesc` — if top level of a view, the view's tupdesc.
    /// Read by [`get_variable`]'s `varInOrderBy` column-name-match path.
    pub resultDesc: Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
    /// `List *targetList` — current query level's SELECT targetlist. Read by
    /// [`get_variable`]'s `varInOrderBy` path; set by the F2 query deparsers.
    pub targetList: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    /// `List *windowClause` — current query level's WINDOW clause (list of
    /// `WindowClause`). Read by the `WindowFunc` `OVER` query-decompilation path.
    pub windowClause: PgVec<'mcx, types_nodes::rawnodes::WindowClause<'mcx>>,
    /// `int prettyFlags` — enabling of pretty-print functions.
    pub prettyFlags: i32,
    /// `int wrapColumn` — max line length, or -1 for no limit.
    pub wrapColumn: i32,
    /// `int indentLevel` — current indent level for pretty-print.
    pub indentLevel: i32,
    /// `bool varprefix` — true to print prefixes on Vars.
    pub varprefix: bool,
    /// `bool colNamesVisible` — do we care about output column names?
    pub colNamesVisible: bool,
    /// `bool inGroupBy` — deparsing GROUP BY clause?
    pub inGroupBy: bool,
    /// `bool varInOrderBy` — deparsing simple Var in ORDER BY?
    pub varInOrderBy: bool,
    /// `Bitmapset *appendparents` — if not null, map child Vars of these relids
    /// back to the parent rel.
    pub appendparents: Option<types_nodes::bitmapset::Bitmapset<'mcx>>,
}

/// `typedef struct { ... } deparse_namespace` (`ruleutils.c` 159-186).
///
/// One Var-namespace level per query/plan context. Carries both the Query-side
/// fields (rtable / rtable_names / rtable_columns / ctes / appendrels /
/// using_names) and the nine plan-only fields (plan / ancestors / outer_plan /
/// inner_plan / *_tlist / index_tlist). The plan-only fields stay at their
/// zero/None default until ruleutils F0b populates them; F0a never reads them.
pub struct DeparseNamespace<'mcx> {
    /// `List *rtable` — list of `RangeTblEntry` nodes (the query's range table).
    /// F0a borrows the Query's range table rather than owning a copy.
    pub rtable: PgVec<'mcx, RangeTblEntry<'mcx>>,
    /// `List *rtable_names` — parallel list of names for RTEs (`None` for
    /// nameless RTEs such as unnamed joins).
    pub rtable_names: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `List *rtable_columns` — parallel list of `deparse_columns` structs.
    pub rtable_columns: PgVec<'mcx, DeparseColumns<'mcx>>,
    /// `List *subplans` — list of `Plan` trees for SubPlans (PlannedStmt case).
    /// Plan-only; not populated until F0b (carried as the generic Node list).
    pub subplans: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `List *ctes` — list of `CommonTableExpr` nodes (Query case).
    pub ctes: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `AppendRelInfo **appendrels` — array of `AppendRelInfo` nodes, indexed by
    /// child relid, or empty. Plan-only (PlannedStmt case).
    pub appendrels: PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>>,
    /// `char *ret_old_alias` — alias for OLD in RETURNING list.
    pub ret_old_alias: Option<PgString<'mcx>>,
    /// `char *ret_new_alias` — alias for NEW in RETURNING list.
    pub ret_new_alias: Option<PgString<'mcx>>,
    /// `bool unique_using` — are we making USING names globally unique?
    pub unique_using: bool,
    /// `List *using_names` — list of assigned names for USING columns.
    pub using_names: PgVec<'mcx, PgString<'mcx>>,
    // --- Remaining fields used only when deparsing a Plan tree (F0b): ---
    /// `Plan *plan` — immediate parent of the current expression.
    pub plan: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `List *ancestors` — ancestors of `plan`.
    pub ancestors: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `Plan *outer_plan` — outer subnode, or None.
    pub outer_plan: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `Plan *inner_plan` — inner subnode, or None.
    pub inner_plan: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `List *outer_tlist` — referent for OUTER_VAR Vars.
    pub outer_tlist: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `List *inner_tlist` — referent for INNER_VAR Vars.
    pub inner_tlist: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `List *index_tlist` — referent for INDEX_VAR Vars.
    pub index_tlist: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    // --- Special namespace representing a function signature (F1): ---
    /// `char *funcname`.
    pub funcname: Option<PgString<'mcx>>,
    /// `int numargs`.
    pub numargs: i32,
    /// `char **argnames`.
    pub argnames: PgVec<'mcx, Option<PgString<'mcx>>>,
}

/// `typedef struct { ... } deparse_columns` (`ruleutils.c` 228-313).
///
/// Per-relation column alias data. The string arrays are `Vec<Option<String>>`
/// where `None` is the C `char *` NULL (a dropped column / unassigned slot).
pub struct DeparseColumns<'mcx> {
    /// `int num_cols` — length of `colnames[]`.
    pub num_cols: i32,
    /// `char **colnames` — array of C strings and NULLs (indexed by varattno-1).
    pub colnames: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `int num_new_cols` — length of `new_colnames[]`.
    pub num_new_cols: i32,
    /// `char **new_colnames` — array of C strings (no dropped columns).
    pub new_colnames: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `bool *is_new_col` — which new_colnames are new since original parsing.
    pub is_new_col: PgVec<'mcx, bool>,
    /// `bool printaliases` — should we actually print a column alias list?
    pub printaliases: bool,
    /// `List *parentUsing` — names used as USING names in joins above this RTE.
    pub parentUsing: PgVec<'mcx, PgString<'mcx>>,
    /// `int leftrti` — rangetable index of left child (JOIN RTE only).
    pub leftrti: i32,
    /// `int rightrti` — rangetable index of right child (JOIN RTE only).
    pub rightrti: i32,
    /// `int *leftattnos` — left-child varattnos of join cols, or 0.
    pub leftattnos: PgVec<'mcx, i32>,
    /// `int *rightattnos` — right-child varattnos of join cols, or 0.
    pub rightattnos: PgVec<'mcx, i32>,
    /// `List *usingNames` — names assigned to merged columns.
    pub usingNames: PgVec<'mcx, PgString<'mcx>>,
    /// `HTAB *names_hash` — copies of all strings in this struct's colnames /
    /// new_colnames / parentUsing. Built only for sufficiently wide relations
    /// (>= 32 cols) and only during set_relation/set_join_column_names; `None`
    /// otherwise. C uses a string `HTAB`; the owned model is a `BTreeMap` set
    /// (the payload is the key itself).
    pub names_hash: Option<BTreeMap<alloc::string::String, ()>>,
}

impl<'mcx> DeparseColumns<'mcx> {
    /// `palloc0(sizeof(deparse_columns))` — a zeroed `deparse_columns` (all
    /// arrays empty, all scalars zero, `names_hash` NULL).
    fn zeroed(mcx: Mcx<'mcx>) -> DeparseColumns<'mcx> {
        DeparseColumns {
            num_cols: 0,
            colnames: PgVec::new_in(mcx),
            num_new_cols: 0,
            new_colnames: PgVec::new_in(mcx),
            is_new_col: PgVec::new_in(mcx),
            printaliases: false,
            parentUsing: PgVec::new_in(mcx),
            leftrti: 0,
            rightrti: 0,
            leftattnos: PgVec::new_in(mcx),
            rightattnos: PgVec::new_in(mcx),
            usingNames: PgVec::new_in(mcx),
            names_hash: None,
        }
    }
}

impl<'mcx> DeparseNamespace<'mcx> {
    /// `memset(dpns, 0, sizeof(deparse_namespace))` — a zeroed namespace.
    fn zeroed(mcx: Mcx<'mcx>) -> DeparseNamespace<'mcx> {
        DeparseNamespace {
            rtable: PgVec::new_in(mcx),
            rtable_names: PgVec::new_in(mcx),
            rtable_columns: PgVec::new_in(mcx),
            subplans: PgVec::new_in(mcx),
            ctes: PgVec::new_in(mcx),
            appendrels: PgVec::new_in(mcx),
            ret_old_alias: None,
            ret_new_alias: None,
            unique_using: false,
            using_names: PgVec::new_in(mcx),
            plan: None,
            ancestors: PgVec::new_in(mcx),
            outer_plan: None,
            inner_plan: None,
            outer_tlist: PgVec::new_in(mcx),
            inner_tlist: PgVec::new_in(mcx),
            index_tlist: PgVec::new_in(mcx),
            funcname: None,
            numargs: 0,
            argnames: PgVec::new_in(mcx),
        }
    }
}

/// `typedef struct { char name[NAMEDATALEN]; int counter; } NameHashEntry`
/// (`ruleutils.c` 318-322) — an entry in `set_rtable_names`' hash table. In the
/// owned model the name is the `BTreeMap` key; this struct documents the C type
/// and pairs the key with its `counter` payload.
pub struct NameHashEntry {
    /// `char name[NAMEDATALEN]` — hash key.
    pub name: alloc::string::String,
    /// `int counter` — largest addition used so far for this name.
    pub counter: i32,
}

/// `deparse_columns_fetch(rangetable_index, dpns)` (`ruleutils.c` 315-317) —
/// borrow the `deparse_columns` for the 1-based range-table index.
fn deparse_columns_fetch<'a, 'mcx>(
    rangetable_index: i32,
    dpns: &'a DeparseNamespace<'mcx>,
) -> &'a DeparseColumns<'mcx> {
    &dpns.rtable_columns[(rangetable_index - 1) as usize]
}

/* -------------------------------------------------------------------------- *
 * OOM-safe list/string allocation helpers (charged to mcx).
 * -------------------------------------------------------------------------- */

/// `lappend(list, x)` — push, growing fallibly (palloc OOM surfaces as the
/// recoverable `PgError`).
fn lappend<T>(mcx: Mcx<'_>, list: &mut PgVec<'_, T>, x: T) -> PgResult<()> {
    list.try_reserve(1).map_err(|_| mcx.oom(0))?;
    list.push(x);
    Ok(())
}

/// Clone a `PgString` into `mcx` (C `pstrdup`).
fn pstrdup<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(s, mcx)
}

/* -------------------------------------------------------------------------- *
 * set_rtable_names + the EXPLAIN frontend (ruleutils.c 3854-4020).
 * -------------------------------------------------------------------------- */

/// `get_rel_name(rte->relid)` through the lsyscache seam — the live relation
/// name, or `None` if the relation is gone (a deleted rel mid-deparse).
fn get_rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)
}

/// `set_rtable_names: select RTE aliases to be used in printing a query`
/// (`ruleutils.c` 3883-4020).
///
/// Fills `dpns.rtable_names` one-for-one with `dpns.rtable`; each name is unique
/// among those in the new namespace plus the ancestor `parent_namespaces`. If
/// `rels_used` is `Some`, only RTE indexes in it are given aliases.
pub fn set_rtable_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    parent_namespaces: &[DeparseNamespace<'mcx>],
    rels_used: Option<&types_nodes::bitmapset::Bitmapset<'mcx>>,
) -> PgResult<()> {
    dpns.rtable_names = PgVec::new_in(mcx);
    // nothing more to do if empty rtable
    if dpns.rtable.is_empty() {
        return Ok(());
    }

    // We use a hash table to hold known names, so this is O(N) not O(N^2).
    // names_hash maps a known name to its NameHashEntry counter.
    let mut names_hash: BTreeMap<alloc::string::String, i32> = BTreeMap::new();

    // Preload the hash table with names appearing in parent_namespaces.
    for olddpns in parent_namespaces {
        for oldname in olddpns.rtable_names.iter() {
            let oldname = match oldname {
                Some(n) => n,
                None => continue,
            };
            // we do not complain about duplicate names in parent namespaces
            names_hash.insert(oldname.as_str().into(), 0);
        }
    }

    // Now we can scan the rtable.
    let mut rtindex: i32 = 1;
    // Index the rtable by position; the per-RTE refname can come from the
    // catalog (get_rel_name), so the loop is value-producing/fallible.
    let nrte = dpns.rtable.len();
    for i in 0..nrte {
        // CHECK_FOR_INTERRUPTS() is process-global and handled by the host loop.

        // Determine the candidate refname for this RTE.
        let mut refname: Option<PgString<'mcx>> = {
            let rte = &dpns.rtable[i];
            if rels_used.is_some()
                && !backend_nodes_core_seams::bms_is_member::call(rtindex, rels_used)
            {
                // Ignore unreferenced RTE.
                None
            } else if let Some(alias) = rte.alias.as_ref() {
                // If RTE has a user-defined alias, prefer that.
                match alias.aliasname.as_ref() {
                    Some(n) => Some(pstrdup(mcx, n.as_str())?),
                    None => None,
                }
            } else if rte.rtekind == RTE_RELATION {
                // Use the current actual name of the relation.
                get_rel_name(mcx, rte.relid)?
            } else if rte.rtekind == RTE_JOIN {
                // Unnamed join has no refname.
                None
            } else {
                // Otherwise use whatever the parser assigned.
                match rte.eref.as_ref().and_then(|e| e.aliasname.as_ref()) {
                    Some(n) => Some(pstrdup(mcx, n.as_str())?),
                    None => None,
                }
            }
        };

        // If the selected name isn't unique, append digits to make it so, and
        // make a new hash entry for it once we've got a unique name. For a very
        // long input name, we might have to truncate to stay within NAMEDATALEN.
        if let Some(rn) = refname.as_ref() {
            let key: alloc::string::String = rn.as_str().into();
            if let Some(counter) = names_hash.get(&key).copied() {
                // Name already in use, must choose a new one.
                let mut refnamelen = rn.len();
                let base: alloc::string::String = rn.as_str().into();
                let mut counter = counter;
                let modname: alloc::string::String;
                loop {
                    counter += 1;
                    let m = loop {
                        let candidate = format!("{}_{}", &base[..refnamelen], counter);
                        if (candidate.len() as i32) < NAMEDATALEN {
                            break candidate;
                        }
                        // drop chars from refname to keep all the digits
                        refnamelen = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(
                            base.as_bytes(),
                            refnamelen as i32,
                            refnamelen as i32 - 1,
                        ) as usize;
                    };
                    if !names_hash.contains_key(&m) {
                        modname = m;
                        break;
                    }
                }
                // record the bumped counter against the original name's entry
                names_hash.insert(key, counter);
                // init new hash entry for the chosen modified name
                names_hash.insert(modname.clone(), 0);
                refname = Some(pstrdup(mcx, &modname)?);
            } else {
                // Name not previously used, need only initialize hentry.
                names_hash.insert(key, 0);
            }
        }

        lappend(mcx, &mut dpns.rtable_names, refname)?;
        rtindex += 1;
    }

    Ok(())
}

/// `select_rtable_names_for_explain(rtable, rels_used)` (`ruleutils.c`
/// 3854-3868) — choose the display alias for each RTE referenced in a plan
/// (`rels_used`). A frontend to [`set_rtable_names`]. Installed inward so
/// EXPLAIN can reach it across the cycle.
pub fn select_rtable_names_for_explain<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &PgVec<'mcx, RangeTblEntry<'mcx>>,
    rels_used: &types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>> {
    let mut dpns = DeparseNamespace::zeroed(mcx);

    // dpns.rtable = rtable (the engine borrows by copying RTE images into mcx;
    // set_rtable_names only reads alias/eref/rtekind/relid off each RTE).
    dpns.rtable = clone_rtable(mcx, rtable)?;
    // subplans = NIL; ctes = NIL; appendrels = NULL — zeroed() already.
    set_rtable_names(mcx, &mut dpns, &[], Some(rels_used))?;
    // We needn't bother computing column aliases yet.

    Ok(dpns.rtable_names)
}

/// Re-home a range table's RTE images into `mcx` (the deparse namespace owns its
/// own `List *rtable` copy, as C does when it stores the Query/PlannedStmt
/// rtable pointer — here we deep-copy because the owned model has no shared
/// pointers). Uses the node-tree `RangeTblEntry::clone_in`.
fn clone_rtable<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &PgVec<'mcx, RangeTblEntry<'mcx>>,
) -> PgResult<PgVec<'mcx, RangeTblEntry<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(rtable.len()).map_err(|_| mcx.oom(0))?;
    for rte in rtable.iter() {
        out.push(rte.clone_in(mcx)?);
    }
    Ok(out)
}

/* -------------------------------------------------------------------------- *
 * deparse_context_for — pure relation-only frontend (ruleutils.c 3700-3737).
 * -------------------------------------------------------------------------- */

/// `deparse_context_for(aliasname, relid)` (`ruleutils.c` 3700-3737) — build a
/// one-deep deparse namespace stack for a single relation, used by callers that
/// deparse a partial expression over one rel (CHECK / index predicate text).
pub fn deparse_context_for<'mcx>(
    mcx: Mcx<'mcx>,
    aliasname: &str,
    relid: Oid,
) -> PgResult<PgVec<'mcx, DeparseNamespace<'mcx>>> {
    let mut dpns = DeparseNamespace::zeroed(mcx);

    // Build a minimal RTE for the rel.
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_RELATION;
    rte.relid = relid;
    rte.relkind = RELKIND_RELATION; // no need for exactness here
    rte.rellockmode = AccessShareLock;
    // rte->alias = makeAlias(aliasname, NIL);
    let alias = Alias {
        aliasname: Some(pstrdup(mcx, aliasname)?),
        colnames: PgVec::new_in(mcx),
    };
    rte.alias = Some(mcx::alloc_in(mcx, alias)?);
    // rte->eref = rte->alias (a second copy, since the owned model has no shared
    // pointers; both carry the same aliasname/colnames).
    let eref = Alias {
        aliasname: Some(pstrdup(mcx, aliasname)?),
        colnames: PgVec::new_in(mcx),
    };
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.lateral = false;
    rte.inh = false;
    rte.inFromCl = true;

    // Build one-element rtable.
    lappend(mcx, &mut dpns.rtable, rte)?;
    // subplans = NIL; ctes = NIL; appendrels = NULL — zeroed().
    set_rtable_names(mcx, &mut dpns, &[], None)?;
    set_simple_column_names(mcx, &mut dpns)?;

    // Return a one-deep namespace stack.
    let mut stack = PgVec::new_in(mcx);
    lappend(mcx, &mut stack, dpns)?;
    Ok(stack)
}

/* -------------------------------------------------------------------------- *
 * set_deparse_for_query (ruleutils.c 4028-4085).
 * -------------------------------------------------------------------------- */

/// `set_deparse_for_query(dpns, query, parent_namespaces)` (`ruleutils.c`
/// 4028-4085) — initialize a `deparse_namespace` from scratch for deparsing a
/// `Query` tree: assign RTE aliases, zero the column structs, run the USING-name
/// pass over the jointree, then assign the remaining per-RTE column aliases.
pub fn set_deparse_for_query<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    query: &types_nodes::copy_query::Query<'mcx>,
    parent_namespaces: &[DeparseNamespace<'mcx>],
) -> PgResult<()> {
    // Initialize *dpns and fill rtable/ctes links.
    *dpns = DeparseNamespace::zeroed(mcx);
    dpns.rtable = clone_rtable(mcx, &query.rtable)?;
    // subplans = NIL; appendrels = NULL — zeroed().
    dpns.ctes = clone_node_vec(mcx, &query.cteList)?;
    dpns.ret_old_alias = match query.returningOldAlias.as_ref() {
        Some(s) => Some(pstrdup(mcx, s.as_str())?),
        None => None,
    };
    dpns.ret_new_alias = match query.returningNewAlias.as_ref() {
        Some(s) => Some(pstrdup(mcx, s.as_str())?),
        None => None,
    };

    // Assign a unique relation alias to each RTE.
    set_rtable_names(mcx, dpns, parent_namespaces, None)?;

    // Initialize dpns->rtable_columns to contain zeroed structs.
    dpns.rtable_columns = PgVec::new_in(mcx);
    while dpns.rtable_columns.len() < dpns.rtable.len() {
        lappend(mcx, &mut dpns.rtable_columns, DeparseColumns::zeroed(mcx))?;
    }

    // If it's a utility query, it won't have a jointree.
    if let Some(jointree) = query.jointree.as_ref() {
        // Detect whether global uniqueness of USING names is needed.
        let jt = Node::FromExpr(clone_fromexpr(mcx, jointree)?);
        dpns.unique_using = has_dangerous_join_using(mcx, dpns, &jt)?;

        // Select names for USING-merged columns via a recursive jointree pass.
        let empty: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
        set_using_names(mcx, dpns, &jt, &empty)?;
    }

    // Now assign remaining column aliases for each RTE. We do this in a linear
    // scan of the rtable, so as to process RTEs whether or not they are in the
    // jointree. JOIN RTEs must be processed after their children, which is OK
    // because they appear later in the rtable list than their children.
    let n = dpns.rtable.len();
    for i in 0..n {
        let is_join = dpns.rtable[i].rtekind == RTE_JOIN;
        // Detach the colinfo for this RTE so we can mutate it while reading
        // sibling colinfos/the rtable; reinsert it afterward (the C code mutates
        // *colinfo in place with the other structs reachable through dpns).
        let mut colinfo = core::mem::replace(
            &mut dpns.rtable_columns[i],
            DeparseColumns::zeroed(mcx),
        );
        if is_join {
            // need an owned RTE image for the by-value reads
            let rte = dpns.rtable[i].clone_in(mcx)?;
            set_join_column_names(mcx, dpns, &rte, &mut colinfo)?;
        } else {
            let rte = dpns.rtable[i].clone_in(mcx)?;
            set_relation_column_names(mcx, dpns, &rte, &mut colinfo)?;
        }
        dpns.rtable_columns[i] = colinfo;
    }

    Ok(())
}

/// `set_simple_column_names(dpns)` (`ruleutils.c` 4097-4118) — fill in column
/// aliases for non-query situations (EXPLAIN / relation-only RTEs). Join RTEs
/// are skipped (left all-zero).
pub fn set_simple_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
) -> PgResult<()> {
    // Initialize dpns->rtable_columns to contain zeroed structs.
    dpns.rtable_columns = PgVec::new_in(mcx);
    while dpns.rtable_columns.len() < dpns.rtable.len() {
        lappend(mcx, &mut dpns.rtable_columns, DeparseColumns::zeroed(mcx))?;
    }

    // Assign unique column aliases within each non-join RTE.
    let n = dpns.rtable.len();
    for i in 0..n {
        if dpns.rtable[i].rtekind != RTE_JOIN {
            let mut colinfo = core::mem::replace(
                &mut dpns.rtable_columns[i],
                DeparseColumns::zeroed(mcx),
            );
            let rte = dpns.rtable[i].clone_in(mcx)?;
            set_relation_column_names(mcx, dpns, &rte, &mut colinfo)?;
            dpns.rtable_columns[i] = colinfo;
        }
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * has_dangerous_join_using / set_using_names (ruleutils.c 4139-4365).
 * -------------------------------------------------------------------------- */

/// `has_dangerous_join_using(dpns, jtnode)` (`ruleutils.c` 4139-4191) — search
/// the jointree for an unnamed JOIN USING whose merged columns are not simple
/// Var references (which would force globally-unique USING aliases).
pub fn has_dangerous_join_using<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &DeparseNamespace<'mcx>,
    jtnode: &Node<'mcx>,
) -> PgResult<bool> {
    match jtnode {
        Node::RangeTblRef(_) => {
            // nothing to do here
            Ok(false)
        }
        Node::FromExpr(f) => {
            for child in f.fromlist.iter() {
                if has_dangerous_join_using(mcx, dpns, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Node::JoinExpr(j) => {
            // Is it an unnamed JOIN with USING?
            if j.alias.is_none() && !j.usingClause.is_empty() {
                // Check each join alias var; if any merged col isn't a simple
                // reference to an underlying column, we have a dangerous case.
                let jrte = rt_fetch(j.rtindex, &dpns.rtable)?;
                for i in 0..(jrte.joinmergedcols as usize) {
                    let aliasvar = jrte.joinaliasvars.get(i).ok_or_else(|| {
                        elog_error(format!("joinaliasvars index {i} out of range"))
                    })?;
                    if !aliasvar.is_var() {
                        return Ok(true);
                    }
                }
            }

            // Nope, but inspect children.
            if let Some(larg) = j.larg.as_ref() {
                if has_dangerous_join_using(mcx, dpns, larg)? {
                    return Ok(true);
                }
            }
            if let Some(rarg) = j.rarg.as_ref() {
                if has_dangerous_join_using(mcx, dpns, rarg)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        other => Err(elog_error(format!(
            "unrecognized node type: {}",
            other.tag().0
        ))),
    }
}

/// `set_using_names(dpns, jtnode, parentUsing)` (`ruleutils.c` 4209-4365) —
/// select column aliases for USING-merged columns in a recursive descent of the
/// jointree. `dpns.unique_using` must already be set.
pub fn set_using_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    jtnode: &Node<'mcx>,
    parent_using: &PgVec<'mcx, PgString<'mcx>>,
) -> PgResult<()> {
    match jtnode {
        Node::RangeTblRef(_) => {
            // nothing to do now
            Ok(())
        }
        Node::FromExpr(f) => {
            for child in f.fromlist.iter() {
                set_using_names(mcx, dpns, child, parent_using)?;
            }
            Ok(())
        }
        Node::JoinExpr(j) => {
            let rtindex = j.rtindex;
            let rte = rt_fetch(rtindex, &dpns.rtable)?.clone_in(mcx)?;

            // Get info about the shape of the join — fills the join fields of
            // this RTE's colinfo (leftrti/rightrti/leftattnos/rightattnos).
            {
                let mut colinfo = core::mem::replace(
                    &mut dpns.rtable_columns[(rtindex - 1) as usize],
                    DeparseColumns::zeroed(mcx),
                );
                identify_join_columns(mcx, j, &rte, &mut colinfo)?;
                dpns.rtable_columns[(rtindex - 1) as usize] = colinfo;
            }

            let (leftrti, rightrti, num_cols) = {
                let colinfo = deparse_columns_fetch(rtindex, dpns);
                (colinfo.leftrti, colinfo.rightrti, colinfo.num_cols)
            };

            // If this join is unnamed, any name requirements pushed down to here
            // must be pushed down again to the children.
            if rte.alias.is_none() {
                for i in 0..(num_cols as usize) {
                    let colname = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        match colinfo.colnames.get(i).and_then(|c| c.as_ref()) {
                            Some(c) => pstrdup(mcx, c.as_str())?,
                            None => continue,
                        }
                    };
                    let (latt, ratt) = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        (colinfo.leftattnos[i], colinfo.rightattnos[i])
                    };
                    // Push down to left column, unless it's a system column.
                    if latt > 0 {
                        let leftcolinfo = &mut dpns.rtable_columns[(leftrti - 1) as usize];
                        expand_colnames_array_to(mcx, leftcolinfo, latt)?;
                        leftcolinfo.colnames[(latt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                    // Same on the righthand side.
                    if ratt > 0 {
                        let rightcolinfo = &mut dpns.rtable_columns[(rightrti - 1) as usize];
                        expand_colnames_array_to(mcx, rightcolinfo, ratt)?;
                        rightcolinfo.colnames[(ratt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                }
            }

            // The parentUsing list passed down to children. If there's a USING
            // clause, we extend a copy of it with the chosen merged names.
            let mut child_parent_using = clone_str_vec(mcx, parent_using)?;

            // If there's a USING clause, select the USING column names and push
            // those names down to the children.
            if !j.usingClause.is_empty() {
                // USING names must correspond to the first join output columns.
                {
                    let mut colinfo = core::mem::replace(
                        &mut dpns.rtable_columns[(rtindex - 1) as usize],
                        DeparseColumns::zeroed(mcx),
                    );
                    expand_colnames_array_to(mcx, &mut colinfo, j.usingClause.len() as i32)?;
                    dpns.rtable_columns[(rtindex - 1) as usize] = colinfo;
                }

                for i in 0..j.usingClause.len() {
                    let mut colname: PgString<'mcx> =
                        pstrdup(mcx, str_val(&j.usingClause[i])?)?;

                    // Assert it's a merged column.
                    debug_assert!({
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        colinfo.leftattnos[i] != 0 && colinfo.rightattnos[i] != 0
                    });

                    // Adopt passed-down name if any, else select a unique name.
                    let preassigned = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        colinfo.colnames.get(i).and_then(|c| c.as_ref()).map(|c| {
                            // can't pstrdup under the borrow; clone the str out
                            alloc::string::String::from(c.as_str())
                        })
                    };
                    if let Some(pre) = preassigned {
                        colname = pstrdup(mcx, &pre)?;
                    } else {
                        // Prefer user-written output alias if any.
                        if let Some(alias) = rte.alias.as_ref() {
                            if i < alias.colnames.len() {
                                colname = pstrdup(mcx, list_nth_str(&alias.colnames, i)?)?;
                            }
                        }
                        // Make it appropriately unique.
                        let unique = {
                            let colinfo = deparse_columns_fetch(rtindex, dpns);
                            make_colname_unique(mcx, colname.as_str(), dpns, colinfo)?
                        };
                        colname = unique;
                        if dpns.unique_using {
                            let c = pstrdup(mcx, colname.as_str())?;
                            lappend(mcx, &mut dpns.using_names, c)?;
                        }
                        // Save it as output column name, too.
                        let colinfo = &mut dpns.rtable_columns[(rtindex - 1) as usize];
                        colinfo.colnames[i] = Some(pstrdup(mcx, colname.as_str())?);
                    }

                    // Remember selected names for use later.
                    {
                        let c = pstrdup(mcx, colname.as_str())?;
                        let colinfo = &mut dpns.rtable_columns[(rtindex - 1) as usize];
                        lappend(mcx, &mut colinfo.usingNames, c)?;
                    }
                    {
                        let c = pstrdup(mcx, colname.as_str())?;
                        lappend(mcx, &mut child_parent_using, c)?;
                    }

                    // Push down to left column, unless it's a system column.
                    let (latt, ratt) = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        (colinfo.leftattnos[i], colinfo.rightattnos[i])
                    };
                    if latt > 0 {
                        let leftcolinfo = &mut dpns.rtable_columns[(leftrti - 1) as usize];
                        expand_colnames_array_to(mcx, leftcolinfo, latt)?;
                        leftcolinfo.colnames[(latt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                    if ratt > 0 {
                        let rightcolinfo = &mut dpns.rtable_columns[(rightrti - 1) as usize];
                        expand_colnames_array_to(mcx, rightcolinfo, ratt)?;
                        rightcolinfo.colnames[(ratt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                }
            }

            // Mark child deparse_columns structs with correct parentUsing info.
            dpns.rtable_columns[(leftrti - 1) as usize].parentUsing =
                clone_str_vec(mcx, &child_parent_using)?;
            dpns.rtable_columns[(rightrti - 1) as usize].parentUsing =
                clone_str_vec(mcx, &child_parent_using)?;

            // Now recursively assign USING column names in children.
            let larg = j
                .larg
                .as_ref()
                .ok_or_else(|| elog_error("JoinExpr has no larg".into()))?;
            let rarg = j
                .rarg
                .as_ref()
                .ok_or_else(|| elog_error("JoinExpr has no rarg".into()))?;
            // Clone the children out so the recursive call can borrow dpns mutably.
            let larg = Node_clone(mcx, larg)?;
            let rarg = Node_clone(mcx, rarg)?;
            set_using_names(mcx, dpns, &larg, &child_parent_using)?;
            set_using_names(mcx, dpns, &rarg, &child_parent_using)?;
            Ok(())
        }
        other => Err(elog_error(format!(
            "unrecognized node type: {}",
            other.tag().0
        ))),
    }
}

/* -------------------------------------------------------------------------- *
 * set_relation_column_names / set_join_column_names (ruleutils.c 4374-4840).
 * -------------------------------------------------------------------------- */

/// `set_relation_column_names(dpns, rte, colinfo)` (`ruleutils.c` 4374-4566) —
/// select column aliases for a non-join RTE.
pub fn set_relation_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
) -> PgResult<()> {
    // Construct an array of the current "real" column names of the RTE,
    // indexed by physical column number with None for dropped columns.
    let real_colnames: PgVec<'mcx, Option<PgString<'mcx>>> = if rte.rtekind == RTE_RELATION {
        // Relation --- look to the system catalogs for up-to-date info.
        // (relation_open + RelationGetDescr + per-attr attisdropped/attname; a
        // catalog-coupled read owned by the relcache/table-AM substrate.)
        backend_utils_adt_ruleutils_seams::ruleutils_relation_real_colnames::call(mcx, rte.relid)?
    } else if rte.rtekind == RTE_FUNCTION && !rte.functions.is_empty() {
        // Function returning composite: use expandRTE() (include dropped) so
        // dropped columns come back as empty strings -> None.
        backend_utils_adt_ruleutils_seams::ruleutils_expand_function_rte_colnames::call(mcx, rte)?
    } else {
        // Otherwise get the column names from eref. An empty string is a
        // dropped column, so change to None.
        let eref = rte
            .eref
            .as_ref()
            .ok_or_else(|| elog_error("RTE has no eref".into()))?;
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(eref.colnames.len()).map_err(|_| mcx.oom(0))?;
        for cn in eref.colnames.iter() {
            let cname = str_val(cn)?;
            if cname.is_empty() {
                out.push(None);
            } else {
                out.push(Some(pstrdup(mcx, cname)?));
            }
        }
        out
    };
    let ncolumns = real_colnames.len() as i32;

    // Ensure colinfo->colnames has a slot for each column.
    expand_colnames_array_to(mcx, colinfo, ncolumns)?;
    debug_assert_eq!(colinfo.num_cols, ncolumns);

    // Make sufficiently large new_colnames and is_new_col arrays.
    // (num_new_cols stays 0 until after the loop so colname_is_unique won't
    // consult the not-yet-filled new_colnames.)
    colinfo.new_colnames = PgVec::new_in(mcx);
    colinfo.is_new_col = PgVec::new_in(mcx);
    for _ in 0..ncolumns {
        colinfo.new_colnames.push(None);
        colinfo.is_new_col.push(false);
    }

    // If the RTE is wide enough, use a hash table to avoid O(N^2) costs.
    build_colinfo_names_hash(colinfo);

    // Scan the columns, select a unique alias for each, store in colnames and
    // new_colnames. Mark new_colnames entries as new (beyond eref->colnames len).
    let noldcolumns = rte
        .eref
        .as_ref()
        .map(|e| e.colnames.len() as i32)
        .unwrap_or(0);
    let mut changed_any = false;
    let mut j: usize = 0;
    for i in 0..(ncolumns as usize) {
        let real_colname = real_colnames[i].as_ref();

        // Skip dropped columns.
        let real_colname = match real_colname {
            Some(rc) => rc,
            None => {
                debug_assert!(colinfo.colnames[i].is_none());
                continue;
            }
        };

        // If alias already assigned, that's what to use.
        let colname: PgString<'mcx> = if colinfo.colnames[i].is_none() {
            // If user wrote an alias, prefer that over real column name.
            let candidate: PgString<'mcx> = match rte.alias.as_ref() {
                Some(a) if i < a.colnames.len() => pstrdup(mcx, list_nth_str(&a.colnames, i)?)?,
                _ => pstrdup(mcx, real_colname.as_str())?,
            };
            // Unique-ify and insert into colinfo.
            let colname = make_colname_unique(mcx, candidate.as_str(), dpns, colinfo)?;
            colinfo.colnames[i] = Some(pstrdup(mcx, colname.as_str())?);
            add_to_names_hash(colinfo, colname.as_str());
            colname
        } else {
            pstrdup(mcx, colinfo.colnames[i].as_ref().unwrap().as_str())?
        };

        // Put names of non-dropped columns in new_colnames[] too.
        colinfo.new_colnames[j] = Some(pstrdup(mcx, colname.as_str())?);
        // And mark them as new or not.
        colinfo.is_new_col[j] = i as i32 >= noldcolumns;
        j += 1;

        // Remember if any assigned aliases differ from "real" name.
        if !changed_any && colname.as_str() != real_colname.as_str() {
            changed_any = true;
        }
    }

    // We're now done needing the colinfo's names_hash.
    destroy_colinfo_names_hash(colinfo);

    // Set correct length for new_colnames[] array.
    colinfo.num_new_cols = j as i32;

    // Decide whether to print the alias column list.
    colinfo.printaliases = if rte.rtekind == RTE_RELATION {
        changed_any
    } else if rte.rtekind == RTE_FUNCTION {
        true
    } else if rte.rtekind == RTE_TABLEFUNC {
        false
    } else if rte
        .alias
        .as_ref()
        .map(|a| !a.colnames.is_empty())
        .unwrap_or(false)
    {
        true
    } else {
        changed_any
    };

    Ok(())
}

/// `set_join_column_names(dpns, rte, colinfo)` (`ruleutils.c` 4577-4840) —
/// select column aliases for a join RTE. Both input RTEs must already be done.
pub fn set_join_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
) -> PgResult<()> {
    let leftrti = colinfo.leftrti;
    let rightrti = colinfo.rightrti;

    // Ensure colinfo->colnames has a slot for each (old) column.
    let noldcolumns = rte
        .eref
        .as_ref()
        .map(|e| e.colnames.len() as i32)
        .unwrap_or(0);
    expand_colnames_array_to(mcx, colinfo, noldcolumns)?;
    debug_assert_eq!(colinfo.num_cols, noldcolumns);

    // If the RTE is wide enough, use a hash table to avoid O(N^2) costs.
    build_colinfo_names_hash(colinfo);

    // Scan the join output columns; set_using_names() already named the merged
    // (USING) columns, so start the loop after them.
    let mut changed_any = false;
    let using_count = colinfo.usingNames.len() as i32;
    for i in (using_count as usize)..(noldcolumns as usize) {
        let (latt, ratt) = (colinfo.leftattnos[i], colinfo.rightattnos[i]);
        // Join column must refer to at least one input column.
        debug_assert!(latt != 0 || ratt != 0);

        // Get the child column name.
        let real_colname: Option<PgString<'mcx>> = if latt > 0 {
            match dpns.rtable_columns[(leftrti - 1) as usize].colnames[(latt - 1) as usize].as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            }
        } else if ratt > 0 {
            match dpns.rtable_columns[(rightrti - 1) as usize].colnames[(ratt - 1) as usize]
                .as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            }
        } else {
            // We're joining system columns --- use eref name.
            let eref = rte
                .eref
                .as_ref()
                .ok_or_else(|| elog_error("join RTE has no eref".into()))?;
            Some(pstrdup(mcx, list_nth_str(&eref.colnames, i)?)?)
        };

        // If child col has been dropped, no need to assign a join colname.
        let real_colname = match real_colname {
            Some(rc) => rc,
            None => {
                colinfo.colnames[i] = None;
                continue;
            }
        };

        // In an unnamed join, just report child column names as-is.
        if rte.alias.is_none() {
            colinfo.colnames[i] = Some(pstrdup(mcx, real_colname.as_str())?);
            add_to_names_hash(colinfo, real_colname.as_str());
            continue;
        }

        // If alias already assigned, that's what to use.
        let colname: PgString<'mcx> = if colinfo.colnames[i].is_none() {
            let candidate: PgString<'mcx> = match rte.alias.as_ref() {
                Some(a) if i < a.colnames.len() => pstrdup(mcx, list_nth_str(&a.colnames, i)?)?,
                _ => pstrdup(mcx, real_colname.as_str())?,
            };
            let colname = make_colname_unique(mcx, candidate.as_str(), dpns, colinfo)?;
            colinfo.colnames[i] = Some(pstrdup(mcx, colname.as_str())?);
            add_to_names_hash(colinfo, colname.as_str());
            colname
        } else {
            pstrdup(mcx, colinfo.colnames[i].as_ref().unwrap().as_str())?
        };

        // Remember if any assigned aliases differ from "real" name.
        if !changed_any && colname.as_str() != real_colname.as_str() {
            changed_any = true;
        }
    }

    // Calculate number of columns the join would have if re-parsed now, and
    // create storage for the new_colnames and is_new_col arrays.
    let (left_num_new, right_num_new) = (
        dpns.rtable_columns[(leftrti - 1) as usize].num_new_cols,
        dpns.rtable_columns[(rightrti - 1) as usize].num_new_cols,
    );
    let nnewcolumns = left_num_new + right_num_new - colinfo.usingNames.len() as i32;
    colinfo.num_new_cols = nnewcolumns;
    colinfo.new_colnames = PgVec::new_in(mcx);
    colinfo.is_new_col = PgVec::new_in(mcx);
    for _ in 0..nnewcolumns {
        colinfo.new_colnames.push(None);
        colinfo.is_new_col.push(false);
    }

    // Generate the new_colnames array. Must match the parser column ordering:
    // merged columns first (USING order), then non-merged left (attnum order),
    // then non-merged right.
    let mut leftmerged: types_nodes::bitmapset::Bitmapset<'mcx> =
        types_nodes::bitmapset::Bitmapset { words: PgVec::new_in(mcx) };
    let mut rightmerged: types_nodes::bitmapset::Bitmapset<'mcx> =
        types_nodes::bitmapset::Bitmapset { words: PgVec::new_in(mcx) };

    // Handle merged columns; they are first and can't be new.
    let mut i: usize = 0;
    let mut j: usize = 0;
    while i < (noldcolumns as usize)
        && colinfo.leftattnos[i] != 0
        && colinfo.rightattnos[i] != 0
    {
        // column name is already determined and known unique
        colinfo.new_colnames[j] = match colinfo.colnames[i].as_ref() {
            Some(c) => Some(pstrdup(mcx, c.as_str())?),
            None => None,
        };
        colinfo.is_new_col[j] = false;

        // build bitmapsets of child attnums of merged columns
        if colinfo.leftattnos[i] > 0 {
            bms_add_member(mcx, &mut leftmerged, colinfo.leftattnos[i])?;
        }
        if colinfo.rightattnos[i] > 0 {
            bms_add_member(mcx, &mut rightmerged, colinfo.rightattnos[i])?;
        }
        i += 1;
        j += 1;
    }

    // Handle non-merged left-child columns.
    let mut ic: usize = 0;
    let left_num_new = dpns.rtable_columns[(leftrti - 1) as usize].num_new_cols;
    for jc in 0..(left_num_new as usize) {
        let is_new = dpns.rtable_columns[(leftrti - 1) as usize].is_new_col[jc];
        if !is_new {
            // Advance ic to next non-dropped old column of left child.
            while ic < dpns.rtable_columns[(leftrti - 1) as usize].num_cols as usize
                && dpns.rtable_columns[(leftrti - 1) as usize].colnames[ic].is_none()
            {
                ic += 1;
            }
            debug_assert!(ic < dpns.rtable_columns[(leftrti - 1) as usize].num_cols as usize);
            ic += 1;
            // If it is a merged column, we already processed it.
            if bms_is_member_local(&leftmerged, ic as i32) {
                continue;
            }
            // Else, advance i to the corresponding existing join column.
            while i < colinfo.num_cols as usize && colinfo.colnames[i].is_none() {
                i += 1;
            }
            debug_assert!(i < colinfo.num_cols as usize);
            debug_assert_eq!(ic as i32, colinfo.leftattnos[i]);
            // Use the already-assigned name of this column.
            colinfo.new_colnames[j] = match colinfo.colnames[i].as_ref() {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            i += 1;
        } else {
            let child_colname = match dpns.rtable_columns[(leftrti - 1) as usize].new_colnames[jc]
                .as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            // Unique-ify the new child column name and assign, unless we're in
            // an unnamed join, in which case just copy.
            if rte.alias.is_some() {
                let base = child_colname
                    .as_ref()
                    .map(|c| alloc::string::String::from(c.as_str()))
                    .unwrap_or_default();
                let uniq = make_colname_unique(mcx, &base, dpns, colinfo)?;
                if !changed_any && uniq.as_str() != base.as_str() {
                    changed_any = true;
                }
                colinfo.new_colnames[j] = Some(pstrdup(mcx, uniq.as_str())?);
            } else {
                colinfo.new_colnames[j] = child_colname;
            }
            let added: Option<alloc::string::String> = colinfo.new_colnames[j]
                .as_ref()
                .map(|c| alloc::string::String::from(c.as_str()));
            if let Some(a) = added {
                add_to_names_hash(colinfo, &a);
            }
        }
        colinfo.is_new_col[j] = dpns.rtable_columns[(leftrti - 1) as usize].is_new_col[jc];
        j += 1;
    }

    // Handle non-merged right-child columns in exactly the same way.
    let mut ic: usize = 0;
    let right_num_new = dpns.rtable_columns[(rightrti - 1) as usize].num_new_cols;
    for jc in 0..(right_num_new as usize) {
        let is_new = dpns.rtable_columns[(rightrti - 1) as usize].is_new_col[jc];
        if !is_new {
            while ic < dpns.rtable_columns[(rightrti - 1) as usize].num_cols as usize
                && dpns.rtable_columns[(rightrti - 1) as usize].colnames[ic].is_none()
            {
                ic += 1;
            }
            debug_assert!(ic < dpns.rtable_columns[(rightrti - 1) as usize].num_cols as usize);
            ic += 1;
            if bms_is_member_local(&rightmerged, ic as i32) {
                continue;
            }
            while i < colinfo.num_cols as usize && colinfo.colnames[i].is_none() {
                i += 1;
            }
            debug_assert!(i < colinfo.num_cols as usize);
            debug_assert_eq!(ic as i32, colinfo.rightattnos[i]);
            colinfo.new_colnames[j] = match colinfo.colnames[i].as_ref() {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            i += 1;
        } else {
            let child_colname = match dpns.rtable_columns[(rightrti - 1) as usize].new_colnames
                [jc]
                .as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            if rte.alias.is_some() {
                let base = child_colname
                    .as_ref()
                    .map(|c| alloc::string::String::from(c.as_str()))
                    .unwrap_or_default();
                let uniq = make_colname_unique(mcx, &base, dpns, colinfo)?;
                if !changed_any && uniq.as_str() != base.as_str() {
                    changed_any = true;
                }
                colinfo.new_colnames[j] = Some(pstrdup(mcx, uniq.as_str())?);
            } else {
                colinfo.new_colnames[j] = child_colname;
            }
            let added: Option<alloc::string::String> = colinfo.new_colnames[j]
                .as_ref()
                .map(|c| alloc::string::String::from(c.as_str()));
            if let Some(a) = added {
                add_to_names_hash(colinfo, &a);
            }
        }
        colinfo.is_new_col[j] = dpns.rtable_columns[(rightrti - 1) as usize].is_new_col[jc];
        j += 1;
    }

    // Assert we processed the right number of columns (USE_ASSERT_CHECKING).
    #[cfg(debug_assertions)]
    {
        let mut i = i;
        while i < colinfo.num_cols as usize && colinfo.colnames[i].is_none() {
            i += 1;
        }
        debug_assert_eq!(i, colinfo.num_cols as usize);
        debug_assert_eq!(j, nnewcolumns as usize);
    }

    // We're now done needing the colinfo's names_hash.
    destroy_colinfo_names_hash(colinfo);

    // For a named join, print column aliases if we changed any from the child
    // names. Unnamed joins cannot print aliases.
    colinfo.printaliases = if rte.alias.is_some() { changed_any } else { false };

    Ok(())
}

/* -------------------------------------------------------------------------- *
 * colname_is_unique / make_colname_unique / expand_colnames_array_to
 * + the names_hash helpers (ruleutils.c 4847-5056).
 * -------------------------------------------------------------------------- */

/// `colname_is_unique(colname, dpns, colinfo)` (`ruleutils.c` 4847-4915).
fn colname_is_unique<'mcx>(
    colname: &str,
    dpns: &DeparseNamespace<'mcx>,
    colinfo: &DeparseColumns<'mcx>,
) -> bool {
    // If we have a hash table, consult that instead of linearly scanning.
    if let Some(h) = colinfo.names_hash.as_ref() {
        if h.contains_key(colname) {
            return false;
        }
    } else {
        // Check against already-assigned column aliases within RTE.
        for oldname in colinfo.colnames.iter() {
            if let Some(o) = oldname {
                if o.as_str() == colname {
                    return false;
                }
            }
        }
        // If we're building a new_colnames array, check that too.
        for oldname in colinfo.new_colnames.iter() {
            if let Some(o) = oldname {
                if o.as_str() == colname {
                    return false;
                }
            }
        }
        // Also check against names already assigned for parent-join USING cols.
        for oldname in colinfo.parentUsing.iter() {
            if oldname.as_str() == colname {
                return false;
            }
        }
    }

    // Also check against USING-column names that must be globally unique.
    for oldname in dpns.using_names.iter() {
        if oldname.as_str() == colname {
            return false;
        }
    }

    true
}

/// `make_colname_unique(colname, dpns, colinfo)` (`ruleutils.c` 4922-4954).
fn make_colname_unique<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
    dpns: &DeparseNamespace<'mcx>,
    colinfo: &DeparseColumns<'mcx>,
) -> PgResult<PgString<'mcx>> {
    if !colname_is_unique(colname, dpns, colinfo) {
        let mut colnamelen = colname.len();
        let mut i = 0i32;
        let modname: alloc::string::String;
        loop {
            i += 1;
            let m = loop {
                let candidate = format!("{}_{}", &colname[..colnamelen], i);
                if (candidate.len() as i32) < NAMEDATALEN {
                    break candidate;
                }
                // drop chars from colname to keep all the digits
                colnamelen = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(
                    colname.as_bytes(),
                    colnamelen as i32,
                    colnamelen as i32 - 1,
                ) as usize;
            };
            if colname_is_unique(&m, dpns, colinfo) {
                modname = m;
                break;
            }
        }
        pstrdup(mcx, &modname)
    } else {
        pstrdup(mcx, colname)
    }
}

/// `expand_colnames_array_to(colinfo, n)` (`ruleutils.c` 4961-4972) — make
/// `colinfo.colnames` at least `n` items long, zero-filling the new entries.
fn expand_colnames_array_to<'mcx>(
    mcx: Mcx<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
    n: i32,
) -> PgResult<()> {
    if n > colinfo.num_cols {
        colinfo
            .colnames
            .try_reserve((n - colinfo.num_cols) as usize)
            .map_err(|_| mcx.oom(0))?;
        while (colinfo.colnames.len() as i32) < n {
            colinfo.colnames.push(None);
        }
        colinfo.num_cols = n;
    }
    Ok(())
}

/// `build_colinfo_names_hash(colinfo)` (`ruleutils.c` 4977-5030) — build the
/// names_hash for RTEs with >= 32 columns, preloaded with any names already
/// present in colnames/new_colnames/parentUsing.
fn build_colinfo_names_hash(colinfo: &mut DeparseColumns<'_>) {
    // Use a hash table only for RTEs with at least 32 columns.
    if colinfo.num_cols < 32 {
        return;
    }
    let mut h: BTreeMap<alloc::string::String, ()> = BTreeMap::new();
    for oldname in colinfo.colnames.iter() {
        if let Some(o) = oldname {
            h.insert(o.as_str().into(), ());
        }
    }
    for oldname in colinfo.new_colnames.iter() {
        if let Some(o) = oldname {
            h.insert(o.as_str().into(), ());
        }
    }
    for oldname in colinfo.parentUsing.iter() {
        h.insert(oldname.as_str().into(), ());
    }
    colinfo.names_hash = Some(h);
}

/// `add_to_names_hash(colinfo, name)` (`ruleutils.c` 5035-5043) — add a string
/// to the names_hash, if one is in use.
fn add_to_names_hash(colinfo: &mut DeparseColumns<'_>, name: &str) {
    if let Some(h) = colinfo.names_hash.as_mut() {
        h.insert(name.into(), ());
    }
}

/// `destroy_colinfo_names_hash(colinfo)` (`ruleutils.c` 5048-5056).
fn destroy_colinfo_names_hash(colinfo: &mut DeparseColumns<'_>) {
    colinfo.names_hash = None;
}

/* -------------------------------------------------------------------------- *
 * identify_join_columns / get_rtable_name (ruleutils.c 5064-5139).
 * -------------------------------------------------------------------------- */

/// `identify_join_columns(j, jrte, colinfo)` (`ruleutils.c` 5064-5125) — figure
/// out where the columns of a join come from. Fills leftrti/rightrti and the
/// leftattnos/rightattnos arrays (usingNames is filled later).
fn identify_join_columns<'mcx>(
    mcx: Mcx<'mcx>,
    j: &JoinExpr<'mcx>,
    jrte: &RangeTblEntry<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
) -> PgResult<()> {
    // Extract left/right child RT indexes.
    colinfo.leftrti = match j.larg.as_ref().map(|n| &**n) {
        Some(Node::RangeTblRef(r)) => r.rtindex,
        Some(Node::JoinExpr(jj)) => jj.rtindex,
        Some(other) => {
            return Err(elog_error(format!(
                "unrecognized node type in jointree: {}",
                other.tag().0
            )))
        }
        None => return Err(elog_error("JoinExpr larg is NULL".into())),
    };
    colinfo.rightrti = match j.rarg.as_ref().map(|n| &**n) {
        Some(Node::RangeTblRef(r)) => r.rtindex,
        Some(Node::JoinExpr(jj)) => jj.rtindex,
        Some(other) => {
            return Err(elog_error(format!(
                "unrecognized node type in jointree: {}",
                other.tag().0
            )))
        }
        None => return Err(elog_error("JoinExpr rarg is NULL".into())),
    };

    // Children are processed earlier than the join in the second pass.
    debug_assert!(colinfo.leftrti < j.rtindex);
    debug_assert!(colinfo.rightrti < j.rtindex);

    // Initialize result arrays with zeroes.
    let numjoincols = jrte.joinaliasvars.len();
    debug_assert_eq!(
        numjoincols,
        jrte.eref.as_ref().map(|e| e.colnames.len()).unwrap_or(0)
    );
    colinfo.leftattnos = PgVec::new_in(mcx);
    colinfo.rightattnos = PgVec::new_in(mcx);
    for _ in 0..numjoincols {
        colinfo.leftattnos.push(0);
        colinfo.rightattnos.push(0);
    }

    // Deconstruct joinleftcols/joinrightcols into the desired format. Merged
    // (USING) columns are the first columns of the join output.
    let mut jcolno: usize = 0;
    for &leftattno in jrte.joinleftcols.iter() {
        colinfo.leftattnos[jcolno] = leftattno;
        jcolno += 1;
    }
    let mut rcolno: i32 = 0;
    for &rightattno in jrte.joinrightcols.iter() {
        if rcolno < jrte.joinmergedcols {
            // merged column?
            colinfo.rightattnos[rcolno as usize] = rightattno;
        } else {
            colinfo.rightattnos[jcolno] = rightattno;
            jcolno += 1;
        }
        rcolno += 1;
    }
    debug_assert_eq!(jcolno, numjoincols);

    Ok(())
}

/// `get_rtable_name(rtindex, context)` (`ruleutils.c` 5132-5139) — the
/// previously-assigned alias for a 1-based RTE index in the topmost namespace.
pub fn get_rtable_name<'a, 'mcx>(
    rtindex: i32,
    context: &'a DeparseContext<'mcx>,
) -> PgResult<Option<&'a str>> {
    let dpns = context
        .namespaces
        .first()
        .ok_or_else(|| elog_error("deparse context has no namespace".into()))?;
    debug_assert!(rtindex > 0 && rtindex <= dpns.rtable_names.len() as i32);
    match dpns.rtable_names.get((rtindex - 1) as usize) {
        Some(opt) => Ok(opt.as_ref().map(|s| s.as_str())),
        None => Err(elog_error(format!(
            "get_rtable_name: index {rtindex} out of range"
        ))),
    }
}

/* -------------------------------------------------------------------------- *
 * The plan-navigation half (ruleutils.c 5151-5337) — issue #159 gated (F0b).
 *
 * These read `Plan` fields (outerPlan/innerPlan/targetlist/Append/MergeAppend/
 * SubqueryScan/CteScan/WorkTableScan/ModifyTable/IndexOnlyScan/ForeignScan/
 * CustomScan/RecursiveUnion). The planner does not yet emit an owned `Plan`
 * tree this engine can walk (the deparse-namespace plan-only fields stay None),
 * so these are seam-and-panic until the plan layer lands as ruleutils F0b.
 * -------------------------------------------------------------------------- */

/// `set_deparse_plan(dpns, plan)` (`ruleutils.c` 5151-5225) — set up the
/// namespace to deparse subexpressions of a given `Plan` node (outer/inner
/// plan + tlists + index_tlist). **F0b / #159-gated**: no owned `Plan` producer.
pub fn set_deparse_plan<'mcx>(
    _mcx: Mcx<'mcx>,
    _dpns: &mut DeparseNamespace<'mcx>,
    _plan: &Node<'mcx>,
) -> PgResult<()> {
    panic!(
        "ruleutils set_deparse_plan: Plan-tree deparse (ruleutils F0b) is gated \
         on the planner producing an owned Plan tree (issue #159); no owned \
         Plan producer exists yet"
    )
}

/// `find_recursive_union(dpns, wtscan)` (`ruleutils.c` 5232-5248) — locate the
/// RecursiveUnion ancestor generating a WorkTableScan's work table.
/// **F0b / #159-gated.**
pub fn find_recursive_union<'mcx>(
    _mcx: Mcx<'mcx>,
    _dpns: &DeparseNamespace<'mcx>,
    _wtscan: &Node<'mcx>,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    panic!(
        "ruleutils find_recursive_union: Plan-tree deparse (ruleutils F0b) is \
         gated on the planner producing an owned Plan tree (issue #159)"
    )
}

/// `push_child_plan(dpns, plan, save_dpns)` (`ruleutils.c` 5262-5274).
/// **F0b / #159-gated.**
pub fn push_child_plan<'mcx>(
    _mcx: Mcx<'mcx>,
    _dpns: &mut DeparseNamespace<'mcx>,
    _plan: &Node<'mcx>,
    _save_dpns: &mut DeparseNamespace<'mcx>,
) -> PgResult<()> {
    panic!(
        "ruleutils push_child_plan: Plan-tree deparse (ruleutils F0b) is gated \
         on the planner producing an owned Plan tree (issue #159)"
    )
}

/// `pop_child_plan(dpns, save_dpns)` (`ruleutils.c` 5279-5292).
/// **F0b / #159-gated.**
pub fn pop_child_plan<'mcx>(
    _dpns: &mut DeparseNamespace<'mcx>,
    _save_dpns: &mut DeparseNamespace<'mcx>,
) {
    panic!(
        "ruleutils pop_child_plan: Plan-tree deparse (ruleutils F0b) is gated \
         on the planner producing an owned Plan tree (issue #159)"
    )
}

/// `push_ancestor_plan(dpns, ancestor_cell, save_dpns)` (`ruleutils.c`
/// 5309-5325). **F0b / #159-gated.**
pub fn push_ancestor_plan<'mcx>(
    _mcx: Mcx<'mcx>,
    _dpns: &mut DeparseNamespace<'mcx>,
    _ancestor_index: usize,
    _save_dpns: &mut DeparseNamespace<'mcx>,
) -> PgResult<()> {
    panic!(
        "ruleutils push_ancestor_plan: Plan-tree deparse (ruleutils F0b) is \
         gated on the planner producing an owned Plan tree (issue #159)"
    )
}

/// `pop_ancestor_plan(dpns, save_dpns)` (`ruleutils.c` 5330-5337).
/// **F0b / #159-gated.**
pub fn pop_ancestor_plan<'mcx>(
    _dpns: &mut DeparseNamespace<'mcx>,
    _save_dpns: &mut DeparseNamespace<'mcx>,
) {
    panic!(
        "ruleutils pop_ancestor_plan: Plan-tree deparse (ruleutils F0b) is \
         gated on the planner producing an owned Plan tree (issue #159)"
    )
}

/* -------------------------------------------------------------------------- *
 * Local list/node-clone + bitmapset helpers.
 * -------------------------------------------------------------------------- */

/// Clone a `PgVec<PgString>` into `mcx`.
fn clone_str_vec<'mcx>(
    mcx: Mcx<'mcx>,
    src: &PgVec<'mcx, PgString<'mcx>>,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(src.len()).map_err(|_| mcx.oom(0))?;
    for s in src.iter() {
        out.push(pstrdup(mcx, s.as_str())?);
    }
    Ok(out)
}

/// Clone a `PgVec<NodePtr>` into `mcx` (C `list_copy` deep-ish; we deep-copy
/// because the owned tree has no shared pointers).
fn clone_node_vec<'mcx>(
    mcx: Mcx<'mcx>,
    src: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(src.len()).map_err(|_| mcx.oom(0))?;
    for n in src.iter() {
        out.push(Node_clone(mcx, n)?);
    }
    Ok(out)
}

/// `copyObject(node)` for a boxed Node.
#[allow(non_snake_case)]
fn Node_clone<'mcx>(
    mcx: Mcx<'mcx>,
    n: &PgBox<'mcx, Node<'mcx>>,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    mcx::alloc_in(mcx, n.clone_in(mcx)?)
}

/// Clone a `FromExpr` into `mcx`.
fn clone_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &FromExpr<'mcx>,
) -> PgResult<FromExpr<'mcx>> {
    f.clone_in(mcx)
}

/// `bms_add_member(a, x)` — set membership add over our local Bitmapset image
/// (set_join_column_names' leftmerged/rightmerged are transient workspace).
fn bms_add_member<'mcx>(
    mcx: Mcx<'mcx>,
    a: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
    x: i32,
) -> PgResult<()> {
    debug_assert!(x > 0);
    const BITS_PER_WORD: i32 = 64;
    let wordnum = (x / BITS_PER_WORD) as usize;
    let bitnum = (x % BITS_PER_WORD) as u32;
    if wordnum >= a.words.len() {
        a.words
            .try_reserve(wordnum + 1 - a.words.len())
            .map_err(|_| mcx.oom(0))?;
        while a.words.len() <= wordnum {
            a.words.push(0);
        }
    }
    a.words[wordnum] |= 1u64 << bitnum;
    Ok(())
}

/// `bms_is_member(x, a)` over our local Bitmapset image.
fn bms_is_member_local(a: &types_nodes::bitmapset::Bitmapset<'_>, x: i32) -> bool {
    if x < 0 {
        return false;
    }
    const BITS_PER_WORD: i32 = 64;
    let wordnum = (x / BITS_PER_WORD) as usize;
    let bitnum = (x % BITS_PER_WORD) as u32;
    match a.words.get(wordnum) {
        Some(w) => (w & (1u64 << bitnum)) != 0,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::nodes::Node;
    use types_nodes::parsenodes::{RangeTblEntry, RTE_JOIN, RTE_SUBQUERY};
    use types_nodes::primnodes::{Expr, Var};
    use types_nodes::rawnodes::{Alias, FromExpr, JoinExpr, RangeTblRef};
    use types_nodes::value::StringNode;

    /// `makeString(s)` as a boxed `Node`.
    fn make_string<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgBox<'mcx, Node<'mcx>> {
        mcx::alloc_in(
            mcx,
            Node::String(StringNode {
                sval: PgString::from_str_in(s, mcx).unwrap(),
            }),
        )
        .unwrap()
    }

    /// An `Alias` with the given aliasname (or none) and column names.
    fn make_alias<'mcx>(
        mcx: Mcx<'mcx>,
        name: Option<&str>,
        cols: &[&str],
    ) -> PgBox<'mcx, Alias<'mcx>> {
        let mut colnames = PgVec::new_in(mcx);
        for c in cols {
            colnames.push(make_string(mcx, c));
        }
        mcx::alloc_in(
            mcx,
            Alias {
                aliasname: name.map(|n| PgString::from_str_in(n, mcx).unwrap()),
                colnames,
            },
        )
        .unwrap()
    }

    /// A subquery RTE with an alias and the given eref column names.
    fn subquery_rte<'mcx>(
        mcx: Mcx<'mcx>,
        alias: &str,
        cols: &[&str],
    ) -> RangeTblEntry<'mcx> {
        let mut rte = RangeTblEntry::new_in(mcx);
        rte.rtekind = RTE_SUBQUERY;
        rte.alias = Some(make_alias(mcx, Some(alias), cols));
        rte.eref = Some(make_alias(mcx, Some(alias), cols));
        rte.inFromCl = true;
        rte
    }

    #[test]
    fn rtable_names_dedup() {
        let ctx = MemoryContext::new("rtable_names_dedup");
        let mcx = ctx.mcx();
        // Two subquery RTEs both aliased "t" -> "t", "t_1".
        let mut dpns = DeparseNamespace::zeroed(mcx);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(subquery_rte(mcx, "t", &["a"]));
            v.push(subquery_rte(mcx, "t", &["b"]));
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        assert_eq!(dpns.rtable_names.len(), 2);
        assert_eq!(dpns.rtable_names[0].as_ref().unwrap().as_str(), "t");
        assert_eq!(dpns.rtable_names[1].as_ref().unwrap().as_str(), "t_1");
    }

    #[test]
    fn relation_column_names_subquery_no_change() {
        let ctx = MemoryContext::new("relation_column_names");
        let mcx = ctx.mcx();
        let mut dpns = DeparseNamespace::zeroed(mcx);
        let rte = subquery_rte(mcx, "s", &["x", "y"]);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(rte.clone_in(mcx).unwrap());
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        let mut colinfo = DeparseColumns::zeroed(mcx);
        let rte0 = dpns.rtable[0].clone_in(mcx).unwrap();
        set_relation_column_names(mcx, &mut dpns, &rte0, &mut colinfo).unwrap();
        // Subquery with user alias colnames "x","y" matching its own names ->
        // no change, but it has a user-written alias colname list, so a
        // non-RELATION/FUNCTION/TABLEFUNC RTE with alias->colnames prints aliases.
        assert_eq!(colinfo.num_cols, 2);
        assert_eq!(colinfo.colnames[0].as_ref().unwrap().as_str(), "x");
        assert_eq!(colinfo.colnames[1].as_ref().unwrap().as_str(), "y");
        assert!(colinfo.printaliases); // alias->colnames non-empty
    }

    #[test]
    fn relation_column_names_unique_collision() {
        let ctx = MemoryContext::new("col_collision");
        let mcx = ctx.mcx();
        let mut dpns = DeparseNamespace::zeroed(mcx);
        // Subquery whose two columns are both named "c" -> deduped to c, c_1.
        let rte = subquery_rte(mcx, "s", &["c", "c"]);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(rte.clone_in(mcx).unwrap());
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        let mut colinfo = DeparseColumns::zeroed(mcx);
        let rte0 = dpns.rtable[0].clone_in(mcx).unwrap();
        set_relation_column_names(mcx, &mut dpns, &rte0, &mut colinfo).unwrap();
        assert_eq!(colinfo.colnames[0].as_ref().unwrap().as_str(), "c");
        assert_eq!(colinfo.colnames[1].as_ref().unwrap().as_str(), "c_1");
        assert!(colinfo.printaliases); // changed_any
    }

    #[test]
    fn deparse_for_query_unnamed_join_using() {
        // Build: SELECT ... FROM (subq a(k,x)) JOIN (subq b(k,y)) USING (k)
        // as an unnamed join RTE with merged column k.
        let ctx = MemoryContext::new("join_using");
        let mcx = ctx.mcx();
        let mut q = types_nodes::copy_query::Query::new(mcx);
        q.commandType = types_nodes::nodes::CmdType::CMD_SELECT;

        // RTE 1: subquery a(k,x). RTE 2: subquery b(k,y). RTE 3: the join.
        let a = subquery_rte(mcx, "a", &["k", "x"]);
        let b = subquery_rte(mcx, "b", &["k", "y"]);
        // join RTE: output cols k (merged), x, y. eref colnames + joinaliasvars.
        let mut jrte = RangeTblEntry::new_in(mcx);
        jrte.rtekind = RTE_JOIN;
        jrte.jointype = types_nodes::jointype::JoinType::JOIN_INNER;
        jrte.joinmergedcols = 1;
        // joinaliasvars: k (Var to left.k), x (Var left.x), y (Var right.y).
        let mut jav = PgVec::new_in(mcx);
        for (varno, varattno) in [(1, 1), (1, 2), (2, 2)] {
            let v = Var {
                varno,
                varattno,
                ..Default::default()
            };
            jav.push(mcx::alloc_in(mcx, Node::Expr(Expr::Var(v))).unwrap());
        }
        jrte.joinaliasvars = jav;
        jrte.joinleftcols = {
            let mut v = PgVec::new_in(mcx);
            v.push(1);
            v.push(2);
            v
        };
        jrte.joinrightcols = {
            let mut v = PgVec::new_in(mcx);
            v.push(1);
            v.push(2);
            v
        };
        // eref colnames k,x,y (no alias -> unnamed join).
        jrte.eref = Some(make_alias(mcx, Some("unnamed_join"), &["k", "x", "y"]));

        q.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(a);
            v.push(b);
            v.push(jrte);
            v
        };

        // jointree: FromExpr { fromlist: [ JoinExpr(larg=RTR 1, rarg=RTR 2,
        // usingClause=[k], rtindex=3) ] }
        let join = JoinExpr {
            jointype: types_nodes::jointype::JoinType::JOIN_INNER,
            isNatural: false,
            larg: Some(mcx::alloc_in(mcx, Node::RangeTblRef(RangeTblRef { rtindex: 1 })).unwrap()),
            rarg: Some(mcx::alloc_in(mcx, Node::RangeTblRef(RangeTblRef { rtindex: 2 })).unwrap()),
            usingClause: {
                let mut v = PgVec::new_in(mcx);
                v.push(make_string(mcx, "k"));
                v
            },
            join_using_alias: None,
            quals: None,
            alias: None, // unnamed join
            rtindex: 3,
        };
        let fromexpr = FromExpr {
            fromlist: {
                let mut v = PgVec::new_in(mcx);
                v.push(mcx::alloc_in(mcx, Node::JoinExpr(join)).unwrap());
                v
            },
            quals: None,
        };
        q.jointree = Some(mcx::alloc_in(mcx, fromexpr).unwrap());

        let mut dpns = DeparseNamespace::zeroed(mcx);
        set_deparse_for_query(mcx, &mut dpns, &q, &[]).unwrap();

        // rtable_names: a, b, (None for unnamed join).
        assert_eq!(dpns.rtable_names[0].as_ref().unwrap().as_str(), "a");
        assert_eq!(dpns.rtable_names[1].as_ref().unwrap().as_str(), "b");
        assert!(dpns.rtable_names[2].is_none());

        // The join colinfo: merged column "k" named (usingNames has 1 entry),
        // leftrti=1, rightrti=2.
        let jcol = &dpns.rtable_columns[2];
        assert_eq!(jcol.leftrti, 1);
        assert_eq!(jcol.rightrti, 2);
        assert_eq!(jcol.usingNames.len(), 1);
        assert_eq!(jcol.usingNames[0].as_str(), "k");
        // Unnamed join cannot print aliases.
        assert!(!jcol.printaliases);
        // Join output colnames: k (merged), x, y.
        assert_eq!(jcol.colnames[0].as_ref().unwrap().as_str(), "k");
        assert_eq!(jcol.colnames[1].as_ref().unwrap().as_str(), "x");
        assert_eq!(jcol.colnames[2].as_ref().unwrap().as_str(), "y");
    }

    #[test]
    fn rtable_name_prefers_user_alias() {
        // A relation RTE with a user alias takes the alias name without any
        // catalog lookup (get_rel_name is only used for un-aliased relations).
        let ctx = MemoryContext::new("alias_pref");
        let mcx = ctx.mcx();
        let mut rte = RangeTblEntry::new_in(mcx);
        rte.rtekind = RTE_RELATION;
        rte.relid = Oid::default();
        rte.alias = Some(make_alias(mcx, Some("myrel"), &[]));
        rte.eref = Some(make_alias(mcx, Some("myrel"), &[]));
        let mut dpns = DeparseNamespace::zeroed(mcx);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(rte);
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        assert_eq!(dpns.rtable_names[0].as_ref().unwrap().as_str(), "myrel");
    }

    #[test]
    #[should_panic(expected = "ruleutils set_deparse_plan")]
    fn plan_nav_is_seam_and_panic() {
        // The plan-navigation half is #159-gated (F0b): set_deparse_plan panics
        // loudly until the planner produces an owned Plan tree.
        let ctx = MemoryContext::new("plan_nav");
        let mcx = ctx.mcx();
        let mut dpns = DeparseNamespace::zeroed(mcx);
        let plan = Node::RangeTblRef(RangeTblRef { rtindex: 1 });
        let _ = set_deparse_plan(mcx, &mut dpns, &plan);
    }
}
