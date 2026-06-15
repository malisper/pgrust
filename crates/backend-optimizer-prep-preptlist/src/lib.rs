//! `backend/optimizer/prep/preptlist.c` — SELECT core of the parse-tree
//! targetlist preprocessor.
//!
//! 1:1 port of PostgreSQL 18.3 `preprocess_targetlist` (SELECT path) plus the
//! standalone `get_plan_rowmark` lookup, over this repo's lifetime-free owned
//! `Query<'mcx>` model and the `PlannerInfo` arena handle world.
//!
//! ## What this unit is
//!
//! This crate is the new owner of `optimizer/prep/preptlist.c`. preptlist
//! preprocesses `root->parse->targetList` into `root->processed_tlist`. It owns:
//!
//! * `preprocess_targetlist` — the driver, called from `grouping_planner`
//!   (planner.c, still unported). Declared as the inward seam
//!   [`backend_optimizer_prep_preptlist_seams::preprocess_targetlist`] and
//!   installed by [`init_seams`]. **SELECT core ported** (the only reachable
//!   path on the current SELECT-analyze milestone); the INSERT/UPDATE/DELETE/
//!   MERGE legs and the FOR-UPDATE/SHARE rowMarks junk stanza seam-and-panic
//!   until the DML-analyze family + the PlanRowMark-carrier keystone land.
//! * `extract_update_targetlist_colnos` — the UPDATE colno extractor, a plain
//!   `pub fn` (its only caller is `preprocess_targetlist`'s UPDATE leg, in this
//!   crate, and INSERT...ON CONFLICT in nodeModifyTable's planning, same layer).
//! * `get_plan_rowmark` — the `PlanRowMark` lookup, a plain `pub fn`. It backs
//!   the already-declared+consumed cross-unit seam
//!   `backend_optimizer_util_restrictinfo_seams::has_plan_rowmark` (used by
//!   indxpath `check_index_predicates`), installed here by [`init_seams`].
//!
//! ## Model notes
//!
//! * The C `preprocess_targetlist(PlannerInfo *root)` reads `root->parse` (the
//!   top `Query`) and writes `root->processed_tlist`/`root->update_colnos`.
//!   `PlannerInfo` is lifetime-free here and the top `Query` lives in the
//!   [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) store behind
//!   `root.parse`'s `QueryId`. The planner driver resolves it
//!   (`run.resolve_mut(root.parse)`) and threads the `&mut Query` alongside
//!   `&mut PlannerInfo`; the two are distinct objects so there's no aliasing
//!   conflict. `mcx` is the planner-run context new nodes allocate in.
//! * `root->processed_tlist` is a `List *` of `TargetEntry *` that, in C,
//!   aliases the TLEs of `parse->targetList` (for SELECT, with no INSERT
//!   expansion / no junk additions, it is exactly `parse->targetList`). This
//!   repo carries `processed_tlist` as a `Vec<NodeId>` of arena handles into
//!   `PlannerInfo.node_arena` ([`ArenaNode::TargetEntry`] id-space), so each
//!   resolved `TargetEntry<'mcx>` is **deep-cloned** into the arena via
//!   `TargetEntry::clone_in` / `Expr::clone_in` (keystone #280 — a shallow
//!   `.clone()` panics on `Aggref`/`SubLink`/`SubPlan` children a TLE's expr can
//!   carry) and the resulting handle stored. The clone is the faithful analogue
//!   of the C alias: downstream planner stages read `processed_tlist` through
//!   the arena exactly as C reads the `TargetEntry *` list, and the SELECT path
//!   never mutates the source `parse->targetList` TLEs after this point (the
//!   in-place renumbering only happens on the UPDATE leg, deferred).
//! * The FOR-UPDATE/SHARE rowMarks junk-column stanza walks `root->rowMarks`
//!   (`List *` of `PlanRowMark *`). Here `rowMarks` is `Vec<NodeId>` of opaque
//!   handles with no backing store — `PlanRowMark`s are produced by
//!   `preprocess_rowmarks` (planmain.c, unported), which runs before this pass.
//!   The list is therefore always empty on every currently reachable path; when
//!   it is non-empty we cannot resolve `rc->rti`/`rc->allMarkTypes` to build the
//!   junk Vars, so we seam-and-panic rather than silently skip required columns
//!   (the PlanRowMark-carrier keystone must land first). Same sanctioned
//!   boundary as `remove_useless_result_rtes` (prepjointree FAMILY 5).

#![no_std]
#![allow(non_snake_case)]
// The project-wide error contract is the un-boxed `PgResult`.
#![allow(clippy::result_large_err)]

extern crate alloc;

use types_core::primitive::AttrNumber;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::CmdType;
use types_pathnodes::{NodeId, PlannerInfo, TargetEntryNode};

// ===========================================================================
// preprocess_targetlist (preptlist.c:64) — SELECT core
// ===========================================================================

/// `preprocess_targetlist(root)` (preptlist.c:64) — driver for preprocessing
/// the parse-tree targetlist. SELECT path.
///
/// See the crate docs for the carrier model and the DML/rowMarks deferrals.
pub fn preprocess_targetlist<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let result_relation = parse.resultRelation;
    let command_type = parse.commandType;

    // C 80-94: if there is a result relation, open it (INSERT/UPDATE/DELETE/
    // MERGE) so we can look for missing columns; else Assert(SELECT). On the
    // SELECT path there is no result relation. The DML legs (target_rte fetch +
    // table_open) are deferred to the DML-analyze family.
    if result_relation != 0 {
        panic!(
            "preprocess_targetlist: result-relation (INSERT/UPDATE/DELETE/MERGE) leg not yet \
             ported — needs the DML-analyze family (expand_insert_targetlist / \
             extract_update_targetlist_colnos / add_row_identity_columns / table_open)"
        );
    }
    debug_assert!(command_type == CmdType::CMD_SELECT);
    if command_type != CmdType::CMD_SELECT {
        // A non-SELECT command with no result relation is a parser/rewriter bug
        // (the Assert in C). Defensive: the INSERT/UPDATE tlist expansion is the
        // only other branch and it requires a result relation.
        panic!(
            "preprocess_targetlist: non-SELECT command with no result relation \
             (parser/rewriter messed up)"
        );
    }

    // C 105-109: tlist = parse->targetList. For SELECT (not INSERT, not UPDATE)
    // the tlist is taken verbatim; no expand_insert_targetlist / no
    // extract_update_targetlist_colnos.
    //
    // C 119-128 (UPDATE/DELETE/MERGE non-inherited junk row-identity columns)
    // and C 136-212 (MERGE per-action tlists + join-condition Vars): result
    // relation required, so unreachable on the SELECT path.

    // C 229-287: rowMarks junk-column stanza (FOR UPDATE/SHARE locking +
    // EvalPlanQual). `root->rowMarks` is empty on every reachable SELECT path
    // (produced by the unported `preprocess_rowmarks`); seam-and-panic if not.
    if !root.rowMarks.is_empty() {
        panic!(
            "preprocess_targetlist: FOR-UPDATE/SHARE rowMarks junk-column stanza not yet ported — \
             `root.rowMarks` is carried as unresolved `NodeId` handles (no arena store / \
             `rti`/`allMarkTypes`/`rowmarkId` accessors); needs the PlanRowMark-carrier keystone \
             (preprocess_rowmarks owner)"
        );
    }

    // C 296-325: if the query has a RETURNING list, add resjunk Vars for other
    // relations. `returningList` is non-NULL only for a data-modifying statement
    // (INSERT/UPDATE/DELETE/MERGE ... RETURNING), all of which require a result
    // relation and were rejected above. Unreachable on the SELECT path.
    debug_assert!(parse.returningList.is_empty());

    // C 327: root->processed_tlist = tlist. Carry the SELECT targetlist into the
    // node_arena, deep-cloning each TLE (and its expr tree) so the lifetime-free
    // arena owns a faithful copy of the resolved `TargetEntry<'mcx>` (the C alias
    // of `parse->targetList`). Store the resulting `NodeId` handles.
    let mut processed: alloc::vec::Vec<NodeId> = alloc::vec::Vec::with_capacity(parse.targetList.len());
    for tle in parse.targetList.iter() {
        // C: a TargetEntry always has a non-NULL expr.
        let expr_src = tle.expr.as_deref().expect(
            "preprocess_targetlist: TargetEntry with NULL expr in targetList (parser bug)",
        );
        // Deep-clone the expr into the arena (lifetime-free `Expr`); #280.
        let expr_clone = expr_src.clone_in(mcx)?;
        let expr_id = root.alloc_node(expr_clone);

        let te_node = TargetEntryNode {
            expr: expr_id,
            resno: tle.resno,
            resname: tle.resname.as_ref().map(|s| alloc::string::String::from(s.as_str())),
            ressortgroupref: tle.ressortgroupref,
            resorigtbl: tle.resorigtbl,
            resorigcol: tle.resorigcol,
            resjunk: tle.resjunk,
        };
        let te_id = root.alloc_targetentry(te_node);
        processed.push(te_id);
    }
    root.processed_tlist = processed;

    // C 329-330: if target_relation, table_close — no result relation on SELECT.
    Ok(())
}

// ===========================================================================
// extract_update_targetlist_colnos (preptlist.c:347)
// ===========================================================================

/// `extract_update_targetlist_colnos(tlist)` (preptlist.c:347) — extract the
/// target-table column numbers an UPDATE's targetlist assigns to, then renumber
/// the TLEs to the sequential convention.
///
/// The C convention: an UPDATE's non-resjunk TLE `resno` is the target column
/// number; this pulls those into a separate list and rewrites each `resno` to a
/// consecutive 1..n. Operates on the in-arena `TargetEntryNode`s addressed by
/// `tlist` (the resolved UPDATE targetlist's arena handles).
///
/// Only reachable from the UPDATE leg of `preprocess_targetlist` and from
/// INSERT...ON CONFLICT...UPDATE planning — both DML, deferred. Ported eagerly
/// (pure renumbering over the arena) so the DML legs only need to call it.
pub fn extract_update_targetlist_colnos(
    root: &mut PlannerInfo,
    tlist: &[NodeId],
) -> alloc::vec::Vec<AttrNumber> {
    let mut update_colnos: alloc::vec::Vec<AttrNumber> = alloc::vec::Vec::new();
    let mut nextresno: AttrNumber = 1;
    for &id in tlist.iter() {
        let tle = root.targetentry_mut(id);
        if !tle.resjunk {
            update_colnos.push(tle.resno);
        }
        tle.resno = nextresno;
        nextresno += 1;
    }
    update_colnos
}

// ===========================================================================
// get_plan_rowmark (preptlist.c:525)
// ===========================================================================

/// `get_plan_rowmark(rowmarks, rtindex)` (preptlist.c:525) — locate the
/// `PlanRowMark` for the given RT index, or `None` if none.
///
/// In C, `rowmarks` is a `List *` of `PlanRowMark *` and the function scans for
/// `rc->rti == rtindex`. Here `rowmarks` is a `Vec<NodeId>` of opaque handles
/// with no backing store (`PlanRowMark`s come from the unported
/// `preprocess_rowmarks`); the list is empty on every reachable path, so the
/// scan finds nothing and returns `None`. A non-empty list means a DML/locking
/// path that needs the PlanRowMark-carrier keystone to resolve `rc->rti` — we
/// seam-and-panic there rather than silently return `None` (which would
/// mis-report "no rowmark" and skip required junk-column / locking logic).
///
/// Returns the matching `NodeId` handle (the `PlanRowMark`'s arena id) or
/// `None`. The only consumer in the current tree is `check_index_predicates`
/// (indxpath), which only needs the not-NULL test — see [`has_plan_rowmark`].
pub fn get_plan_rowmark(rowmarks: &[NodeId], _rtindex: u32) -> Option<NodeId> {
    if rowmarks.is_empty() {
        return None;
    }
    panic!(
        "get_plan_rowmark: PlanRowMark lookup not yet ported — `rowMarks` carries unresolved \
         `NodeId` handles (no arena store / `rti` accessor); needs the PlanRowMark-carrier \
         keystone (preprocess_rowmarks owner)"
    );
}

/// Backs `backend_optimizer_util_restrictinfo_seams::has_plan_rowmark`: does the
/// query carry a `PlanRowMark` for `rtindex`? (`get_plan_rowmark(...) != NULL`.)
///
/// C site: `check_index_predicates` (indxpath.c:4029) ORs this with
/// `bms_is_member(rel->relid, root->all_result_relids)` to detect a
/// FOR-UPDATE/target relation. On the SELECT path `root.rowMarks` is empty so
/// this is `false`; the DML/locking path panics inside `get_plan_rowmark`.
fn seam_has_plan_rowmark(root: &PlannerInfo, rtindex: u32) -> bool {
    get_plan_rowmark(&root.rowMarks, rtindex).is_some()
}

// ===========================================================================
// seam wiring
// ===========================================================================

/// Install the seams this unit owns. Wired into the central init sequence.
pub fn init_seams() {
    backend_optimizer_prep_preptlist_seams::preprocess_targetlist::set(preprocess_targetlist);
    backend_optimizer_util_restrictinfo_seams::has_plan_rowmark::set(seam_has_plan_rowmark);
}

#[cfg(test)]
mod tests;
