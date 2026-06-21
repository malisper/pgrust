//! `(PlannedStmt *) stringToNode(pstmtspace)` — the worker plan-shipping
//! `PlannedStmt` reader (execParallel.c `ExecParallelGetQueryDesc`).
//!
//! `PlannedStmt` is **not** a `Node` enum variant in the trimmed model, so it
//! cannot route through `parse_node_string` (which returns a `Node`). This
//! module supplies a dedicated reader that drives the shared `pg_strtok` cursor
//! itself (via `read.c`'s [`with_strtok`](backend_nodes_core::read::with_strtok)),
//! reads the opening `{ PLANNEDSTMT`, and reverses the dummy-`PlannedStmt`
//! serialization `ExecSerializePlan`/`out_dummy_plannedstmt`
//! (`backend-nodes-outfuncs/src/serialize_plan.rs`) emits — **field for field,
//! in the exact order written** — so the round-trip is byte-stable.
//!
//! Several fields the writer emits do not exist on the trimmed carrier
//! (`queryId`/`planId`/`rewindPlanIDs`/`appendRelations`/...); the reader still
//! consumes their tokens (to keep the cursor aligned) and discards the value,
//! mirroring the C dummy whose corresponding fields are read into a `PlannedStmt`
//! that the worker likewise never inspects.

use alloc::vec::Vec;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsenodes::{RTEPermissionInfo, RangeTblEntry};

use backend_nodes_core::read;

use crate::{
    elog_error, next_token, read_bitmapset_opt_field, read_bool_field, read_enum_field,
    read_int64_field, read_int_field, read_location_field, read_node_field, read_node_list_field,
    tok_str,
};

/// `string_to_planned_stmt(text)` — the seam body. Install-time entry: point the
/// shared cursor at `text` and read the `{ PLANNEDSTMT ... }`-framed dummy.
pub fn string_to_planned_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    text: &str,
) -> PgResult<PgBox<'mcx, PlannedStmt<'mcx>>> {
    read::with_strtok(text, || read_top_planned_stmt(mcx))
}

/// With the cursor freshly installed, consume the opening `{`, the
/// `PLANNEDSTMT` label, the body, and the closing `}` (mirrors `read.c`'s
/// `nodeRead` `LEFT_BRACE` arm, specialized to the single `PLANNEDSTMT` tag).
fn read_top_planned_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, PlannedStmt<'mcx>>> {
    // `{`
    let open = next_token()?;
    if open.bytes != b"{" {
        return Err(elog_error("expected '{' at start of PlannedStmt string"));
    }
    // node-type LABEL
    let label = next_token()?;
    if label.bytes != b"PLANNEDSTMT" {
        return Err(elog_error(alloc::format!(
            "expected PLANNEDSTMT label, got \"{}\"",
            tok_str(&label)
        )));
    }
    let pstmt = read_planned_stmt_body(mcx)?;
    // `}`
    let close = next_token()?;
    if close.bytes != b"}" {
        return Err(elog_error("did not find '}' at end of PlannedStmt"));
    }
    alloc_in(mcx, pstmt).map_err(Into::into)
}

/// Reverse `out_dummy_plannedstmt` field-for-field. The cursor is positioned
/// just past the `PLANNEDSTMT` label; read each `:field value` in write order.
fn read_planned_stmt_body<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PlannedStmt<'mcx>> {
    // :commandType (enum)
    let command_type = match read_enum_field()? {
        x if x == CmdType::CMD_SELECT as i32 => CmdType::CMD_SELECT,
        x if x == CmdType::CMD_UPDATE as i32 => CmdType::CMD_UPDATE,
        x if x == CmdType::CMD_INSERT as i32 => CmdType::CMD_INSERT,
        x if x == CmdType::CMD_DELETE as i32 => CmdType::CMD_DELETE,
        x if x == CmdType::CMD_MERGE as i32 => CmdType::CMD_MERGE,
        x if x == CmdType::CMD_UTILITY as i32 => CmdType::CMD_UTILITY,
        x if x == CmdType::CMD_NOTHING as i32 => CmdType::CMD_NOTHING,
        // The worker dummy is always CMD_SELECT; an unknown code is malformed.
        other => {
            return Err(elog_error(alloc::format!(
                "unrecognized PlannedStmt commandType {other}"
            )))
        }
    };

    // :queryId (int64) — carrier holds it.
    let query_id = read_int64_field()?;
    // :planId (int64) — carrier omits; consume & discard.
    let _plan_id = read_int64_field()?;

    // :hasReturning :hasModifyingCTE :canSetTag :transientPlan :dependsOnRole
    // :parallelModeNeeded  (bools, in write order)
    let has_returning = read_bool_field()?;
    let has_modifying_cte = read_bool_field()?;
    let can_set_tag = read_bool_field()?;
    let transient_plan = read_bool_field()?;
    let depends_on_role = read_bool_field()?;
    let parallel_mode_needed = read_bool_field()?;

    // :jitFlags (int)
    let jit_flags = read_int_field()?;

    // :planTree (Plan node)
    let plan_tree: Option<PgBox<'mcx, Node<'mcx>>> = read_node_field(mcx)?;

    // :partPruneInfos — the writer emits `<>` (NIL) for the worker-shipping
    // cases; the carrier types it opaque with no reader, so consume the `<>`
    // token (skip label, read value) and keep the empty list.
    {
        let _label = next_token()?;
        let v = next_token()?;
        if !v.bytes.is_empty() {
            return Err(elog_error(
                "PlannedStmt partPruneInfos round-trip expects NIL (<>) from the worker dummy",
            ));
        }
    }

    // :rtable (List of RANGETBLENTRY)
    let rtable = read_rte_pgvec_opt(mcx)?;

    // :unprunableRelids (Bitmapset) — `read_bitmapset_opt_field` skips the
    // `:fldname` label itself, so do NOT pre-skip it here.
    let unprunable_relids = read_bitmapset_opt_field(mcx)?;

    // :permInfos (List of RTEPERMISSIONINFO)
    let perm_infos = read_rteperminfo_pgvec_opt(mcx)?;

    // :resultRelations (IntList) — dummy NIL; consume.
    let result_relations = read_int_list_pgvec_opt(mcx)?;
    // :appendRelations (List) — carrier omits; dummy NIL; consume & discard.
    let _append_relations = read_node_list_field(mcx)?;

    // :subplans (List, may have NULL holes)
    let subplans = read_subplans(mcx)?;

    // :rewindPlanIDs (Bitmapset) — carrier omits; consume & discard. Use the
    // bitmapset reader (it skips the `:fldname` label and handles the `(b ...)`
    // member form, which `node_read` rejects as a top-level node).
    let _rewind_plan_ids = read_bitmapset_opt_field(mcx)?;

    // :rowMarks (List of PlanRowMark) — dummy NIL; consume.
    let row_marks = {
        let items = read_node_list_field(mcx)?;
        if items.is_empty() {
            None
        } else {
            // The worker dummy always emits NIL here; a non-empty list would mean
            // a PlanRowMark stream the carrier reader does not model.
            return Err(elog_error(
                "PlannedStmt rowMarks round-trip expects NIL from the worker dummy",
            ));
        }
    };

    // :relationOids (OidList) — dummy NIL; consume.
    let relation_oids = read_oid_list_pgvec_opt(mcx)?;
    // :invalItems (List) — dummy NIL; consume & discard.
    let _inval_items = read_node_list_field(mcx)?;

    // :paramExecTypes (OidList)
    let param_exec_types = read_oid_list_pgvec_opt(mcx)?;

    // :utilityStmt (Node) — dummy NULL.
    let utility_stmt = read_node_field(mcx)?;

    // :stmt_location :stmt_len (location fields → -1, value consumed)
    let stmt_location = read_location_field()?;
    let stmt_len = read_location_field()?;

    Ok(PlannedStmt {
        commandType: command_type,
        queryId: query_id,
        utilityStmt: utility_stmt,
        resultRelations: result_relations,
        relationOids: relation_oids,
        planTree: plan_tree,
        rowMarks: row_marks,
        canSetTag: can_set_tag,
        hasReturning: has_returning,
        hasModifyingCTE: has_modifying_cte,
        parallelModeNeeded: parallel_mode_needed,
        jitFlags: jit_flags,
        permInfos: perm_infos,
        paramExecTypes: param_exec_types,
        rtable,
        unprunableRelids: unprunable_relids,
        subplans,
        stmt_location,
        stmt_len,
        transientPlan: transient_plan,
        dependsOnRole: depends_on_role,
        invalItems: None,
        partPruneInfos: Vec::new(),
        appendRelations: Vec::new(),
    })
}

/// `:subplans (...)` — a `List *` of plan nodes where some elements are NULL
/// holes (`<>`), preserving list indexes (execParallel.c keeps the index of
/// parallel-safe subplans by leaving NULL holes for the unsafe ones). The
/// generic `node_read` list path rejects NULL holes, so read the `(...)` form
/// manually into the carrier's `PgVec<Option<PgBox<Node>>>`.
fn read_subplans<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>>>> {
    let _label = next_token()?; // skip :subplans
    let open = next_token()?;
    if open.bytes.is_empty() {
        // `<>` — C NIL.
        return Ok(None);
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for subplans list"));
    }
    let mut out: PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>> = PgVec::new_in(mcx);
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        if t.is_empty() {
            // `<>` NULL hole for a non-parallel-safe subplan.
            out.push(None);
            continue;
        }
        // A real subplan node: feed the just-read opening token into node_read.
        let child = read::node_read(mcx, Some(t))?;
        out.push(child);
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// Typed-vec list readers (mirror read_parse_family's private ones): read a
// node list, downcast each framed element to the concrete carrier type.
// ---------------------------------------------------------------------------

fn read_rte_pgvec_opt<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, RangeTblEntry<'mcx>>>> {
    let items = read_node_list_field(mcx)?;
    if items.is_empty() {
        return Ok(None);
    }
    let mut v = vec_with_capacity_in(mcx, items.len())?;
    for it in items {
        let n = PgBox::into_inner(it);
        let tag = n.node_tag();
        match n.into_rangetblentry() {
            Some(r) => v.push(r),
            None => {
                return Err(elog_error(alloc::format!(
                    "expected RangeTblEntry in rtable, got {tag:?}"
                )))
            }
        }
    }
    Ok(Some(v))
}

fn read_rteperminfo_pgvec_opt<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, RTEPermissionInfo<'mcx>>>> {
    let items = read_node_list_field(mcx)?;
    if items.is_empty() {
        return Ok(None);
    }
    let mut v = vec_with_capacity_in(mcx, items.len())?;
    for it in items {
        let n = PgBox::into_inner(it);
        let tag = n.node_tag();
        match n.into_rtepermissioninfo() {
            Some(p) => v.push(p),
            None => {
                return Err(elog_error(alloc::format!(
                    "expected RTEPermissionInfo in permInfos, got {tag:?}"
                )))
            }
        }
    }
    Ok(Some(v))
}

// ---------------------------------------------------------------------------
// Scalar Int/Oid list readers: `(i ...)` / `(o ...)` form; `<>` → None.
// (read_parse_family's equivalents are private; re-derive locally.)
// ---------------------------------------------------------------------------

fn read_scalar_list_opt(disc: u8) -> PgResult<Option<Vec<i64>>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None); // `<>` — C NIL
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for scalar list"));
    }
    let tag = next_token()?;
    if tag.bytes == b")" {
        // present-but-empty `()` — shouldn't happen for `(i/o ...)` but be safe.
        return Ok(Some(Vec::new()));
    }
    if tag.bytes.len() != 1 || tag.bytes[0] != disc {
        return Err(elog_error("unexpected scalar-list discriminator"));
    }
    let mut out: Vec<i64> = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        let s = tok_str(&t);
        let val: i64 = s
            .parse()
            .map_err(|_| elog_error("unrecognized integer in scalar list"))?;
        out.push(val);
    }
    Ok(Some(out))
}

fn read_int_list_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, i32>>> {
    match read_scalar_list_opt(b'i')? {
        None => Ok(None),
        Some(vals) => {
            let mut out = vec_with_capacity_in(mcx, vals.len())?;
            for v in vals {
                out.push(v as i32);
            }
            Ok(Some(out))
        }
    }
}

fn read_oid_list_pgvec_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, u32>>> {
    match read_scalar_list_opt(b'o')? {
        None => Ok(None),
        Some(vals) => {
            let mut out = vec_with_capacity_in(mcx, vals.len())?;
            for v in vals {
                out.push(v as u32);
            }
            Ok(Some(out))
        }
    }
}
