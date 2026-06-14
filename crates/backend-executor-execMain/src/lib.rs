//! `backend-executor-execMain` (`executor/execMain.c`) — the executor driver.
//!
//! ## F0a keystone scope
//!
//! This crate is the executor-ownership **keystone landing point** (#166 F0a).
//! It currently lands:
//!
//! - the canonical owned [`QueryDesc`](types_nodes::querydesc::QueryDesc) (the
//!   `CreateQueryDesc`-backed bundle, defined in `types-nodes`),
//! - `CreateQueryDesc` + the `ExecutorStart` / `ExecutorEnd` **skeleton** (the
//!   EState-fill prologue; the `InitPlan` plan-state-tree build is the F0d
//!   frontier and seam-and-panics there),
//! - the self-contained permission checks `ExecCheckPermissions` /
//!   `ExecCheckOneRelPerms` (SELECT form) + `ExecCheckXactReadOnly`, and it
//!   **installs** the `exec_check_permissions_select` seam (consumed by
//!   `ri_triggers`' `RI_Initial_Check`).
//!
//! Everything else `backend-executor-execMain-seams` declares (the EvalPlanQual
//! machinery, the constraint/partition checks, `InitResultRelInfo`, the
//! CteScan-leader operations, …) stays declared-but-uninstalled and
//! seam-and-panics until the corresponding family of #166 lands. The CATALOG
//! status stays `needs-decomp`, so the seam-install recurrence guard
//! (`every_declared_seam_is_installed_by_its_owner`) legitimately exempts the
//! still-unfinished surface (mirror-pg-and-panic on a live port frontier).

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::ToString;

use mcx::MemoryContext;
use types_acl::acl::{AclResult, ACL_SELECT};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::parsestmt::{DestReceiverHandle, ParamListInfoHandle};
use types_nodes::querydesc::QueryDesc;

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_objectaddress_seams as objaddr;
use backend_executor_execMain_seams as seams;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_init_miscinit_seams as miscinit;

// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) — the column-bitmap
// offset the planner applies to selectedCols bit numbers.
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

/// `CreateQueryDesc(plannedstmt, sourceText, snapshot, crosscheck_snapshot,
/// dest, params, queryEnv, instrument_options)` (execMain.c).
///
/// Allocates the per-query "ExecutorState" context, builds the `EState` in it
/// (`CreateExecutorState`), and copies the read-only inputs. `ExecutorStart`
/// fills the plan-state tree and result tupdesc.
#[allow(clippy::too_many_arguments)]
pub fn CreateQueryDesc(
    parent: &MemoryContext,
    plannedstmt: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    crosscheck_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    dest: DestReceiverHandle,
    params: ParamListInfoHandle,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    QueryDesc::create(
        parent,
        plannedstmt,
        source_text,
        snapshot,
        crosscheck_snapshot,
        dest,
        params,
        instrument_options,
    )
}

/// `standard_ExecutorStart(queryDesc, eflags)` (execMain.c) — EState-fill
/// prologue.
///
/// Fills the `EState` fields the C prologue sets directly from `queryDesc`
/// (params, source text, query env, instrument/jit flags, top eflags), then
/// reaches the `InitPlan` boundary — building the plan-state tree
/// (`ExecInitNode`) and wiring the top `JunkFilter`/result tupdesc + the
/// `DestReceiver` dispatch. That is the F0d frontier of #166 (it needs the full
/// node-init dispatch and the DestReceiver receiver-value router, F0b), so it
/// seam-and-panics there.
///
/// The C `XactReadOnly`/`IsInParallelMode` gate (→ [`ExecCheckXactReadOnly`]),
/// the `GetCurrentCommandId` output-CID assignment, `RegisterSnapshot`, and
/// `AfterTriggerBeginQuery` are likewise part of the F0d driver (they pull in
/// xact/snapmgr/trigger owners) and land with it.
pub fn standard_ExecutorStart(query_desc: &mut QueryDesc, eflags: i32) {
    // estate->es_param_list_info = queryDesc->params;
    // estate->es_sourceText = queryDesc->sourceText;
    // estate->es_queryEnv = queryDesc->queryEnv;
    // estate->es_top_eflags = eflags; estate->es_instrument = ...;
    // estate->es_jit_flags = queryDesc->plannedstmt->jitFlags;
    let params = query_desc.params;
    let instrument = query_desc.instrument_options;
    query_desc.work.with_mut(|w| {
        w.estate.es_param_list_info = params;
        w.estate.es_top_eflags = eflags;
        w.estate.es_instrument = instrument;
        w.estate.es_jit_flags = w.plannedstmt.jitFlags;
        // es_sourceText aliases the bundle-owned copy.
        // (Carried inside the bundle as `w.source_text`; the EState's
        // `es_sourceText` view of it lands when its first reader does.)
    });

    // InitPlan(queryDesc, eflags): build the plan-state tree, the top junk
    // filter / result tupdesc, and bind the DestReceiver. F0d frontier.
    let _ = eflags;
    panic!(
        "execMain InitPlan (plan-state tree build + DestReceiver dispatch) not \
         yet ported — #166 F0d (needs the ExecInitNode node-init dispatch and \
         the DestReceiver receiver-value router, F0b)"
    );
}

/// `ExecutorStart(queryDesc, eflags)` — the hookable entry; routes to
/// [`standard_ExecutorStart`] (there is no `ExecutorStart_hook` consumer yet).
pub fn ExecutorStart(query_desc: &mut QueryDesc, eflags: i32) {
    standard_ExecutorStart(query_desc, eflags)
}

/// `standard_ExecutorEnd(queryDesc)` (execMain.c) — teardown skeleton.
///
/// `ExecEndPlan` (shutting the plan-state tree down) is the F0d frontier; it
/// seam-and-panics there. Dropping the [`QueryDesc`] then releases the per-query
/// context (and with it the `EState` and everything `ExecutorStart` built) —
/// the bundle drop is the `FreeExecutorState` + per-query-context free.
pub fn standard_ExecutorEnd(query_desc: &mut QueryDesc) {
    let started = query_desc.work.with(|w| w.planstate.is_some());
    if started {
        panic!(
            "execMain ExecEndPlan (plan-state tree shutdown) not yet ported — \
             #166 F0d"
        );
    }
    // Not started (no plan-state tree): nothing to shut down; the caller drops
    // the QueryDesc, freeing the per-query context (FreeExecutorState).
}

/// `ExecutorEnd(queryDesc)` — routes to [`standard_ExecutorEnd`].
pub fn ExecutorEnd(query_desc: &mut QueryDesc) {
    standard_ExecutorEnd(query_desc)
}

// ===========================================================================
// Permission checks (the self-contained, fully-portable surface).
// ===========================================================================

/// `ExecCheckXactReadOnly(plannedstmt)` (execMain.c) — read-only / parallel
/// gate, restricted to what the trimmed `PlannedStmt`/`RTEPermissionInfo`
/// expose.
///
/// The C body walks `plannedstmt->permInfos`, skipping SELECT-only and
/// temp-namespace rels, and `PreventCommandIfReadOnly`s the rest, then
/// `PreventCommandIfParallelMode`s any non-SELECT / modifying-CTE command. The
/// trimmed `RTEPermissionInfo` carries no `requiredPerms` yet (it lands with
/// the full `ExecCheckPermissions` consumer, docs/types.md rule 3), so the
/// per-rel write-permission classification cannot be reproduced here; the
/// function panics at that boundary. The parallel-mode tail reads only fields
/// this layer already carries and is reproduced. F0d wires the real
/// `PreventCommandIf*` (xact owner) callers in.
pub fn ExecCheckXactReadOnly(plannedstmt: &PlannedStmt<'_>) {
    if plannedstmt.permInfos.as_ref().is_some_and(|p| !p.is_empty()) {
        panic!(
            "execMain ExecCheckXactReadOnly per-rel write-permission \
             classification needs RTEPermissionInfo.requiredPerms (trimmed; \
             lands with the full ExecCheckPermissions consumer) — #166 F0d"
        );
    }
    // The parallel-mode tail (PreventCommandIfParallelMode) reaches the xact
    // owner; F0d wires it. With no write rels, the read-only gate is a no-op
    // here for a plain SELECT.
    let _ = plannedstmt.commandType;
}

/// `ExecCheckOneRelPerms` for the SELECT-on-columns case (execMain.c).
///
/// `relid`/`relkind` identify an `RTE_RELATION`; `selected_cols` are the
/// `selectedCols` bit numbers (offset by `FirstLowInvalidHeapAttributeNumber`),
/// requiring `ACL_SELECT`. Mirrors the C SELECT branch: a relation-level
/// `pg_class_aclmask` short-circuit, else per-column `pg_attribute_aclcheck`
/// (whole-row reference → `pg_attribute_aclcheck_all(ALL)`), and the
/// empty-`selectedCols` "SELECT on any column" rule.
fn exec_check_one_rel_perms_select(
    relid: Oid,
    selected_cols: &[i16],
    userid: Oid,
) -> PgResult<bool> {
    // relPerms = pg_class_aclmask(relOid, userid, ACL_SELECT, ACLMASK_ALL);
    // remainingPerms = ACL_SELECT & ~relPerms;  (i.e. do we still need col-priv?)
    if aclchk::pg_class_aclcheck::call(relid, userid, ACL_SELECT)? == AclResult::AclcheckOk {
        // Relation-level SELECT satisfies it outright.
        return Ok(true);
    }

    // remainingPerms & ACL_SELECT: check at column level.
    // When the query references no columns, allow if SELECT on ANY column.
    if selected_cols.is_empty() {
        if aclchk::pg_attribute_aclcheck_all::call(
            relid,
            userid,
            ACL_SELECT,
            types_acl::acl::AclMaskHow::AclmaskAny,
        )? != AclResult::AclcheckOk
        {
            return Ok(false);
        }
        return Ok(true);
    }

    for &col in selected_cols {
        // bit #s are offset by FirstLowInvalidHeapAttributeNumber.
        let attno = col + FirstLowInvalidHeapAttributeNumber;
        if attno == 0 {
            // Whole-row reference: must have priv on all cols.
            if aclchk::pg_attribute_aclcheck_all::call(
                relid,
                userid,
                ACL_SELECT,
                types_acl::acl::AclMaskHow::AclmaskAll,
            )? != AclResult::AclcheckOk
            {
                return Ok(false);
            }
        } else if aclchk::pg_attribute_aclcheck::call(relid, attno, userid, ACL_SELECT)?
            != AclResult::AclcheckOk
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
/// (execMain.c), restricted to the SELECT-on-relations form `RI_Initial_Check`
/// uses (the `exec_check_permissions_select` seam).
///
/// Each `(relid, relkind, selectedCols)` is an `RTE_RELATION` requiring
/// `ACL_SELECT`. Returns `true` when every rel passes; with
/// `ereport_on_violation` a denial raises the standard `aclcheck_error`
/// (carried on `Err`), else returns `Ok(false)`.
pub fn exec_check_permissions_select(
    rels: &[(Oid, u8, &[i16])],
    ereport_on_violation: bool,
) -> PgResult<bool> {
    // userid to check as: current user (no setuid indication in this form).
    let userid = miscinit::get_user_id::call();

    for &(relid, relkind, cols) in rels {
        let ok = exec_check_one_rel_perms_select(relid, cols, userid)?;
        if !ok {
            if ereport_on_violation {
                // aclcheck_error(ACLCHECK_NO_PRIV,
                //   get_relkind_objtype(get_rel_relkind(relid)),
                //   get_rel_name(relid));
                let objtype = objaddr::get_relkind_objtype::call(relkind);
                let name = lsyscache_get_rel_name(relid)?;
                aclchk::aclcheck_error::call(AclResult::AclcheckNoPriv, objtype, name)?;
            }
            return Ok(false);
        }
    }
    Ok(true)
}

/// `get_rel_name(relid)` → owned `String` (the `aclcheck_error` objectname).
fn lsyscache_get_rel_name(relid: Oid) -> PgResult<Option<alloc::string::String>> {
    // get_rel_name pallocs into a context; the seam takes one. Use a transient
    // child of TopMemoryContext-equivalent: aclcheck_error only reads the name
    // for the error text. The lsyscache seam allocates in the supplied mcx;
    // copy the bytes out into an owned String so no 'mcx escapes.
    let tmp = MemoryContext::new("execMain get_rel_name");
    let out = lsyscache::get_rel_name::call(tmp.mcx(), relid)?
        .map(|s| s.as_str().to_string());
    Ok(out)
}

/// Install the seams this unit OWNS and is ready to install (F0a: only the
/// self-contained SELECT-permission check). The rest of
/// `backend-executor-execMain-seams` stays uninstalled (mirror-and-panic) until
/// the matching #166 family lands; the unit is CATALOG `needs-decomp`, so the
/// recurrence guard exempts the unfinished surface.
pub fn init_seams() {
    seams::exec_check_permissions_select::set(exec_check_permissions_select);
}
