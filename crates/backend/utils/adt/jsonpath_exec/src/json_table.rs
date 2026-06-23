//! JSON_TABLE plan execution (jsonpath_exec.c:4082-4493).
//!
//! These are the `TableFuncRoutine` callbacks (`JsonbTableRoutine`) plus the
//! recursive `JsonTablePlanState` row-pattern machinery. The plan-walk control
//! flow — `JsonTableInitPlan`, `JsonTablePlanNextRow`/`ScanNextRow`/
//! `JoinNextRow`, `JsonTableResetRowPattern`/`ResetNestedPlan` — is ported 1:1
//! here. The pieces that touch the *executor* subsystem (the
//! `TableFuncScanState`/`PlanState`/`ExprState` nodes, `ExecEvalExpr`,
//! `exprType`/`exprTypmod`, the `JsonTablePlan` node tags, the per-column
//! `caseValue` plumbing) are reached through the JSON_TABLE seams, since those
//! nodes and the expression evaluator are separate ports.
//!
//! The jsonpath evaluation each row pattern needs is done in-crate via
//! [`crate::executeJsonPathPublic`].

use ::mcx::Mcx;

use utils_error::{ereport, PgError, PgResult};
use ::types_error::ERROR;

use crate::seam;
use seam::{JsonTablePlan, JsonTableVariable};

use crate::{
    executeJsonPathPublic, CountJsonPathVarsPub, GetJsonPathVarPub, JsonPathVariable, JsonPathVars,
    JsonValueList, JsonValueListClearPub, JsonValueListInitIteratorPub, JsonValueListIterator,
    JsonValueListNextPub, JsonbValueToJsonbPub,
};

/// Random number to identify [`JsonTableExecContext`] for sanity checking
/// (jsonpath_exec.c:219).
const JSON_TABLE_EXEC_CONTEXT_MAGIC: i32 = 418352867;

/// The kind of a `JsonTablePlan` node (C: `IsA(plan, JsonTablePathScan)` /
/// `JsonTableSiblingJoin`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonTablePlanKind {
    /// C: `JsonTablePathScan`.
    PathScan,
    /// C: `JsonTableSiblingJoin`.
    SiblingJoin,
}

fn plan_kind(plan: &JsonTablePlan) -> JsonTablePlanKind {
    match plan {
        JsonTablePlan::PathScan(_) => JsonTablePlanKind::PathScan,
        JsonTablePlan::SiblingJoin(_) => JsonTablePlanKind::SiblingJoin,
    }
}

/// C: `struct JsonTablePlanRowSource` (jsonpath_exec.c:167-171).
///
/// In C `value` is a `Datum` pointing at a jsonb varlena; we carry the varlena
/// bytes directly (produced in-crate by `JsonbValueToJsonb`), so the
/// nested-plan row pattern can be re-evaluated without a fmgr round-trip.
#[derive(Clone, Debug, Default)]
struct JsonTablePlanRowSource {
    /// C: `Datum value` — the row-pattern jsonb varlena bytes when not null.
    value: Vec<u8>,
    /// C: `bool isnull`.
    isnull: bool,
}

/// C: `struct JsonTablePlanState` (jsonpath_exec.c:177-216).
struct JsonTablePlanState {
    /// Original plan (C: `JsonTablePlan *plan`).
    plan: JsonTablePlan,
    /// jsonpath to evaluate (C: `JsonPath *path`) — full on-disk bytes.
    path: Vec<u8>,
    /// PASSING arguments passed to the jsonpath executor (C: `List *args`).
    args: JsonPathVars,
    /// List + iterator of jsonpath result values (C: `found` / `iter`).
    found: JsonValueList,
    iter: JsonValueListIterator,
    /// Currently selected row (C: `current`).
    current: JsonTablePlanRowSource,
    /// Counter for ORDINAL columns (C: `int ordinal`).
    ordinal: i32,
    /// Nested plan, if any (C: `nested`).
    nested: Option<Box<JsonTablePlanState>>,
    /// Left sibling, if any (C: `left`).
    left: Option<Box<JsonTablePlanState>>,
    /// Right sibling, if any (C: `right`).
    right: Option<Box<JsonTablePlanState>>,
    /// Whether this plan is a nested child (C: `parent != NULL`).
    has_parent: bool,
}

/// C: `struct JsonTableExecContext` (jsonpath_exec.c:221-233).
pub struct JsonTableExecContext {
    /// C: `int magic`.
    magic: i32,
    /// State of the root-path plan (C: `rootplanstate`).
    rootplanstate: Box<JsonTablePlanState>,
    /// Per-column owning plan-state, recorded as the [`ChildStep`] path from the
    /// root plan-state (C: `JsonTablePlanState **colplanstates`).
    colplan_paths: Vec<Vec<ChildStep>>,
}

/// One step in the path from the root plan-state to a column's owning plan-state.
#[derive(Clone, Copy, Debug)]
enum ChildStep {
    Nested,
    Left,
    Right,
}

/// C: `GetJsonTableExecContext` (jsonpath_exec.c:4088).
fn check_magic(cxt: &JsonTableExecContext, fname: &str) -> PgResult<()> {
    if cxt.magic != JSON_TABLE_EXEC_CONTEXT_MAGIC {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("{fname} called with invalid TableFuncScanState"))
            .into_error());
    }
    Ok(())
}

/// Convert the seam-provided PASSING variables into the executor's
/// [`JsonPathVariable`] vocabulary.
fn vars_from_table_vars(vars: Vec<JsonTableVariable>) -> JsonPathVars {
    JsonPathVars::List(
        vars.into_iter()
            .map(|v| JsonPathVariable {
                name: v.name,
                typid: v.typid,
                typmod: v.typmod,
                value: v.value,
                value_bytes: v.value_bytes,
                isnull: v.isnull,
            })
            .collect(),
    )
}

/// C: `JsonTableInitOpaque` (jsonpath_exec.c:4109) — `TableFuncRoutine` callback.
/// Returns the opaque context the executor stores in `state->opaque`.
///
/// The executor (`nodeTableFuncscan`, holder of the `TableFuncScanState`) builds
/// the root [`JsonTablePlan`] from `tf->plan`, evaluates the PASSING argument
/// expressions (`state->passingvalexprs` via `ExecEvalExpr`) into
/// `args`, and supplies the column count (`list_length(tf->colvalexprs)`),
/// because those touch executor state / the expression evaluator. The
/// row-pattern plan-walk machinery below is wholly in-crate.
pub fn JsonTableInitOpaque(
    rootplan: JsonTablePlan,
    args: Vec<JsonTableVariable>,
    ncols: usize,
) -> PgResult<JsonTableExecContext> {
    let args = vars_from_table_vars(args);

    let mut colplan_paths: Vec<Vec<ChildStep>> = vec![Vec::new(); ncols];

    // Initialize plan for the root path and, recursively, any child plans.
    let rootplanstate =
        JsonTableInitPlan(rootplan, false, &args, &mut colplan_paths, &mut Vec::new())?;

    Ok(JsonTableExecContext {
        magic: JSON_TABLE_EXEC_CONTEXT_MAGIC,
        rootplanstate: Box::new(rootplanstate),
        colplan_paths,
    })
}

/// C: `JsonTableInitPlan` (jsonpath_exec.c:4191).
///
/// `cur_path` is the path of [`ChildStep`]s from the root to the plan being
/// initialized; it records each column's owning plan-state path in
/// `colplan_paths` so [`JsonTableGetValue`] can locate the plan-state later.
fn JsonTableInitPlan(
    plan: JsonTablePlan,
    has_parent: bool,
    args: &JsonPathVars,
    colplan_paths: &mut [Vec<ChildStep>],
    cur_path: &mut Vec<ChildStep>,
) -> PgResult<JsonTablePlanState> {
    match plan {
        JsonTablePlan::PathScan(scan) => {
            // Record this plan-state's path for each column it owns
            // (C: `cxt->colplanstates[i] = planstate`).
            let mut i = scan.col_min;
            while i >= 0 && i <= scan.col_max {
                colplan_paths[i as usize] = cur_path.clone();
                i += 1;
            }

            let nested = if let Some(child) = scan.child.clone() {
                cur_path.push(ChildStep::Nested);
                let ns = JsonTableInitPlan(*child, true, args, colplan_paths, cur_path)?;
                cur_path.pop();
                Some(Box::new(ns))
            } else {
                None
            };

            let path = scan.path.clone();
            Ok(JsonTablePlanState {
                path,
                plan: JsonTablePlan::PathScan(scan),
                args: args.clone(),
                found: JsonValueList::default(),
                iter: JsonValueListInitIteratorPub(&JsonValueList::default()),
                current: JsonTablePlanRowSource {
                    value: Vec::new(),
                    isnull: true,
                },
                ordinal: 0,
                nested,
                left: None,
                right: None,
                has_parent,
            })
        }
        JsonTablePlan::SiblingJoin(join) => {
            cur_path.push(ChildStep::Left);
            let left =
                JsonTableInitPlan(*join.lplan.clone(), has_parent, args, colplan_paths, cur_path)?;
            cur_path.pop();

            cur_path.push(ChildStep::Right);
            let right =
                JsonTableInitPlan(*join.rplan.clone(), has_parent, args, colplan_paths, cur_path)?;
            cur_path.pop();

            Ok(JsonTablePlanState {
                path: Vec::new(),
                plan: JsonTablePlan::SiblingJoin(join),
                args: args.clone(),
                found: JsonValueList::default(),
                iter: JsonValueListInitIteratorPub(&JsonValueList::default()),
                current: JsonTablePlanRowSource {
                    value: Vec::new(),
                    isnull: true,
                },
                ordinal: 0,
                nested: None,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
                has_parent,
            })
        }
    }
}

/// C: `JsonTableSetDocument` (jsonpath_exec.c:4238) — `TableFuncRoutine`
/// callback. `value` is the input document jsonb varlena bytes
/// (`DatumGetJsonbP(value)`); the provider supplies the detoasted bytes.
pub fn JsonTableSetDocument(
    mcx: Mcx<'_>,
    cxt: &mut JsonTableExecContext,
    value: &[u8],
) -> PgResult<()> {
    check_magic(cxt, "JsonTableSetDocument")?;
    JsonTableResetRowPattern(mcx, &mut cxt.rootplanstate, value)
}

/// C: `JsonTableResetRowPattern` (jsonpath_exec.c:4251). `item` is the
/// row-pattern document jsonb varlena bytes (C: `DatumGetJsonbP(item)`).
fn JsonTableResetRowPattern(
    mcx: Mcx<'_>,
    planstate: &mut JsonTablePlanState,
    item: &[u8],
) -> PgResult<()> {
    let error_on_error = match &planstate.plan {
        JsonTablePlan::PathScan(scan) => scan.error_on_error,
        JsonTablePlan::SiblingJoin(_) => {
            return Err(ereport(ERROR)
                .errmsg_internal("JsonTableResetRowPattern on a non-PathScan plan")
                .into_error());
        }
    };

    JsonValueListClearPub(&mut planstate.found);

    let mut found = JsonValueList::default();
    let res = executeJsonPathPublic(
        mcx,
        &planstate.path,
        &planstate.args,
        GetJsonPathVarPub,
        CountJsonPathVarsPub,
        item,
        error_on_error,
        Some(&mut found),
        true,
    )?;
    planstate.found = found;

    if crate::jper_is_error(res) {
        debug_assert!(!error_on_error);
        JsonValueListClearPub(&mut planstate.found);
    }

    // Reset plan iterator to the beginning of the item list.
    planstate.iter = JsonValueListInitIteratorPub(&planstate.found);
    planstate.current.value = Vec::new();
    planstate.current.isnull = true;
    planstate.ordinal = 0;

    Ok(())
}

/// C: `JsonTablePlanNextRow` (jsonpath_exec.c:4291).
fn JsonTablePlanNextRow(mcx: Mcx<'_>, planstate: &mut JsonTablePlanState) -> PgResult<bool> {
    match plan_kind(&planstate.plan) {
        JsonTablePlanKind::PathScan => JsonTablePlanScanNextRow(mcx, planstate),
        JsonTablePlanKind::SiblingJoin => JsonTablePlanJoinNextRow(mcx, planstate),
    }
}

/// C: `JsonTablePlanScanNextRow` (jsonpath_exec.c:4318).
fn JsonTablePlanScanNextRow(mcx: Mcx<'_>, planstate: &mut JsonTablePlanState) -> PgResult<bool> {
    // If planstate already has an active row and there is a nested plan, check if
    // it has an active row to join with the former.
    if !planstate.current.isnull {
        if let Some(nested) = planstate.nested.as_mut() {
            if JsonTablePlanNextRow(mcx, nested)? {
                return Ok(true);
            }
        }
    }

    // Fetch new row from the list of found values to set as active.
    let jbv = JsonValueListNextPub(&mut planstate.iter);

    // End of list?
    let jbv = match jbv {
        None => {
            planstate.current.value = Vec::new();
            planstate.current.isnull = true;
            return Ok(false);
        }
        Some(v) => v,
    };

    // Set current row item for subsequent JsonTableGetValue() calls.
    planstate.current.value = JsonbValueToJsonbPub(mcx, &jbv)?;
    planstate.current.isnull = false;

    // Next row!
    planstate.ordinal += 1;

    // Process nested plan(s), if any.
    if planstate.nested.is_some() {
        // Re-evaluate the nested path using the above parent row.
        let parent_current = planstate.current.clone();
        let mut nested = planstate
            .nested
            .take()
            .ok_or_else(|| PgError::error("JsonTablePlanScanNextRow: nested plan is NULL"))?;
        JsonTableResetNestedPlan(mcx, &mut nested, &parent_current)?;
        // Now fetch the nested plan's current row to be joined.
        let _ = JsonTablePlanNextRow(mcx, &mut nested)?;
        planstate.nested = Some(nested);
    }

    Ok(true)
}

/// C: `JsonTableResetNestedPlan` (jsonpath_exec.c:4380).
fn JsonTableResetNestedPlan(
    mcx: Mcx<'_>,
    planstate: &mut JsonTablePlanState,
    parent_current: &JsonTablePlanRowSource,
) -> PgResult<()> {
    // This better be a child plan.
    debug_assert!(planstate.has_parent);

    match plan_kind(&planstate.plan) {
        JsonTablePlanKind::PathScan => {
            if !parent_current.isnull {
                JsonTableResetRowPattern(mcx, planstate, &parent_current.value)?;
            }
            // If this plan itself has a child nested plan, it will be reset when
            // the caller calls JsonTablePlanNextRow() on this plan.
        }
        JsonTablePlanKind::SiblingJoin => {
            let mut left = planstate.left.take().ok_or_else(|| {
                PgError::error("JsonTableResetNestedPlan: left sibling plan is NULL")
            })?;
            JsonTableResetNestedPlan(mcx, &mut left, parent_current)?;
            planstate.left = Some(left);

            let mut right = planstate.right.take().ok_or_else(|| {
                PgError::error("JsonTableResetNestedPlan: right sibling plan is NULL")
            })?;
            JsonTableResetNestedPlan(mcx, &mut right, parent_current)?;
            planstate.right = Some(right);
        }
    }
    Ok(())
}

/// C: `JsonTablePlanJoinNextRow` (jsonpath_exec.c:4409).
fn JsonTablePlanJoinNextRow(mcx: Mcx<'_>, planstate: &mut JsonTablePlanState) -> PgResult<bool> {
    // Fetch row from left sibling.
    let mut left = planstate
        .left
        .take()
        .ok_or_else(|| PgError::error("JsonTablePlanJoinNextRow: left sibling plan is NULL"))?;
    let mut right = planstate
        .right
        .take()
        .ok_or_else(|| PgError::error("JsonTablePlanJoinNextRow: right sibling plan is NULL"))?;

    let result = if !JsonTablePlanNextRow(mcx, &mut left)? {
        // Left sibling ran out of rows, so start fetching from the right.
        JsonTablePlanNextRow(mcx, &mut right)?
    } else {
        true
    };

    planstate.left = Some(left);
    planstate.right = Some(right);
    Ok(result)
}

/// C: `JsonTableFetchRow` (jsonpath_exec.c:4436) — `TableFuncRoutine` callback.
pub fn JsonTableFetchRow(mcx: Mcx<'_>, cxt: &mut JsonTableExecContext) -> PgResult<bool> {
    check_magic(cxt, "JsonTableFetchRow")?;
    JsonTablePlanNextRow(mcx, &mut cxt.rootplanstate)
}

/// The current-row source data for a JSON_TABLE column, extracted from the
/// owning plan-state (C: `cxt->colplanstates[colnum]->current` + `->ordinal`).
///
/// This is the pure-data half of C's `JsonTableGetValue`: the executor
/// (`nodeTableFuncscan`) holds the column `JsonExpr` `ExprState`s
/// (`colvalexprs`) and the `ExprContext`, so it performs the `ExecEvalExpr`
/// (with `caseValue_datum` = the row pattern) and the ORDINAL-column fallback
/// itself; this crate only supplies what it owns — the row-pattern jsonb bytes,
/// its null-ness, and the ordinal counter.
#[derive(Clone, Debug)]
pub struct JsonTableRowValue {
    /// C: `current->value` as the row-pattern jsonb varlena bytes (valid only
    /// when `!isnull`).
    pub value: Vec<u8>,
    /// C: `current->isnull` — the row pattern value is NULL.
    pub isnull: bool,
    /// C: `planstate->ordinal` — used for an ORDINAL column.
    pub ordinal: i32,
}

/// C: `JsonTableGetValue` (jsonpath_exec.c:4452), data half — locate column
/// `colnum`'s owning plan-state and report its current row-pattern value, its
/// null-ness, and the ordinal counter.
pub fn JsonTableCurrentRow(
    cxt: &mut JsonTableExecContext,
    colnum: i32,
) -> PgResult<JsonTableRowValue> {
    check_magic(cxt, "JsonTableGetValue")?;

    // Locate the column's owning plan-state via the recorded path.
    let path = cxt.colplan_paths[colnum as usize].clone();
    let planstate = follow_path(&cxt.rootplanstate, &path)?;
    Ok(JsonTableRowValue {
        value: planstate.current.value.clone(),
        isnull: planstate.current.isnull,
        ordinal: planstate.ordinal,
    })
}

/// C: `JsonTableDestroyOpaque` (jsonpath_exec.c:4174) — invalidate the context.
pub fn JsonTableDestroyOpaque(cxt: &mut JsonTableExecContext) -> PgResult<()> {
    check_magic(cxt, "JsonTableDestroyOpaque")?;
    cxt.magic = 0;
    Ok(())
}

/// Follow a [`ChildStep`] path from the root plan-state to a descendant.
fn follow_path<'a>(
    root: &'a JsonTablePlanState,
    path: &[ChildStep],
) -> PgResult<&'a JsonTablePlanState> {
    let mut cur = root;
    for step in path {
        cur = match step {
            ChildStep::Nested => cur
                .nested
                .as_ref()
                .ok_or_else(|| PgError::error("follow_path: nested plan is NULL"))?,
            ChildStep::Left => cur
                .left
                .as_ref()
                .ok_or_else(|| PgError::error("follow_path: left sibling plan is NULL"))?,
            ChildStep::Right => cur
                .right
                .as_ref()
                .ok_or_else(|| PgError::error("follow_path: right sibling plan is NULL"))?,
        };
    }
    Ok(cur)
}
