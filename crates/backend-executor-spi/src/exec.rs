//! SPI execute / prepare / cursor legs and the `DestSPI` receiver.
//!
//! # Port status: seam-and-panic into genuinely-unported owners
//!
//! Every function here is a faithful structural placeholder whose body
//! `panic!`s (never `todo!`) with the exact prerequisite. They cannot be filled
//! until the owners SPI drives land:
//!
//! * `SPI_prepare` / `_SPI_prepare_plan` → `raw_parser` +
//!   `pg_analyze_and_rewrite_*` (`backend-tcop-postgres` +
//!   `backend-parser-analyze`, **todo**) and `CreateCachedPlan` /
//!   `CreateOneShotCachedPlan` / `CompleteCachedPlan`
//!   (`backend-utils-cache-plancache`, merged — but the one-shot / copy / parent
//!   seams are not yet declared);
//! * `SPI_execute_snapshot` / `_SPI_execute_plan` / `_SPI_pquery` →
//!   `GetCachedPlan` + `CreateQueryDesc` + `ExecutorStart/Run/Finish/End`
//!   (`backend-executor-execMain`, **needs-decomp**; #167 only wired a plain
//!   `DestNone`/`DestRemote` SELECT and guard-panics elsewhere) and the
//!   **`DestSPI` receiver**, which requires `CreateDestReceiver(DestSPI)` —
//!   the `backend-tcop-dest` receiver-value router keystone
//!   (#166-F0b / #168 / #169) that is *declared but installed by nobody*. The
//!   `DestSPI` vtable (`spi_dest_startup` / `spi_printtup`) can only be
//!   registered with a router once that router exists;
//! * cursor open/fetch/move → `PortalStart` / `PortalRunFetch`
//!   (`backend-tcop-pquery`, **todo**), plus the same dest-router for fetch.
//!
//! `spi_keepplan` / `spi_freeplan` / `spi_plan_is_valid` operate on an
//! `SpiPlanPtr` produced only by `spi_prepare`, so they are reachable only
//! after the prepare leg lands; they panic with the same prerequisite.

use backend_executor_spi_seams::TsRewriteResult;
use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_ri_triggers::{ResultColumn, SpiExecResult, SpiPlanPtr};
use types_tuple::Datum;

/// The prerequisite message shared by the execution-driver legs.
const NEED_EXECUTOR_DRIVER: &str = "backend-executor-spi: SPI_execute*/_SPI_pquery executor-driver leg \
    is decomp-blocked — needs backend-executor-execMain ExecutorStart/Run/Finish/End + CreateQueryDesc \
    for non-trivial plans (needs-decomp, #166/#167 only wired a plain DestNone SELECT) AND the \
    CreateDestReceiver(DestSPI) router (backend-tcop-dest receiver-value keystone, #166-F0b/#168/#169, \
    declared but installed by nobody). DestSPI vtable cannot be filled until the dest router exists.";

const NEED_PARSER: &str = "backend-executor-spi: SPI_prepare/_SPI_prepare_plan leg is decomp-blocked — \
    needs raw_parser (backend-tcop-postgres, todo) + pg_analyze_and_rewrite_* (backend-parser-analyze, \
    todo) + the plancache CreateOneShotCachedPlan/CompleteCachedPlan one-shot seams (not yet declared).";

const NEED_CURSOR: &str = "backend-executor-spi: SPI_cursor_open/fetch/move leg is decomp-blocked — \
    needs PortalStart/PortalRunFetch (backend-tcop-pquery, todo) + the CreateDestReceiver(DestSPI) \
    router (backend-tcop-dest keystone).";

/// `SPI_prepare(src, nargs, argtypes)` seam body.
///
/// Decomp-blocked: see [`NEED_PARSER`].
pub(crate) fn spi_prepare_seam(
    _querystr: &[u8],
    _argtypes: &[types_core::Oid],
) -> PgResult<Option<SpiPlanPtr>> {
    panic!("{NEED_PARSER}");
}

/// `SPI_keepplan(plan)` seam body. Reachable only via a prepared plan.
pub(crate) fn spi_keepplan_seam(_plan: SpiPlanPtr) -> PgResult<()> {
    panic!("{NEED_PARSER}");
}

/// `SPI_freeplan(plan)` seam body. Reachable only via a prepared plan.
pub(crate) fn spi_freeplan_seam(_plan: SpiPlanPtr) -> PgResult<()> {
    panic!("{NEED_PARSER}");
}

/// `SPI_plan_is_valid(plan)` seam body. Reachable only via a prepared plan.
pub(crate) fn spi_plan_is_valid_seam(_plan: SpiPlanPtr) -> bool {
    panic!("{NEED_PARSER}");
}

/// `SPI_execute_snapshot(plan, vals, nulls, snapshot, crosscheck, read_only,
/// fire_triggers, tcount)` seam body (the RI-trigger execution variant).
///
/// Decomp-blocked: see [`NEED_EXECUTOR_DRIVER`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn spi_execute_snapshot_seam<'mcx>(
    _plan: SpiPlanPtr,
    _vals: &[Datum<'mcx>],
    _nulls: &[bool],
    _snapshot: Option<types_snapshot::SnapshotData>,
    _crosscheck: Option<types_snapshot::SnapshotData>,
    _read_only: bool,
    _fire_triggers: bool,
    _tcount: i64,
) -> PgResult<SpiExecResult> {
    panic!("{NEED_EXECUTOR_DRIVER}");
}

/// `spi_first_row_columns` seam body: render the first result tuple's columns.
///
/// Decomp-blocked: this reads `SPI_tuptable->vals[0]`, which is populated only
/// by the `DestSPI` receiver during a real `_SPI_execute_plan`. See
/// [`NEED_EXECUTOR_DRIVER`].
pub(crate) fn spi_first_row_columns_seam<'mcx>(
    _mcx: Mcx<'mcx>,
    _attnums: &[i16],
) -> PgResult<PgVec<'mcx, ResultColumn<'mcx>>> {
    panic!("{NEED_EXECUTOR_DRIVER}");
}

/// `tsquery_rewrite_run(command)` seam body (the `ts_rewrite(query, text)` SPI
/// driver homed here per the seam contract; consumed by
/// `backend-utils-adt-ts-small`).
///
/// Decomp-blocked: drives `SPI_connect` → `SPI_prepare` →
/// `SPI_cursor_open`/`SPI_cursor_fetch`, i.e. the cursor + prepare + executor
/// legs above. See [`NEED_CURSOR`].
pub(crate) fn tsquery_rewrite_run_seam(_command: String) -> PgResult<TsRewriteResult> {
    panic!("{NEED_CURSOR}");
}
