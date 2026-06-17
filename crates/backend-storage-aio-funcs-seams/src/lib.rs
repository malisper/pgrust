//! Seam declarations for the `backend-storage-aio-funcs` unit
//! (`storage/aio/aio_funcs.c`: the `pg_get_aios()` SQL set-returning function).
//!
//! The *algorithmic logic* of aio_funcs.c — the lock-free per-handle copy/retry
//! protocol, the per-column row layout, and the iovec byte-length accumulation —
//! is ported in-crate in the owner crate. aio_funcs.c is otherwise a thin SQL
//! wrapper that reaches into genuinely-external subsystems, each crossing one of
//! the seams declared here:
//!
//!  * **`storage/proc.c` / `storage/proc.h`** — `GetPGProcByNumber(owner)->pid`,
//!    the owning backend's OS pid, folded into [`proc_pid_by_number`].
//!  * **`utils/fmgr/funcapi.c` / `utils/sort/tuplestore.c`** — the
//!    `tuplestore_putvalues` of an `InitMaterializedSRF`-prepared
//!    `ReturnSetInfo`, plus the `Int32GetDatum` / `Int64GetDatum` /
//!    `BoolGetDatum` / `CStringGetTextDatum` Datum assembly it consumes; folded
//!    into [`tuplestore_putvalues`], which receives the already-computed owned
//!    [`AioRow`].

extern crate alloc;

use alloc::string::String;

use types_core::ProcNumber;
use types_error::PgResult;

/// One emitted `pg_get_aios()` row (`PG_GET_AIOS_COLS = 15` columns). Field
/// order matches the C `values[]`/`nulls[]` column indices exactly. A `None`
/// in an `Option` column maps to `nulls[i] = true`; a `Some`/scalar maps to the
/// corresponding `Int32GetDatum`/`Int64GetDatum`/`BoolGetDatum`/
/// `CStringGetTextDatum`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AioRow {
    /// `values[0]` — owning pid (`Int32GetDatum(owner_pid)`); `None` when the
    /// owner pid is 0 (`nulls[0]`).
    pub pid: Option<i32>,
    /// `values[1]` — IO's id (`Int32GetDatum(ioh_id)`).
    pub io_id: i32,
    /// `values[2]` — IO's generation (`Int64GetDatum(start_generation)`).
    pub io_generation: i64,
    /// `values[3]` — IO's state name (`CStringGetTextDatum(...)`).
    pub state: String,
    /// `values[4]` — IO's operation name; `None` for a `HANDED_OUT` handle whose
    /// remaining columns are all NULL (`nulls[4..]`).
    pub operation: Option<String>,
    /// `values[5]` — op offset (`Int64GetDatum`); `None` for `PGAIO_OP_INVALID`
    /// or `HANDED_OUT`.
    pub off: Option<i64>,
    /// `values[6]` — op iovec byte length (`Int64GetDatum`); `None` likewise.
    pub length: Option<i64>,
    /// `values[7]` — IO's target name; `None` for `HANDED_OUT`.
    pub target: Option<String>,
    /// `values[8]` — length of the IO's data array (`Int16GetDatum`); `None` for
    /// `HANDED_OUT`.
    pub handle_data_len: Option<i16>,
    /// `values[9]` — raw syscall result (`Int32GetDatum`); `None` unless the IO
    /// reached a COMPLETED_* state (or `HANDED_OUT`).
    pub raw_result: Option<i32>,
    /// `values[10]` — distilled result status string; `None` for `HANDED_OUT`.
    pub result: Option<String>,
    /// `values[11]` — target description; `None` for `HANDED_OUT`.
    pub target_desc: Option<String>,
    /// `values[12]` — `flags & PGAIO_HF_SYNCHRONOUS` (`BoolGetDatum`); `None`
    /// for `HANDED_OUT`.
    pub f_sync: Option<bool>,
    /// `values[13]` — `flags & PGAIO_HF_REFERENCES_LOCAL`; `None` for
    /// `HANDED_OUT`.
    pub f_localmem: Option<bool>,
    /// `values[14]` — `flags & PGAIO_HF_BUFFERED`; `None` for `HANDED_OUT`.
    pub f_buffered: Option<bool>,
}

seam_core::seam!(
    /// `GetPGProcByNumber(owner)->pid` (`storage/proc.h`) — the OS pid of the
    /// backend that owns an AIO handle, by its `ProcNumber`. The unported
    /// `storage/lmgr/proc.c` PGPROC array owns the mapping.
    pub fn proc_pid_by_number(owner: ProcNumber) -> PgResult<i32>
);

seam_core::seam!(
    /// `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls)`
    /// (aio_funcs.c) — append one already-rendered [`AioRow`] to the
    /// `InitMaterializedSRF`-prepared materialized result set. The
    /// `Int32GetDatum` / `Int64GetDatum` / `BoolGetDatum` / `CStringGetTextDatum`
    /// Datum assembly the C performs inline is folded into the consumer.
    pub fn tuplestore_putvalues(row: AioRow) -> PgResult<()>
);
