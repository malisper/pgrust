//! The set-returning-function **materialize sink** — the lifetime-free bridge
//! that carries a live `ReturnSetInfo` (SFRM_Materialize protocol) across the
//! dual-home `types_fmgr` <-> `types_nodes` fcinfo boundary.
//!
//! ## Why a thread-local sink, not a struct field
//!
//! The executor frame (`types_nodes::FunctionCallInfoBaseData<'mcx>`) carries a
//! real `ReturnSetInfo<'mcx>` in `resultinfo`. The fmgr frame
//! (`types_fmgr::FunctionCallInfoBaseData`) carries only a tag-only
//! `Option<ContextNode>` (the WONTFIX dual-home: the two crates never dep each
//! other and live in incompatible `no_std`/`std` + lifetime worlds, so the
//! `ReturnSetInfo` node cannot travel *through* the fmgr fcinfo struct).
//!
//! C's `ExecMakeTableFunctionResult` builds a `ReturnSetInfo` on the stack,
//! points `fcinfo->resultinfo` at it, and calls the function via
//! `FunctionCallInvoke`; the callee (`fmgr_sql` / `plpgsql_call_handler`) reads
//! `rsinfo->allowedModes & SFRM_Materialize`, runs the body, and fills
//! `rsinfo->setResult` (a tuplestore) + `rsinfo->setDesc`. The owned model
//! reproduces exactly that protocol with a **process-thread-local stack** of
//! materialize sinks (mirroring `fmgr_core::CurrentFcinfo`, which already
//! snapshots the in-flight fcinfo on a thread-local stack to avoid aliasing the
//! exclusive `&mut fcinfo`): the SRF dispatcher pushes a sink before the by-OID
//! call, the callee resolves it with [`with_top`] and appends its materialized
//! rows, and the dispatcher pops it after the call and rebuilds the executor's
//! `Tuplestorestate<'mcx>` from the collected rows.
//!
//! The collected rows are **owned, lifetime-free** (`usize` word +
//! [`RefPayload`] + null flag per column) -- exactly the form `fmgr_sql`'s
//! capture receiver already produces and the form `tuplestore_putvalues`
//! reconstructs in the per-query arena. No `'mcx` value ever crosses the
//! thread-local.

use std::cell::RefCell;

use crate::boundary::RefPayload;

/// One materialized result column: the owned `(word | by-ref payload, isnull)`
/// split -- the same shape `fmgr_sql`'s capture receiver and the fmgr arg edge
/// (`datum_to_ref_arg`) use. A by-value column carries its bare machine word in
/// `value` (`ref_payload == None`); a by-reference column carries its owned
/// referent image in `ref_payload` (`value` meaningless).
#[derive(Debug, Default)]
pub struct MatCell {
    /// Column by-value word (valid when `ref_payload` is `None`).
    pub value: usize,
    /// Column by-reference payload (valid for a by-reference column type).
    pub ref_payload: Option<RefPayload>,
    /// Column NULL flag.
    pub isnull: bool,
}

/// One materialized result row -- a column-major series of [`MatCell`].
pub type MatRow = Vec<MatCell>;

/// A live SFRM_Materialize sink: the SRF dispatcher's `ReturnSetInfo` projected
/// to the lifetime-free fields the callee reads/writes across the fmgr boundary.
#[derive(Debug, Default)]
pub struct MatSrfSink {
    /// `rsinfo->allowedModes` (the `SFRM_*` bitmask) the caller set. The callee
    /// checks `allowedModes & SFRM_Materialize` before choosing materialize mode.
    pub allowed_modes: i32,
    /// `rsinfo->setDesc` column type OIDs -- the descriptor the callee built for
    /// the materialized rows (empty until the callee sets it). For a scalar
    /// SETOF function this is the single result column's type; the dispatcher
    /// cross-checks it against the caller's `expectedDesc`.
    pub set_desc_types: Vec<types_core::Oid>,
    /// The accumulated materialized rows (`rsinfo->setResult`, the tuplestore).
    pub rows: Vec<MatRow>,
    /// Whether the callee actually ran in materialize mode and filled the sink
    /// (`rsinfo->returnMode == SFRM_Materialize`). A callee that cannot
    /// materialize leaves this `false`, and the dispatcher errors / falls back.
    pub materialized: bool,
}

thread_local! {
    /// The per-thread stack of in-flight SRF materialize sinks (nested via a
    /// SETOF function whose body calls another SETOF function). Innermost last.
    static MAT_SRF_STACK: RefCell<Vec<MatSrfSink>> = const { RefCell::new(Vec::new()) };
}

/// RAII pop guard for the materialize-sink stack -- panic-safe, so a `PGFunction`
/// that `ereport(ERROR)`s (unwinds) through the dispatch never leaks a dead sink
/// frame. Yields the popped [`MatSrfSink`] on `take`.
#[must_use = "drop or `take` the guard to pop the materialize sink"]
pub struct MatSrfGuard {
    popped: bool,
}

impl MatSrfGuard {
    /// Pop the sink this guard owns and return it (the dispatcher reads the
    /// collected rows / descriptor back out). Disarms the `Drop` pop.
    pub fn take(mut self) -> MatSrfSink {
        self.popped = true;
        MAT_SRF_STACK.with(|s| s.borrow_mut().pop().unwrap_or_default())
    }
}

impl Drop for MatSrfGuard {
    fn drop(&mut self) {
        if !self.popped {
            MAT_SRF_STACK.with(|s| {
                s.borrow_mut().pop();
            });
        }
    }
}

/// Push a materialize sink with the caller's `allowedModes` and return its RAII
/// guard. C: building the `ReturnSetInfo` and pointing `fcinfo->resultinfo` at
/// it just before `FunctionCallInvoke`.
pub fn push(allowed_modes: i32) -> MatSrfGuard {
    MAT_SRF_STACK.with(|s| {
        s.borrow_mut().push(MatSrfSink {
            allowed_modes,
            ..MatSrfSink::default()
        });
    });
    MatSrfGuard { popped: false }
}

/// Run `f` against the innermost in-flight materialize sink (C: the callee
/// reading/writing `fcinfo->resultinfo`). `None` when no SRF dispatch is on this
/// thread's stack (the callee was reached as an ordinary scalar call -- C's
/// `rsinfo == NULL`).
pub fn with_top<R>(f: impl FnOnce(Option<&mut MatSrfSink>) -> R) -> R {
    MAT_SRF_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        f(stack.last_mut())
    })
}

/// Whether a materialize sink is in flight on this thread (C: `rsinfo != NULL`).
pub fn is_active() -> bool {
    MAT_SRF_STACK.with(|s| !s.borrow().is_empty())
}
