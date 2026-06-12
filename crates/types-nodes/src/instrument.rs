//! Per-node instrumentation vocabulary (executor/instrument.h), trimmed.

/// `Instrumentation` (instrument.h) — per-node timing/row-count statistics.
/// Trimmed to a presence marker: ports so far only test
/// `PlanState.instrument` for NULL-ness and hand it across the `InstrEndLoop`
/// seam; the counters arrive with the instrument.c owner.
#[derive(Debug, Default)]
pub struct Instrumentation;
