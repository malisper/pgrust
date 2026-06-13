//! Array-accumulation builder vocabulary (`utils/array.h`), trimmed.
//!
//! `ArrayBuildStateAny` is the polymorphic accumulator that
//! `initArrayResultAny`/`accumArrayResultAny`/`makeArrayResultAny` (arrayfuncs.c)
//! pass between calls. nodeSubplan only threads it opaquely — it never inspects
//! the accumulated element/dimension state — so the heavy element/array
//! sub-states arrive with the arrayfuncs unit when it lands. The carrier is a
//! heterogeneous owned slot until then.

/// `ArrayBuildStateAny *` (utils/array.h) — the polymorphic array accumulator.
/// Trimmed to an owned opaque slot: nodeSubplan passes it through the
/// init/accum/make seams and never reads its contents.
#[derive(Debug, Default)]
pub struct ArrayBuildStateAny {
    /// The arrayfuncs-owned accumulator state (element ArrayBuildState or
    /// per-array ArrayBuildStateArr). Populated by the arrayfuncs unit; opaque
    /// to all other consumers.
    pub inner: Option<alloc::boxed::Box<dyn core::any::Any>>,
}
