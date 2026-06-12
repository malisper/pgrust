//! Seam declarations for the `backend-jit-jit` unit (`jit/jit.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `jit_release_context(context)` (jit.c): release a JIT context (frees
    /// the emitted functions' resources). The context crosses as the
    /// type-erased payload of the executor's `es_jit` `Opaque` handle; the
    /// owner downcasts (loud panic on mismatch) and consumes it.
    pub fn jit_release_context(context: std::boxed::Box<dyn std::any::Any>)
);
