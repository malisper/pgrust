//! Seam declarations for the JIT *provider* (`src/backend/jit/llvm/llvmjit.c`),
//! the shared library `jit.c` loads via `_PG_jit_provider_init`.
//!
//! `jit.c` is provider-independent: it probes for the provider shared library
//! (`pkglib_path/<jit_provider>.so`), `load_external_function`s the
//! `_PG_jit_provider_init` symbol, and calls it to populate the
//! `JitProviderCallbacks` vtable (`reset_after_error`, `release_context`,
//! `compile_expr`).  Those callbacks then route into the provider.
//!
//! The LLVM provider is not in the port catalog (it is an optional, separately
//! built `--with-llvm` module), so these seams have no owner yet and a call
//! panics loudly.  They are only ever reached once a provider has been
//! *successfully* loaded, i.e. after `provider_init()` returns true — which it
//! cannot until the provider lands.  This is the mirror-PG-and-panic surface
//! for the provider boundary.

#![allow(non_snake_case)]

use nodes::execexpr::ExprState;

seam_core::seam!(
    /// `load_external_function(path, "_PG_jit_provider_init", true, NULL)`
    /// followed by `init(&provider)` (`jit.c` `provider_init`): load the JIT
    /// provider shared library and let it install its `JitProviderCallbacks`
    /// vtable.  `ereport(ERROR)`s if the library's dependencies are missing.
    /// Reached only when the provider shared library was found on disk.
    pub fn load_jit_provider_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `JitProviderCallbacks.reset_after_error()` — provider hook to reset its
    /// error-handling state after an error unwinds.  Invoked by
    /// `jit_reset_after_error()` only when a provider is loaded.
    pub fn provider_reset_after_error()
);

seam_core::seam!(
    /// `JitProviderCallbacks.release_context(context)` — provider hook to free
    /// the resources of one `JitContext` (the emitted functions), *not* the
    /// `JitContext` struct itself (`jit_release_context` `pfree`s that after).
    /// Borrows the context (the type-erased payload of the executor's `es_jit`
    /// handle); the caller drops the box afterwards.  Invoked by
    /// `jit_release_context()` only when a provider is loaded.
    pub fn provider_release_context(context: &dyn std::any::Any)
);

seam_core::seam!(
    /// `JitProviderCallbacks.compile_expr(state)` — provider hook to JIT compile
    /// one expression, returning whether it succeeded.  Invoked by
    /// `jit_compile_expr()` only when a provider is loaded.
    pub fn provider_compile_expr<'mcx>(state: &mut ExprState<'mcx>) -> bool
);
