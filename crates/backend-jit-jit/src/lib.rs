//! Port of `src/backend/jit/jit.c` — provider-independent JIT infrastructure.
//!
//! This unit loads a JIT provider shared library on demand, redirects calls
//! into it, and owns the small error-handling/instrumentation glue around it.
//! No code specific to a particular JIT implementation lives here; the actual
//! provider (`jit/llvm/llvmjit.c`) is an optional, separately built module and
//! is *not* in the port catalog — its `JitProviderCallbacks` vtable + loader
//! are the `backend-jit-llvmjit-seams` mirror-and-panic boundary, reachable
//! only once a provider has been successfully loaded.
//!
//! The JIT GUC backing variables (`jit_enabled`, `jit_provider`, the
//! `jit_*_above_cost` reals, etc.) are owned by the GUC-tables unit in this
//! repo; the functions here read `jit_enabled`/`jit_provider` through it.
//!
//! `JitContext`/`JitInstrumentation`/`PGJIT_*` ABI types live in
//! `types-execparallel` (shared with the parallel executor's DSM layout); the
//! `es_jit` context itself crosses to/from the executor as the type-erased
//! payload of an `Opaque` handle.

#![allow(non_snake_case)]

mod fmgr_builtins;

use std::cell::{Cell, RefCell};

use mcx::Mcx;
use types_datum::Datum;
use types_error::{PgResult, DEBUG1};
use types_execparallel::JitInstrumentation;
use types_nodes::execexpr::ExprState;

use backend_jit_llvmjit_seams as provider;
use backend_storage_file_fd_seams as fd;
use backend_utils_misc_guc_tables::vars;

/// `DLSUFFIX` — platform shared-library suffix (`.so` on the build platforms;
/// mirrors `backend-utils-fmgr-dfmgr`).
const DLSUFFIX: &str = ".so";

thread_local! {
    /// `static bool provider_successfully_loaded` — a provider vtable has been
    /// installed and is safe to call.
    static PROVIDER_SUCCESSFULLY_LOADED: Cell<bool> = const { Cell::new(false) };

    /// `static bool provider_failed_loading` — a load attempt failed; don't
    /// retry (loading isn't cheap).
    static PROVIDER_FAILED_LOADING: Cell<bool> = const { Cell::new(false) };
}

/// `pkglib_path` (`globals.c`) — full path to the library directory, decoded
/// from the C `char[MAXPGPATH]` NUL-terminated form (owned by `init-small`).
fn pkglib_path() -> String {
    let bytes = backend_utils_init_small::globals::pkglib_path();
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..len]).into_owned()
}

/// `pg_jit_available(PG_FUNCTION_ARGS)` — SQL-level function returning whether
/// JIT is available in the current backend. Will attempt to load the JIT
/// provider if necessary.
///
/// `PG_RETURN_BOOL(provider_init())`.
pub fn pg_jit_available<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    Ok(Datum::from_bool(provider_init()?))
}

/// `provider_init(void)` — return whether a JIT provider has successfully been
/// loaded, caching the result.
fn provider_init() -> PgResult<bool> {
    /* don't even try to load if not enabled */
    if !vars::jit_enabled.read() {
        return Ok(false);
    }

    /*
     * Don't retry loading after failing - attempting to load JIT provider
     * isn't cheap.
     */
    if PROVIDER_FAILED_LOADING.with(Cell::get) {
        return Ok(false);
    }
    if PROVIDER_SUCCESSFULLY_LOADED.with(Cell::get) {
        return Ok(true);
    }

    /*
     * Check whether shared library exists. We do that check before actually
     * attempting to load the shared library (via load_external_function()),
     * because that'd error out in case the shlib isn't available.
     *
     * C: snprintf(path, MAXPGPATH, "%s/%s%s", pkglib_path, jit_provider, DLSUFFIX);
     */
    let jit_provider = vars::jit_provider.read().unwrap_or_default();
    let path = format!("{}/{}{}", pkglib_path(), jit_provider, DLSUFFIX);
    backend_utils_error::elog(
        DEBUG1,
        format!("probing availability of JIT provider at {path}"),
    )?;
    if !fd::pg_file_exists::call(&path)? {
        backend_utils_error::elog(
            DEBUG1,
            "provider not available, disabling JIT for current session",
        )?;
        PROVIDER_FAILED_LOADING.with(|c| c.set(true));
        return Ok(false);
    }

    /*
     * If loading functions fails, signal failure. We do so because
     * load_external_function() might error out despite the above check if e.g.
     * the library's dependencies aren't installed. We want to signal ERROR in
     * that case, so the user is notified, but we don't want to continually
     * retry.
     */
    PROVIDER_FAILED_LOADING.with(|c| c.set(true));

    /*
     * and initialize
     *
     * C: init = (JitProviderInit) load_external_function(path,
     *           "_PG_jit_provider_init", true, NULL);
     *    init(&provider);
     *
     * The provider library load + vtable install lives behind the provider
     * seam (owner: jit/llvm/llvmjit.c, not in the port catalog).
     */
    provider::load_jit_provider_init::call()?;

    PROVIDER_SUCCESSFULLY_LOADED.with(|c| c.set(true));
    PROVIDER_FAILED_LOADING.with(|c| c.set(false));

    backend_utils_error::elog(
        DEBUG1,
        "successfully loaded JIT provider in current session",
    )?;

    Ok(true)
}

/// `jit_reset_after_error(void)` — reset the JIT provider's error handling.
/// Called after an error has been thrown and the main loop has re-established
/// control.
pub fn jit_reset_after_error() {
    if PROVIDER_SUCCESSFULLY_LOADED.with(Cell::get) {
        provider::provider_reset_after_error::call();
    }
}

/// `jit_release_context(JitContext *context)` — release the resources required
/// by one JIT context.
///
/// The C `pfree(context)` is the `Box<dyn Any>` drop at the end of this
/// function (the context was the type-erased payload of `es_jit`).
pub fn jit_release_context(context: std::boxed::Box<dyn std::any::Any>) {
    if PROVIDER_SUCCESSFULLY_LOADED.with(Cell::get) {
        /* provider frees the emitted functions, not the context struct */
        provider::provider_release_context::call(&*context);
    }

    /* pfree(context) — runs in both branches; drop the box. */
    drop(context);
}

/// `jit_compile_expr(struct ExprState *state)` — ask the provider to JIT
/// compile an expression. Returns true if successful, false if not.
///
/// In this repo the `PlanState.state` back-pointer is intentionally not carried
/// (the executor threads `EState` explicitly), so the C reads of
/// `state->parent->state->es_jit_flags` are supplied by the caller as
/// `jit_flags` (the caller holds the owning `EState`). `has_parent` mirrors the
/// `state->parent` NULL guard.
pub fn jit_compile_expr<'mcx>(
    state: &mut ExprState<'mcx>,
    has_parent: bool,
    jit_flags: i32,
) -> PgResult<bool> {
    /*
     * We can easily create a one-off context for functions without an
     * associated PlanState (and thus EState). But because there's no executor
     * shutdown callback that could deallocate the created function, they'd
     * live to the end of the transactions [...]. Therefore, at least for now,
     * don't create a JITed function in those circumstances.
     */
    if !has_parent {
        return Ok(false);
    }

    /* if no jitting should be performed at all */
    if jit_flags & types_execparallel::PGJIT_PERFORM == 0 {
        return Ok(false);
    }

    /* or if expressions aren't JITed */
    if jit_flags & types_execparallel::PGJIT_EXPR == 0 {
        return Ok(false);
    }

    /* this also takes !jit_enabled into account */
    if provider_init()? {
        return Ok(provider::provider_compile_expr::call(state));
    }

    Ok(false)
}

/// `InstrJitAgg(JitInstrumentation *dst, JitInstrumentation *add)` — aggregate
/// JIT instrumentation information.
pub fn InstrJitAgg(dst: &mut JitInstrumentation, add: &JitInstrumentation) {
    dst.created_functions = dst.created_functions.wrapping_add(add.created_functions);
    dst.generation_counter.add(add.generation_counter);
    dst.deform_counter.add(add.deform_counter);
    dst.inlining_counter.add(add.inlining_counter);
    dst.optimization_counter.add(add.optimization_counter);
    dst.emission_counter.add(add.emission_counter);
}

// ---------------------------------------------------------------------------
// GUC backing storage
//
// The JIT GUC variables are plain C globals defined in `jit.c` (the
// `conf->variable` storage the GUC machinery reads/writes directly — none are
// read from the ControlFile). This unit owns them and installs the accessor
// pairs over its own backing store, mirroring the `max_prepared_xacts` pattern
// in `twophase`. Boot values match the `config_*` `boot_val`s in the GUC
// tables (jit.c initializers, with `jit_provider`'s NULL replaced by the GUC
// `boot_val` "llvmjit").
// ---------------------------------------------------------------------------

thread_local! {
    /// `bool jit_enabled = true` (jit.c:32).
    static JIT_ENABLED: Cell<bool> = const { Cell::new(true) };
    /// `bool jit_debugging_support = false` (jit.c:34).
    static JIT_DEBUGGING_SUPPORT: Cell<bool> = const { Cell::new(false) };
    /// `bool jit_dump_bitcode = false` (jit.c:35).
    static JIT_DUMP_BITCODE: Cell<bool> = const { Cell::new(false) };
    /// `bool jit_expressions = true` (jit.c:36).
    static JIT_EXPRESSIONS: Cell<bool> = const { Cell::new(true) };
    /// `bool jit_profiling_support = false` (jit.c:37).
    static JIT_PROFILING_SUPPORT: Cell<bool> = const { Cell::new(false) };
    /// `bool jit_tuple_deforming = true` (jit.c:38).
    static JIT_TUPLE_DEFORMING: Cell<bool> = const { Cell::new(true) };
    /// `double jit_above_cost = 100000` (jit.c:39).
    static JIT_ABOVE_COST: Cell<f64> = const { Cell::new(100000.0) };
    /// `double jit_inline_above_cost = 500000` (jit.c:40).
    static JIT_INLINE_ABOVE_COST: Cell<f64> = const { Cell::new(500000.0) };
    /// `double jit_optimize_above_cost = 500000` (jit.c:41).
    static JIT_OPTIMIZE_ABOVE_COST: Cell<f64> = const { Cell::new(500000.0) };
    /// `char *jit_provider = NULL` (jit.c:33); GUC `boot_val` is "llvmjit".
    static JIT_PROVIDER: RefCell<Option<String>> =
        RefCell::new(Some(String::from("llvmjit")));
}

/// Install this unit's inward seams (consumed by the executor) and its GUC
/// variable accessors (consumed by the GUC machinery). Wired into
/// `seams-init::init_all()`.
pub fn init_seams() {
    backend_jit_jit_seams::jit_release_context::set(jit_release_context);
    backend_jit_jit_seams::jit_reset_after_error::set(jit_reset_after_error);

    fmgr_builtins::register_jit_builtins();

    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    vars::jit_enabled.install(GucVarAccessors {
        get: || JIT_ENABLED.with(Cell::get),
        set: |v| JIT_ENABLED.with(|c| c.set(v)),
    });
    vars::jit_debugging_support.install(GucVarAccessors {
        get: || JIT_DEBUGGING_SUPPORT.with(Cell::get),
        set: |v| JIT_DEBUGGING_SUPPORT.with(|c| c.set(v)),
    });
    vars::jit_dump_bitcode.install(GucVarAccessors {
        get: || JIT_DUMP_BITCODE.with(Cell::get),
        set: |v| JIT_DUMP_BITCODE.with(|c| c.set(v)),
    });
    vars::jit_expressions.install(GucVarAccessors {
        get: || JIT_EXPRESSIONS.with(Cell::get),
        set: |v| JIT_EXPRESSIONS.with(|c| c.set(v)),
    });
    vars::jit_profiling_support.install(GucVarAccessors {
        get: || JIT_PROFILING_SUPPORT.with(Cell::get),
        set: |v| JIT_PROFILING_SUPPORT.with(|c| c.set(v)),
    });
    vars::jit_tuple_deforming.install(GucVarAccessors {
        get: || JIT_TUPLE_DEFORMING.with(Cell::get),
        set: |v| JIT_TUPLE_DEFORMING.with(|c| c.set(v)),
    });
    vars::jit_above_cost.install(GucVarAccessors {
        get: || JIT_ABOVE_COST.with(Cell::get),
        set: |v| JIT_ABOVE_COST.with(|c| c.set(v)),
    });
    vars::jit_inline_above_cost.install(GucVarAccessors {
        get: || JIT_INLINE_ABOVE_COST.with(Cell::get),
        set: |v| JIT_INLINE_ABOVE_COST.with(|c| c.set(v)),
    });
    vars::jit_optimize_above_cost.install(GucVarAccessors {
        get: || JIT_OPTIMIZE_ABOVE_COST.with(Cell::get),
        set: |v| JIT_OPTIMIZE_ABOVE_COST.with(|c| c.set(v)),
    });
    vars::jit_provider.install(GucVarAccessors {
        get: || JIT_PROVIDER.with(|c| c.borrow().clone()),
        set: |v| JIT_PROVIDER.with(|c| *c.borrow_mut() = v),
    });
}
