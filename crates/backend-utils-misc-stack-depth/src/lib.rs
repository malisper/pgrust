//! `utils/misc/stack_depth.c` — process stack-depth monitoring and limiting.
//!
//! Port of PostgreSQL 18's `stack_depth.c` (PG18 split this out of
//! `tcop/postgres.c`). The module keeps a reference "base" stack address set
//! once at backend start; recursive routines call [`check_stack_depth`] (or
//! [`stack_is_too_deep`]) to bail out before the kernel turns a deep recursion
//! into an unrecoverable `SIGSEGV`.
//!
//! All of the file's logic is ported here directly: the absolute-distance
//! comparison against `max_stack_depth`, the `RLIM_INFINITY`/overflow folding
//! and one-time caching of the platform stack rlimit, the GUC check/assign
//! hooks for `max_stack_depth`, and the `ereport(ERROR, ...)` /
//! `GUC_check_errdetail`/`GUC_check_errhint` diagnostics.
//!
//! The C file-scope statics (`max_stack_depth`, `max_stack_depth_bytes`,
//! `stack_base_ptr`, and the cached rlimit) are per-backend globals, modelled
//! here as `thread_local`s. The two genuine OS boundaries are reading the
//! current stack pointer (the address of a function-local) and
//! `getrlimit(RLIMIT_STACK)`; both use `libc` directly, as elsewhere in this
//! tree.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use backend_utils_error::ereport;
use types_error::{PgResult, ERRCODE_STATEMENT_TOO_COMPLEX, ERROR};
use types_guc::GucSource;

/// `pg_stack_base_t` (miscadmin.h): an opaque stack reference point. C defines
/// it as `char *`; here it is the stack address as an integer (`0` == "not yet
/// set", mirroring C's `NULL`). The value is never dereferenced — only
/// subtracted from another such address — so an integer is a faithful model.
pub type pg_stack_base_t = usize;

/// `STACK_DEPTH_SLOP` (miscadmin.h): `512 * 1024`. Headroom we refuse to let
/// `max_stack_depth` consume out of the platform stack limit.
pub const STACK_DEPTH_SLOP: isize = 512 * 1024;

/// `SSIZE_MAX` (limits.h): the maximum value of the C `ssize_t` type, which
/// this port models as `isize`.
pub const SSIZE_MAX: isize = isize::MAX;

/// `WIN32_STACK_RLIMIT` (Makefile.global): `4194304` = `4 * 1024 * 1024`. Used
/// only on non-Unix platforms, where the backend stack size is fixed at link
/// time rather than discovered via `getrlimit`.
pub const WIN32_STACK_RLIMIT: isize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// File-scope statics (C globals), per backend.
// ---------------------------------------------------------------------------

thread_local! {
    /// C: `int max_stack_depth = 100;` — the GUC value, in kilobytes.
    static MAX_STACK_DEPTH: Cell<i32> = const { Cell::new(100) };

    /// C: `static ssize_t max_stack_depth_bytes = 100 * (ssize_t) 1024;` — the
    /// GUC value converted to bytes for fast checking.
    static MAX_STACK_DEPTH_BYTES: Cell<isize> = const { Cell::new(100 * 1024) };

    /// C: `static char *stack_base_ptr = NULL;` — the reference stack address,
    /// installed by [`set_stack_base`]. `0` models C's `NULL`.
    static STACK_BASE_PTR: Cell<usize> = const { Cell::new(0) };

    /// C: `static ssize_t val = 0;` inside `get_stack_depth_rlimit` — the
    /// cached platform rlimit. `0` is C's "not yet computed" sentinel (the
    /// genuine rlimit can never legitimately be `0`).
    static STACK_DEPTH_RLIMIT_CACHE: Cell<isize> = const { Cell::new(0) };
}

// ---------------------------------------------------------------------------
// Accessors for the GUC-visible values (the C variables are plain globals;
// other subsystems read them directly, so expose getters here).
// ---------------------------------------------------------------------------

/// Current `max_stack_depth` GUC value, in kilobytes (C global `max_stack_depth`).
pub fn max_stack_depth() -> i32 {
    MAX_STACK_DEPTH.with(Cell::get)
}

/// Set the `max_stack_depth` kilobyte global. Used by the GUC var accessors so
/// the GUC machinery can write the variable through `conf->variable`.
pub fn set_max_stack_depth(value: i32) {
    MAX_STACK_DEPTH.with(|c| c.set(value));
}

/// Current `max_stack_depth_bytes` (C static), in bytes.
pub fn max_stack_depth_bytes() -> isize {
    MAX_STACK_DEPTH_BYTES.with(Cell::get)
}

// ---------------------------------------------------------------------------
// Reading the current stack pointer.
// ---------------------------------------------------------------------------

/// The address of a function-local, the same value C reads via
/// `__builtin_frame_address(0)` / `&stack_base`. `#[inline(never)]` keeps the
/// frame real so the address is meaningful.
#[inline(never)]
fn current_stack_addr() -> usize {
    let stack_loc: u8 = 0;
    // The address of a stack local; never dereferenced through this pointer,
    // only used for the distance computation, exactly as in C.
    &stack_loc as *const u8 as usize
}

// ---------------------------------------------------------------------------
// set_stack_base / restore_stack_base
// ---------------------------------------------------------------------------

/// `set_stack_base`: set up the reference point for stack depth checking.
/// Should be called from `main()`. Returns the old reference point, if any.
pub fn set_stack_base() -> pg_stack_base_t {
    // old = stack_base_ptr;  stack_base_ptr = <frame address>;  return old;
    let addr = current_stack_addr();
    STACK_BASE_PTR.with(|c| c.replace(addr))
}

/// `restore_stack_base`: restore the reference point previously returned by
/// [`set_stack_base`]. (Used by PL/Java when calling a backend function from a
/// different thread, whose stack lives at a different address.)
pub fn restore_stack_base(base: pg_stack_base_t) {
    STACK_BASE_PTR.with(|c| c.set(base));
}

// ---------------------------------------------------------------------------
// check_stack_depth / stack_is_too_deep
// ---------------------------------------------------------------------------

/// `check_stack_depth`: check for excessively deep recursion and throw summarily
/// if the stack is too deep.
///
/// C `ereport`s `ERROR`; the spine returns the error as `Err(PgError)` so
/// callers thread it up the stack.
pub fn check_stack_depth() -> PgResult<()> {
    if stack_is_too_deep() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_STATEMENT_TOO_COMPLEX)
            .errmsg("stack depth limit exceeded")
            .errhint(format!(
                "Increase the configuration parameter \"max_stack_depth\" (currently {}kB), \
                 after ensuring the platform's stack depth limit is adequate.",
                max_stack_depth()
            ))
            .into_error());
    }
    Ok(())
}

/// `stack_is_too_deep`: the non-throwing predicate behind [`check_stack_depth`].
///
/// Computes the distance from the reference point to a current local, takes its
/// absolute value (stacks grow up on some machines, down on others), and reports
/// trouble when it exceeds `max_stack_depth_bytes` — but only once
/// `stack_base_ptr` has been set (the `!= NULL` guard, kept last to match C and
/// avoid wasting cycles in the common case).
pub fn stack_is_too_deep() -> bool {
    let stack_base_ptr = STACK_BASE_PTR.with(Cell::get);

    // Compute distance from reference point to my local variables; abs value
    // since stacks grow up on some machines, down on others.
    let stack_top_loc = current_stack_addr();
    let stack_depth = stack_base_ptr.abs_diff(stack_top_loc) as isize;

    // Trouble? The test on stack_base_ptr (!= NULL) prevents erroring out if
    // called before set_stack_base; logically first, placed last for speed.
    stack_depth > MAX_STACK_DEPTH_BYTES.with(Cell::get) && stack_base_ptr != 0
}

// ---------------------------------------------------------------------------
// GUC hooks for max_stack_depth
// ---------------------------------------------------------------------------

/// `check_max_stack_depth`: GUC check hook for `max_stack_depth`. Rejects values
/// that, once converted to bytes, would leave less than `STACK_DEPTH_SLOP` of
/// headroom under the platform stack limit.
///
/// C signature is `bool check_max_stack_depth(int *newval, void **extra,
/// GucSource source)`; the C hook neither mutates `*newval` nor uses
/// `*extra`/`source`. Reports failure detail/hint through the GUC check-error
/// channel (seams) and returns `false` (the C `return false` rejection).
pub fn check_max_stack_depth(newval: i32, _source: GucSource) -> bool {
    let newval_bytes = (newval as isize).saturating_mul(1024);
    let stack_rlimit = get_stack_depth_rlimit();

    if stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP {
        backend_utils_misc_guc_seams::guc_check_errdetail::call(format!(
            "\"max_stack_depth\" must not exceed {}kB.",
            (stack_rlimit - STACK_DEPTH_SLOP) / 1024
        ));
        backend_utils_misc_guc_seams::guc_check_errhint::call(
            "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent."
                .to_string(),
        );
        return false;
    }
    true
}

/// `assign_max_stack_depth`: GUC assign hook for `max_stack_depth`. Records the
/// new byte conversion.
///
/// C signature is `void assign_max_stack_depth(int newval, void *extra)`; there
/// is no `extra`, and the function only writes `max_stack_depth_bytes` (the
/// kilobyte global is written by the GUC machinery itself).
pub fn assign_max_stack_depth(newval: i32) {
    let newval_bytes = (newval as isize).saturating_mul(1024);
    MAX_STACK_DEPTH_BYTES.with(|c| c.set(newval_bytes));
}

// ---------------------------------------------------------------------------
// get_stack_depth_rlimit
// ---------------------------------------------------------------------------

/// `get_stack_depth_rlimit`: obtain the platform stack depth limit, in bytes.
/// Returns `-1` if unknown. The result is cached after the first call (the
/// limit cannot change after process launch). `ssize_t` is modelled as `isize`.
///
/// Folds `getrlimit(RLIMIT_STACK)` exactly as C does: failure -> `-1`,
/// `RLIM_INFINITY` or overflow past `SSIZE_MAX` -> `SSIZE_MAX`, otherwise the
/// finite soft limit.
pub fn get_stack_depth_rlimit() -> isize {
    // C: `static ssize_t val = 0; if (val == 0) { ... } return val;`
    let cached = STACK_DEPTH_RLIMIT_CACHE.with(Cell::get);
    if cached != 0 {
        return cached;
    }

    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes into the provided rlimit struct.
    let val = if unsafe { libc::getrlimit(libc::RLIMIT_STACK, &mut rlim) } < 0 {
        // getrlimit(...) < 0
        -1
    } else if rlim.rlim_cur == libc::RLIM_INFINITY {
        // rlim.rlim_cur == RLIM_INFINITY
        SSIZE_MAX
    } else if rlim.rlim_cur >= SSIZE_MAX as libc::rlim_t {
        // rlim_cur is probably of an unsigned type, so check for overflow
        SSIZE_MAX
    } else {
        rlim.rlim_cur as isize
    };

    STACK_DEPTH_RLIMIT_CACHE.with(|c| c.set(val));
    val
}

// ---------------------------------------------------------------------------
// Seam install
// ---------------------------------------------------------------------------

/// Install this crate's inward seams and the `max_stack_depth` GUC hooks/var.
pub fn init_seams() {
    backend_utils_misc_stack_depth_seams::check_stack_depth::set(check_stack_depth);

    backend_utils_misc_guc_tables::hooks::check_max_stack_depth
        .install(|newval, _extra, source| Ok(check_max_stack_depth(*newval, source)));
    backend_utils_misc_guc_tables::hooks::assign_max_stack_depth
        .install(|newval, _extra| assign_max_stack_depth(newval));
    backend_utils_misc_guc_tables::vars::max_stack_depth.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: max_stack_depth,
            set: set_max_stack_depth,
        },
    );
}

#[cfg(test)]
mod tests;
