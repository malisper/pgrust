#![allow(non_camel_case_types)]

#[cfg(test)]
mod tests;

use std::cell::Cell;

use elog::ereport;
use types_error::{PgResult, ERRCODE_STATEMENT_TOO_COMPLEX, ERROR};
use types_guc::GucSource;

// A stack address, only ever subtracted, never dereferenced; 0 is C's NULL.
pub type pg_stack_base_t = usize;

pub const STACK_DEPTH_SLOP: isize = 512 * 1024;

thread_local! {
    static MAX_STACK_DEPTH: Cell<i32> = const { Cell::new(100) };
    static MAX_STACK_DEPTH_BYTES: Cell<isize> = const { Cell::new(100 * 1024) };
    static STACK_BASE_PTR: Cell<usize> = const { Cell::new(0) };
    // 0 is C's "not yet computed" sentinel (a real rlimit is never 0).
    static STACK_DEPTH_RLIMIT_CACHE: Cell<isize> = const { Cell::new(0) };
}

pub fn max_stack_depth() -> i32 {
    MAX_STACK_DEPTH.get()
}

pub fn set_max_stack_depth(value: i32) {
    MAX_STACK_DEPTH.set(value);
}

pub fn max_stack_depth_bytes() -> isize {
    MAX_STACK_DEPTH_BYTES.get()
}

// C's __builtin_frame_address(0); inline(never) keeps the frame real; no black_box (it spills — docs/benchmarks/stack_depth.md).
#[inline(never)]
fn current_stack_addr() -> usize {
    let stack_loc: u8 = 0;
    &raw const stack_loc as usize
}

// One backend = one thread: recorded at backend-thread spawn (C: in main()).
pub fn set_stack_base() -> pg_stack_base_t {
    let addr = current_stack_addr();
    STACK_BASE_PTR.with(|c| c.replace(addr))
}

pub fn restore_stack_base(base: pg_stack_base_t) {
    STACK_BASE_PTR.set(base);
}

// C's shape: address of an own-frame local, pointer subtraction, compare.
#[inline(never)]
pub fn stack_is_too_deep() -> bool {
    let stack_top_loc: u8 = 0;
    let stack_base_ptr = STACK_BASE_PTR.get();
    let stack_depth = stack_base_ptr.abs_diff(&raw const stack_top_loc as usize) as isize;
    // base != 0 (NULL) guard last: no wasted cycles in the normal case.
    stack_depth > MAX_STACK_DEPTH_BYTES.get() && stack_base_ptr != 0
}

#[inline]
pub fn check_stack_depth() -> PgResult<()> {
    if stack_is_too_deep() {
        return Err(stack_depth_exceeded());
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn stack_depth_exceeded() -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_STATEMENT_TOO_COMPLEX)
            .errmsg("stack depth limit exceeded")
            .errhint(format!(
                "Increase the configuration parameter \"max_stack_depth\" (currently {}kB), \
                 after ensuring the platform's stack depth limit is adequate.",
                max_stack_depth()
            ))
            .into_error(),
    )
}

// C InitializeGUCOptionsFromEnvironment's stack-rlimit branch (guc.c): the
// boot default is 100kB; a usable platform limit raises it to
// min((rlimit - slop)/1024, 2048) kB, as PGC_S_ENV_VAR so conf/argv override.
pub fn adjust_max_stack_depth_from_rlimit() -> PgResult<()> {
    let stack_rlimit = get_stack_depth_rlimit();
    if stack_rlimit > 0 {
        let mut new_limit = stack_rlimit.saturating_sub(STACK_DEPTH_SLOP) / 1024;
        if new_limit > 100 {
            new_limit = new_limit.min(2048);
            guc::SetConfigOption(
                "max_stack_depth",
                Some(&new_limit.to_string()),
                types_guc::GucContext::PGC_POSTMASTER,
                GucSource::PGC_S_ENV_VAR,
            )?;
        }
    }
    Ok(())
}

pub fn check_max_stack_depth(newval: i32, _source: GucSource) -> bool {
    let newval_bytes = newval as isize * 1024;
    let stack_rlimit = get_stack_depth_rlimit();

    if stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP {
        guc::GUC_check_errdetail(format!(
            "\"max_stack_depth\" must not exceed {}kB.",
            (stack_rlimit - STACK_DEPTH_SLOP) / 1024
        ));
        guc::GUC_check_errhint(
            "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.",
        );
        return false;
    }
    true
}

pub fn assign_max_stack_depth(newval: i32) {
    MAX_STACK_DEPTH_BYTES.set(newval as isize * 1024);
}

// Platform stack limit in bytes, -1 if unknown; cached after first call.
pub fn get_stack_depth_rlimit() -> isize {
    // Miri has no getrlimit; -1 is C's "limit unknown" (accept any value).
    // wasm32: WASI has no rlimits either — the same C no-getrlimit arm.
    #[cfg(any(miri, target_family = "wasm"))]
    return -1;
    #[cfg(not(any(miri, target_family = "wasm")))]
    {
        let cached = STACK_DEPTH_RLIMIT_CACHE.get();
        if cached != 0 {
            return cached;
        }

        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes into the provided rlimit struct.
        let val = if unsafe { libc::getrlimit(libc::RLIMIT_STACK, &mut rlim) } < 0 {
            -1
        } else if rlim.rlim_cur == libc::RLIM_INFINITY {
            isize::MAX
        } else if rlim.rlim_cur >= isize::MAX as libc::rlim_t {
            isize::MAX
        } else {
            rlim.rlim_cur as isize
        };

        STACK_DEPTH_RLIMIT_CACHE.set(val);
        val
    }
}

pub fn init_seams() {
    guc_tables::hooks::check_max_stack_depth
        .install(|newval, _extra, source| Ok(check_max_stack_depth(*newval, source)));
    guc_tables::hooks::assign_max_stack_depth
        .install(|newval, _extra| assign_max_stack_depth(newval));
    guc_tables::vars::max_stack_depth.install(guc_tables::GucVarAccessors {
        get: max_stack_depth,
        set: set_max_stack_depth,
    });
}
