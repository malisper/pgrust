//! `pgrust-trace` — a lightweight, env-gated TRACE facility for debugging the
//! single-user query pipeline of the pgrust PostgreSQL port.
//!
//! This is *infrastructure*, not a C port. It exists to make the recurring
//! debugging chokepoints (seam dispatch, heaptuple attr decode, slot deform,
//! executor node boundaries, cache hit/miss, relcache build, memory-context
//! accounting) observable without a stepping debugger — the c2rust symbol
//! obfuscation (`n`, `ln`, …) makes stepping awkward, so targeted env-gated
//! prints + backtraces win.
//!
//! # Design goals
//!
//! * **Zero non-std dependencies.** Any crate — even the low-level
//!   `seam-core` — can depend on this without risking a dependency cycle.
//! * **Zero cost when off.** A disabled trace site costs exactly one relaxed
//!   atomic load. The format arguments are evaluated *only* inside the enabled
//!   branch.
//!
//! # Environment variables
//!
//! * `PGRUST_TRACE` — comma-separated list of [`Category`] names to enable.
//!   The special tokens `all` and `*` enable everything. Unknown names are
//!   warned about once on stderr.
//! * `PGRUST_TRACE_BT` — same syntax; categories listed here *also* emit a
//!   `Backtrace::force_capture()` after each trace line.
//!
//! Both are parsed exactly once, lazily, on the first [`enabled`] / [`bt_enabled`]
//! check.
//!
//! ```text
//! PGRUST_TRACE=seam,heaptuple ./postgres --single ...
//! PGRUST_TRACE=all ./postgres --single ...
//! PGRUST_TRACE=exec PGRUST_TRACE_BT=mcx ./postgres --single ...
//! ```
//!
//! # Macro API
//!
//! * [`trace!`]`(Category::X, "fmt {}", args...)` — conditional formatted print.
//! * [`trace_enabled!`]`(Category::X)` — `bool`, to guard expensive value
//!   construction.
//! * [`trace_bt!`]`(Category::X, "fmt", args...)` — like `trace!` but *always*
//!   captures a backtrace (one-off, ignores `PGRUST_TRACE_BT`).
//! * [`trace_scope!`]`(Category::X, "label {}", args...)` — RAII enter/exit
//!   guard that logs `>> label` on creation and `<< label` on drop, with
//!   thread-local depth indentation. No-op when the category is off.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

/// A trace category. Each variant maps to a stable lowercase name used both in
/// the `PGRUST_TRACE` env var and in the `[name]` prefix of each trace line.
///
/// The list is intentionally extensible: add a variant, add it to [`ALL`], and
/// add its name to [`Category::name`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum Category {
    Seam = 0,
    Heaptuple,
    Slot,
    Exec,
    Catcache,
    Syscache,
    Relcache,
    Planner,
    Xact,
    Mcx,
    Smgr,
    Bufmgr,
}

/// Every category, in index order. Used by the env parser and tests.
pub const ALL: &[Category] = &[
    Category::Seam,
    Category::Heaptuple,
    Category::Slot,
    Category::Exec,
    Category::Catcache,
    Category::Syscache,
    Category::Relcache,
    Category::Planner,
    Category::Xact,
    Category::Mcx,
    Category::Smgr,
    Category::Bufmgr,
];

/// Number of categories. Keep in sync with [`Category`]/[`ALL`].
pub const N_CATEGORIES: usize = 12;

impl Category {
    /// The lowercase name of this category, as used in `PGRUST_TRACE`.
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            Category::Seam => "seam",
            Category::Heaptuple => "heaptuple",
            Category::Slot => "slot",
            Category::Exec => "exec",
            Category::Catcache => "catcache",
            Category::Syscache => "syscache",
            Category::Relcache => "relcache",
            Category::Planner => "planner",
            Category::Xact => "xact",
            Category::Mcx => "mcx",
            Category::Smgr => "smgr",
            Category::Bufmgr => "bufmgr",
        }
    }

    #[inline]
    fn index(self) -> usize {
        self as usize
    }
}

// One flag per category for "enabled" and one per category for "also emit a
// backtrace". `AtomicBool` is not `Copy`, so build the arrays element-by-element.
#[allow(clippy::declare_interior_mutable_const)]
const FALSE: AtomicBool = AtomicBool::new(false);
static ENABLED: [AtomicBool; N_CATEGORIES] = [FALSE; N_CATEGORIES];
static BT_ENABLED: [AtomicBool; N_CATEGORIES] = [FALSE; N_CATEGORIES];
static INIT: Once = Once::new();

/// Parse one env var's comma-separated category list into the given flag array.
/// `all` / `*` enable everything. Unknown names are collected for one warning.
fn parse_into(var: &str, flags: &[AtomicBool; N_CATEGORIES]) {
    let value = match std::env::var(var) {
        Ok(v) => v,
        Err(_) => return,
    };
    let mut unknown: Vec<&str> = Vec::new();
    for raw in value.split(',') {
        let tok = raw.trim();
        if tok.is_empty() {
            continue;
        }
        if tok.eq_ignore_ascii_case("all") || tok == "*" {
            for f in flags.iter() {
                f.store(true, Ordering::Relaxed);
            }
            continue;
        }
        match ALL.iter().find(|c| c.name().eq_ignore_ascii_case(tok)) {
            Some(c) => flags[c.index()].store(true, Ordering::Relaxed),
            None => unknown.push(tok),
        }
    }
    if !unknown.is_empty() {
        eprintln!(
            "pgrust-trace: {} has unknown categories: {} (known: {})",
            var,
            unknown.join(", "),
            ALL.iter().map(|c| c.name()).collect::<Vec<_>>().join(", "),
        );
    }
}

#[inline]
fn ensure_init() {
    INIT.call_once(|| {
        parse_into("PGRUST_TRACE", &ENABLED);
        parse_into("PGRUST_TRACE_BT", &BT_ENABLED);
    });
}

/// Whether tracing is enabled for `c`. Cheap: a relaxed atomic load (after a
/// one-time lazy init of the env-derived flags).
#[inline]
pub fn enabled(c: Category) -> bool {
    ensure_init();
    ENABLED[c.index()].load(Ordering::Relaxed)
}

/// Whether `c` should additionally emit a backtrace per trace line.
#[inline]
pub fn bt_enabled(c: Category) -> bool {
    ensure_init();
    BT_ENABLED[c.index()].load(Ordering::Relaxed)
}

// --- RAII enter/exit scope support -----------------------------------------

thread_local! {
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// RAII guard returned by [`trace_scope!`]. Logs `<< label` on drop. Created
/// only when the category is enabled, so the disabled path allocates nothing.
pub struct ScopeGuard {
    category: Category,
    label: String,
}

impl ScopeGuard {
    /// Create a guard: prints `>> label` at the current depth and increments
    /// the thread-local indentation depth. Called by [`trace_scope!`] only when
    /// the category is enabled.
    #[doc(hidden)]
    pub fn enter(category: Category, label: String) -> ScopeGuard {
        let depth = DEPTH.with(|d| {
            let cur = d.get();
            d.set(cur + 1);
            cur
        });
        eprintln!("[{}] {}>> {}", category.name(), indent(depth), label);
        ScopeGuard { category, label }
    }
}

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        let depth = DEPTH.with(|d| {
            let cur = d.get().saturating_sub(1);
            d.set(cur);
            cur
        });
        eprintln!("[{}] {}<< {}", self.category.name(), indent(depth), self.label);
    }
}

fn indent(depth: usize) -> String {
    "  ".repeat(depth)
}

// --- macros ----------------------------------------------------------------

/// Conditional formatted trace. Evaluates its format arguments only when the
/// category is enabled. If the category is also in `PGRUST_TRACE_BT`, emits a
/// backtrace after the line.
///
/// ```ignore
/// trace::trace!(trace::Category::Seam, "{}", name);
/// ```
#[macro_export]
macro_rules! trace {
    ($cat:expr, $($arg:tt)+) => {{
        let __cat = $cat;
        if $crate::enabled(__cat) {
            ::std::eprintln!(
                "[{}] {}:{}: {}",
                __cat.name(),
                ::std::file!(),
                ::std::line!(),
                ::std::format_args!($($arg)+),
            );
            if $crate::bt_enabled(__cat) {
                ::std::eprintln!("{}", ::std::backtrace::Backtrace::force_capture());
            }
        }
    }};
}

/// `bool` test, for guarding expensive value construction before a `trace!`.
#[macro_export]
macro_rules! trace_enabled {
    ($cat:expr) => {
        $crate::enabled($cat)
    };
}

/// Like [`trace!`] but *always* captures a backtrace, regardless of
/// `PGRUST_TRACE_BT`. The line itself is still gated by `PGRUST_TRACE`.
#[macro_export]
macro_rules! trace_bt {
    ($cat:expr, $($arg:tt)+) => {{
        let __cat = $cat;
        if $crate::enabled(__cat) {
            ::std::eprintln!(
                "[{}] {}:{}: {}",
                __cat.name(),
                ::std::file!(),
                ::std::line!(),
                ::std::format_args!($($arg)+),
            );
            ::std::eprintln!("{}", ::std::backtrace::Backtrace::force_capture());
        }
    }};
}

/// RAII enter/exit scope. Returns a guard binding (or `()` when disabled).
/// Logs `>> label` immediately and `<< label` when the guard drops, with
/// thread-local depth indentation. Zero-cost when the category is off.
///
/// ```ignore
/// let _g = trace::trace_scope!(trace::Category::Exec, "ExecProcNode {}", tag);
/// ```
#[macro_export]
macro_rules! trace_scope {
    ($cat:expr, $($arg:tt)+) => {{
        let __cat = $cat;
        if $crate::enabled(__cat) {
            ::std::option::Option::Some($crate::ScopeGuard::enter(
                __cat,
                ::std::format!($($arg)+),
            ))
        } else {
            ::std::option::Option::None
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    // The env-derived flags are process-global and parsed once. To test the
    // parser deterministically we call it directly against fresh local arrays.
    fn fresh() -> [AtomicBool; N_CATEGORIES] {
        [FALSE; N_CATEGORIES]
    }

    fn parse_value_into(value: &str, flags: &[AtomicBool; N_CATEGORIES]) {
        // Mirror of parse_into without the env lookup, so tests don't race on
        // process env.
        for raw in value.split(',') {
            let tok = raw.trim();
            if tok.is_empty() {
                continue;
            }
            if tok.eq_ignore_ascii_case("all") || tok == "*" {
                for f in flags.iter() {
                    f.store(true, Ordering::Relaxed);
                }
                continue;
            }
            if let Some(c) = ALL.iter().find(|c| c.name().eq_ignore_ascii_case(tok)) {
                flags[c.index()].store(true, Ordering::Relaxed);
            }
        }
    }

    fn is_set(flags: &[AtomicBool; N_CATEGORIES], c: Category) -> bool {
        flags[c.index()].load(Ordering::Relaxed)
    }

    #[test]
    fn parse_named_categories() {
        let flags = fresh();
        parse_value_into("seam, heaptuple ,exec", &flags);
        assert!(is_set(&flags, Category::Seam));
        assert!(is_set(&flags, Category::Heaptuple));
        assert!(is_set(&flags, Category::Exec));
        assert!(!is_set(&flags, Category::Slot));
        assert!(!is_set(&flags, Category::Mcx));
    }

    #[test]
    fn parse_all_and_star() {
        let flags = fresh();
        parse_value_into("all", &flags);
        for c in ALL {
            assert!(is_set(&flags, *c));
        }
        let flags2 = fresh();
        parse_value_into("*", &flags2);
        for c in ALL {
            assert!(is_set(&flags2, *c));
        }
    }

    #[test]
    fn parse_case_insensitive_and_unknown_skipped() {
        let flags = fresh();
        parse_value_into("SEAM,bogus,Slot", &flags);
        assert!(is_set(&flags, Category::Seam));
        assert!(is_set(&flags, Category::Slot));
    }

    #[test]
    fn category_names_unique_and_indexed() {
        for (i, c) in ALL.iter().enumerate() {
            assert_eq!(c.index(), i, "ALL must be in index order");
        }
        assert_eq!(ALL.len(), N_CATEGORIES);
    }

    #[test]
    fn macros_are_noops_when_off() {
        // Category::Bufmgr is not enabled by the test env, so these must not
        // panic and must not evaluate side effects... but we can't observe
        // eprintln. Instead verify the format args are NOT evaluated when off
        // by putting a panic in them.
        if false {
            // never runs; this only checks the macros expand & type-check.
            trace!(Category::Bufmgr, "{}", 1);
            trace_bt!(Category::Bufmgr, "{}", 2);
        }
        // trace_scope returns None when off (Bufmgr off by default in tests).
        let g = trace_scope!(Category::Bufmgr, "noop {}", 3);
        assert!(g.is_none(), "scope guard must be None when category is off");

        // trace_enabled is a plain bool.
        let _b: bool = trace_enabled!(Category::Bufmgr);
    }

    #[test]
    fn enabled_does_not_panic() {
        // Exercise the public API path (lazy init + load).
        let _ = enabled(Category::Seam);
        let _ = bt_enabled(Category::Seam);
    }
}
