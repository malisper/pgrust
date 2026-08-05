//! Env-gated TRACE facility (see README.md). Without the `enabled` feature
//! every trace site compiles to nothing; with it, a disabled category costs
//! one relaxed atomic load per site.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

pub const COMPILED: bool = cfg!(feature = "enabled");

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

pub const N_CATEGORIES: usize = 12;

impl Category {
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

#[allow(clippy::declare_interior_mutable_const)]
const FALSE: AtomicBool = AtomicBool::new(false);
static ENABLED: [AtomicBool; N_CATEGORIES] = [FALSE; N_CATEGORIES];
static BT_ENABLED: [AtomicBool; N_CATEGORIES] = [FALSE; N_CATEGORIES];
static INIT: Once = Once::new();

fn parse_value_into(var: &str, value: &str, flags: &[AtomicBool; N_CATEGORIES]) {
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
            "trace: {} has unknown categories: {} (known: {})",
            var,
            unknown.join(", "),
            ALL.iter().map(|c| c.name()).collect::<Vec<_>>().join(", "),
        );
    }
}

fn parse_env_into(var: &str, flags: &[AtomicBool; N_CATEGORIES]) {
    if let Ok(value) = std::env::var(var) {
        parse_value_into(var, &value, flags);
    }
}

#[inline]
fn ensure_init() {
    INIT.call_once(|| {
        parse_env_into("PGRUST_TRACE", &ENABLED);
        parse_env_into("PGRUST_TRACE_BT", &BT_ENABLED);
    });
}

#[inline]
pub fn enabled(c: Category) -> bool {
    if !COMPILED {
        return false;
    }
    ensure_init();
    ENABLED[c.index()].load(Ordering::Relaxed)
}

#[inline]
pub fn bt_enabled(c: Category) -> bool {
    if !COMPILED {
        return false;
    }
    ensure_init();
    BT_ENABLED[c.index()].load(Ordering::Relaxed)
}

thread_local! {
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct ScopeGuard {
    category: Category,
    label: String,
}

impl ScopeGuard {
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

#[macro_export]
macro_rules! trace_enabled {
    ($cat:expr) => {
        $crate::enabled($cat)
    };
}

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

    fn fresh() -> [AtomicBool; N_CATEGORIES] {
        [FALSE; N_CATEGORIES]
    }

    fn is_set(flags: &[AtomicBool; N_CATEGORIES], c: Category) -> bool {
        flags[c.index()].load(Ordering::Relaxed)
    }

    #[test]
    fn parse_named_categories() {
        let flags = fresh();
        parse_value_into("PGRUST_TRACE", "seam, heaptuple ,exec", &flags);
        assert!(is_set(&flags, Category::Seam));
        assert!(is_set(&flags, Category::Heaptuple));
        assert!(is_set(&flags, Category::Exec));
        assert!(!is_set(&flags, Category::Slot));
        assert!(!is_set(&flags, Category::Mcx));
    }

    #[test]
    fn parse_all_and_star() {
        let flags = fresh();
        parse_value_into("PGRUST_TRACE", "all", &flags);
        for c in ALL {
            assert!(is_set(&flags, *c));
        }
        let flags2 = fresh();
        parse_value_into("PGRUST_TRACE", "*", &flags2);
        for c in ALL {
            assert!(is_set(&flags2, *c));
        }
    }

    #[test]
    fn parse_case_insensitive_and_unknown_skipped() {
        let flags = fresh();
        parse_value_into("PGRUST_TRACE", "SEAM,bogus,Slot", &flags);
        assert!(is_set(&flags, Category::Seam));
        assert!(is_set(&flags, Category::Slot));
        assert!(!is_set(&flags, Category::Exec));
    }

    #[test]
    fn category_names_unique_and_indexed() {
        for (i, c) in ALL.iter().enumerate() {
            assert_eq!(c.index(), i, "ALL must be in index order");
        }
        assert_eq!(ALL.len(), N_CATEGORIES);
    }

    #[test]
    fn macros_expand_and_are_off_by_default() {
        if false {
            trace!(Category::Bufmgr, "{}", 1);
            trace_bt!(Category::Bufmgr, "{}", 2);
        }
        let g = trace_scope!(Category::Bufmgr, "noop {}", 3);
        assert!(g.is_none());
        let _b: bool = trace_enabled!(Category::Bufmgr);
    }

    #[test]
    fn enabled_does_not_panic() {
        let _ = enabled(Category::Seam);
        let _ = bt_enabled(Category::Seam);
    }
}
