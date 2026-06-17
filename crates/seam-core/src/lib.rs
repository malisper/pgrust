//! The `seam!` macro: cycle-breaking function slots.
//!
//! A seam exists only where a direct cargo dependency would create a cycle.
//! Seam declarations for crate X's functions live in the `X-seams` crate;
//! crate X installs every one of them from its `init_seams()`, and the
//! `seams-init` crate calls each crate's `init_seams()` once at startup.
//!
//! Calling an uninstalled seam panics with the seam's full path. Installing
//! a seam twice panics. There is no silent fallback.

/// Trace hooks used by the `seam!` macro expansion. They compile to no-ops
/// unless the `trace-seams` feature is enabled.
#[doc(hidden)]
pub mod __trace {
    #[cfg(feature = "trace-seams")]
    #[inline]
    pub fn seam_hit(path: &str) {
        pgrust_trace::trace!(pgrust_trace::Category::Seam, "{}", path);
    }

    #[cfg(not(feature = "trace-seams"))]
    #[inline]
    pub fn seam_hit(_path: &str) {}

    #[cfg(feature = "trace-seams")]
    #[inline]
    pub fn seam_miss(path: &str) {
        pgrust_trace::trace_bt!(pgrust_trace::Category::Seam, "MISS {}", path);
    }

    #[cfg(not(feature = "trace-seams"))]
    #[inline]
    pub fn seam_miss(_path: &str) {}
}

/// Declare one seam.
///
/// ```ignore
/// seam_core::seam!(pub fn vacuum_rel(relid: types_core::Oid) -> types_error::PgResult<()>);
/// ```
///
/// expands to a module `vacuum_rel` with:
/// - `set(f)`   — install the implementation (owner crate only, exactly once)
/// - `call(..)` — invoke it; panics loudly if not installed
/// - `is_installed()`
///
/// A signature may be generic over lifetimes (and only lifetimes): declare
/// them after the function name and the stored slot becomes a higher-ranked
/// `for<'a, ...> fn(...)` pointer, so installed implementations must work for
/// every lifetime — the shape needed once argument/return types carry an
/// allocator lifetime (e.g. `mcx::Mcx<'mcx>` in, `PgVec<'mcx, u8>` out):
///
/// ```ignore
/// seam_core::seam!(
///     pub fn flatten<'mcx>(mcx: mcx::Mcx<'mcx>, src: &[u8]) -> PgResult<mcx::PgVec<'mcx, u8>>
/// );
/// ```
///
/// Elided lifetimes (`&T`, `Foo<'_>`) in parameter position are likewise
/// higher-ranked, as in any `fn` pointer type.
#[macro_export]
macro_rules! seam {
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident $(<$($lt:lifetime),+ $(,)?>)? ( $($arg:ident : $arg_ty:ty),* $(,)? ) $(-> $ret:ty)?
    ) => {
        $(#[$attr])*
        $vis mod $name {
            #![allow(dead_code, unused_imports)]
            use super::*;

            pub type Signature = $(for<$($lt),+>)? fn($($arg_ty),*) $(-> $ret)?;

            static SLOT: ::std::sync::OnceLock<Signature> = ::std::sync::OnceLock::new();

            /// Install the implementation. Only the owning crate calls this,
            /// from its `init_seams()`.
            pub fn set(implementation: Signature) {
                if SLOT.set(implementation).is_err() {
                    panic!(concat!("seam installed twice: ", module_path!()));
                }
            }

            pub fn is_installed() -> bool {
                SLOT.get().is_some()
            }

            pub fn call $(<$($lt),+>)? ($($arg: $arg_ty),*) $(-> $ret)? {
                match SLOT.get() {
                    Some(f) => {
                        $crate::__trace::seam_hit(module_path!());
                        f($($arg),*)
                    }
                    None => {
                        // Show the full caller stack before the loud panic so a
                        // missing install is debuggable in one run.
                        $crate::__trace::seam_miss(module_path!());
                        panic!(concat!("seam not installed: ", module_path!()))
                    }
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    crate::seam!(pub fn double(x: i32) -> i32);
    crate::seam!(pub fn never_installed(x: i32) -> i32);
    crate::seam!(pub fn first<'a>(xs: &'a [i32]) -> &'a i32);

    #[test]
    fn install_and_call() {
        double::set(|x| x * 2);
        assert!(double::is_installed());
        assert_eq!(double::call(21), 42);
    }

    #[test]
    #[should_panic(expected = "seam not installed")]
    fn uninstalled_call_panics() {
        never_installed::call(1);
    }

    #[test]
    fn lifetime_generic_seam_returns_borrow() {
        first::set(|xs| &xs[0]);
        let data = vec![7, 8, 9];
        assert_eq!(*first::call(&data), 7);
    }
}
