//! The `seam!` macro: cycle-breaking function slots.
//!
//! A seam exists only where a direct cargo dependency would create a cycle.
//! Seam declarations for crate X's functions live in the `X-seams` crate;
//! crate X installs every one of them from its `init_seams()`, and the
//! `seams-init` crate calls each crate's `init_seams()` once at startup.
//!
//! Calling an uninstalled seam panics with the seam's full path. Installing
//! a seam twice panics. There is no silent fallback.

/// Declare one seam.
///
/// ```ignore
/// seam_core::seam!(pub fn vacuum_rel(relid: types::Oid) -> types::PgResult<()>);
/// ```
///
/// expands to a module `vacuum_rel` with:
/// - `set(f)`   — install the implementation (owner crate only, exactly once)
/// - `call(..)` — invoke it; panics loudly if not installed
/// - `is_installed()`
#[macro_export]
macro_rules! seam {
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident ( $($arg:ident : $arg_ty:ty),* $(,)? ) $(-> $ret:ty)?
    ) => {
        $(#[$attr])*
        $vis mod $name {
            #![allow(dead_code, unused_imports)]
            use super::*;

            pub type Signature = fn($($arg_ty),*) $(-> $ret)?;

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

            pub fn call($($arg: $arg_ty),*) $(-> $ret)? {
                match SLOT.get() {
                    Some(f) => f($($arg),*),
                    None => panic!(concat!("seam not installed: ", module_path!())),
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    crate::seam!(pub fn double(x: i32) -> i32);
    crate::seam!(pub fn never_installed(x: i32) -> i32);

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
}
