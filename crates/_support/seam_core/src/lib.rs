// INVARIANT (set-once): SLOT is written only by `set()` during single-threaded
// startup, before any call, never again; it always holds a fn pointer of the
// seam's exact `Signature` — so `call()` is one relaxed load + indirect call.

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

            #[cold]
            #[inline(never)]
            fn __uninstalled_stub $(<$($lt),+>)? ($(_: $arg_ty),*) $(-> $ret)? {
                panic!(concat!("seam not installed: ", module_path!()))
            }

            #[inline(always)]
            fn __stub_ptr() -> *mut () {
                __uninstalled_stub as Signature as *mut ()
            }

            static SLOT: ::std::sync::atomic::AtomicPtr<()> =
                ::std::sync::atomic::AtomicPtr::new(__uninstalled_stub as Signature as *mut ());

            pub fn set(implementation: Signature) {
                let prev = SLOT.swap(
                    implementation as *mut (),
                    ::std::sync::atomic::Ordering::Relaxed,
                );
                if prev != __stub_ptr() {
                    panic!(concat!("seam installed twice: ", module_path!()));
                }
            }

            pub fn is_installed() -> bool {
                SLOT.load(::std::sync::atomic::Ordering::Relaxed) != __stub_ptr()
            }

            pub fn call $(<$($lt),+>)? ($($arg: $arg_ty),*) $(-> $ret)? {
                let raw = SLOT.load(::std::sync::atomic::Ordering::Relaxed);
                // SAFETY: set-once invariant (crate top) — always a valid `Signature`.
                let f: Signature = unsafe { ::std::mem::transmute::<*mut (), Signature>(raw) };
                f($($arg),*)
            }
        }
    };
}

// Link-time seam: direct `extern "Rust"` symbol; fat-LTO inlines the body.
// PRECONDITION: exactly one impl, always linked; test-mocked seams stay `seam!`.
#[macro_export]
macro_rules! seam_linktime {
    (
        link_name = $sym:literal;
        $(#[$attr:meta])*
        $vis:vis fn $name:ident $(<$($lt:lifetime),+ $(,)?>)? ( $($arg:ident : $arg_ty:ty),* $(,)? ) $(-> $ret:ty)?
    ) => {
        $(#[$attr])*
        $vis mod $name {
            #![allow(dead_code, unused_imports)]
            use super::*;

            pub type Signature = $(for<$($lt),+>)? fn($($arg_ty),*) $(-> $ret)?;

            pub const LINK_NAME: &str = $sym;

            extern "Rust" {
                #[link_name = $sym]
                fn __seam_impl $(<$($lt),+>)? ($($arg : $arg_ty),*) $(-> $ret)?;
            }

            #[inline(always)]
            pub fn call $(<$($lt),+>)? ($($arg : $arg_ty),*) $(-> $ret)? {
                // SAFETY: defined once by `seam_linktime_impl!`, const-checked against `Signature`.
                unsafe { __seam_impl($($arg),*) }
            }
        }
    };
}

#[macro_export]
macro_rules! seam_linktime_impl {
    (
        export_fn = $export:ident;
        link_name = $sym:literal;
        signature = $sigpath:path;
        impl = $imp:expr;
        fn $(<$($lt:lifetime),+ $(,)?>)? ( $($arg:ident : $arg_ty:ty),* $(,)? ) $(-> $ret:ty)?
    ) => {
        // Pins the impl to the consumer-facing Signature (no ABI drift).
        const _: () = {
            #[allow(unused)]
            const __SEAM_SIG_CHECK: $sigpath = $imp;
        };

        #[export_name = $sym]
        pub extern "Rust" fn $export $(<$($lt),+>)? ($($arg : $arg_ty),*) $(-> $ret)? {
            ($imp)($($arg),*)
        }
    };
}

#[cfg(test)]
mod tests {
    crate::seam!(pub fn double(x: i32) -> i32);
    crate::seam!(pub fn never_installed(x: i32) -> i32);
    crate::seam!(pub fn installed_twice(x: i32) -> i32);
    crate::seam!(pub fn first<'a>(xs: &'a [i32]) -> &'a i32);

    #[test]
    fn install_and_call() {
        assert!(!double::is_installed());
        double::set(|x| x * 2);
        assert!(double::is_installed());
        assert_eq!(double::call(21), 42);
    }

    #[test]
    #[should_panic(expected = "seam not installed: seam_core::tests::never_installed")]
    fn uninstalled_call_panics_with_path() {
        never_installed::call(1);
    }

    #[test]
    #[should_panic(expected = "seam installed twice: seam_core::tests::installed_twice")]
    fn double_install_panics_with_path() {
        installed_twice::set(|x| x);
        installed_twice::set(|x| x + 1);
    }

    #[test]
    fn lifetime_generic_seam_returns_borrow() {
        first::set(|xs| &xs[0]);
        let data = [7, 8, 9];
        assert_eq!(*first::call(&data), 7);
    }

    crate::seam_linktime!(
        link_name = "__pgrust_seam_test_triple";
        pub fn triple(x: i32) -> i32
    );
    crate::seam_linktime!(
        link_name = "__pgrust_seam_test_first_lt";
        pub fn first_lt<'a>(xs: &'a [i32]) -> &'a i32
    );

    fn triple_impl(x: i32) -> i32 {
        x * 3
    }
    crate::seam_linktime_impl!(
        export_fn = __seam_export_triple;
        link_name = "__pgrust_seam_test_triple";
        signature = triple::Signature;
        impl = triple_impl;
        fn (x: i32) -> i32
    );

    fn first_lt_impl(xs: &[i32]) -> &i32 {
        &xs[0]
    }
    crate::seam_linktime_impl!(
        export_fn = __seam_export_first_lt;
        link_name = "__pgrust_seam_test_first_lt";
        signature = first_lt::Signature;
        impl = first_lt_impl;
        fn <'a>(xs: &'a [i32]) -> &'a i32
    );

    #[test]
    fn linktime_seam_calls_owner_export() {
        assert_eq!(triple::call(14), 42);
    }

    #[test]
    fn linktime_lifetime_seam_returns_borrow() {
        let data = [11, 22, 33];
        assert_eq!(*first_lt::call(&data), 11);
    }
}
