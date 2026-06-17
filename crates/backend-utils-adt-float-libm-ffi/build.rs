//! Link the system math library that PostgreSQL links (`-lm`).
//!
//! On macOS the math symbols (`erf`/`erfc`/`tgamma`/`lgamma`) live in
//! `libSystem`, which Rust links automatically, so no directive is needed
//! (a bare `-lm` is a no-op there). On Linux glibc the symbols live in a
//! separate `libm.so`; emit the link directive so the `extern "C"` block
//! resolves. This binds the SAME C library PostgreSQL binds — never a
//! pure-Rust reimplementation.

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    // macOS: math is in libSystem (linked implicitly); nothing to do.
    // Linux/other glibc/musl: link the standalone math library.
    if target_os != "macos" && target_os != "ios" {
        println!("cargo:rustc-link-lib=m");
    }
}
