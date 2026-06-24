//! Link directives for the PAM provider (`libpam`), behind the `with-pam`
//! feature (PostgreSQL's `--with-pam` configure option, `#ifdef USE_PAM`).
//!
//! PostgreSQL links `-lpam` when `--with-pam` is given. We mirror that: emit
//! `-lpam`. On macOS `libpam` lives in the SDK (`/usr/lib/libpam.dylib`, found
//! automatically via the SDK link path), on Linux it is the system `libpam`.
//! When the feature is OFF (`#ifdef USE_PAM` false) the provider binds nothing,
//! so no link directive is emitted.

fn main() {
    // wasm: no system PAM; the method is unavailable, so emit nothing.
    if std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("wasm") {
        return;
    }

    if std::env::var_os("CARGO_FEATURE_WITH_PAM").is_none() {
        // Feature off => no binding, no link (faithful `#ifdef USE_PAM` off).
        return;
    }

    // Same library PostgreSQL's `--with-pam` links.
    println!("cargo:rustc-link-lib=pam");
}
