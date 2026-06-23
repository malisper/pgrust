//! Link directives for the OpenSSL provider (libssl + libcrypto), behind the
//! `ssl-openssl` feature (PostgreSQL's `--with-ssl=openssl`).
//!
//! macOS ships no public system OpenSSL development headers (Apple removed them
//! years ago; `/usr/lib/libcrypto.dylib` is a private Apple build that rejects
//! direct linking). We therefore link the Homebrew keg-only `openssl@3`. On
//! Linux the system OpenSSL is used directly (no extra search path needed,
//! unless `OPENSSL_DIR` is set).

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=OPENSSL_DIR");
    println!("cargo:rerun-if-env-changed=OPENSSL_LIB_DIR");

    if std::env::var_os("CARGO_FEATURE_SSL_OPENSSL").is_none() {
        // Feature off ⇒ no binding, no link (faithful `#ifdef USE_SSL` off).
        return;
    }

    // Resolve the OpenSSL library directory.
    //   1. OPENSSL_LIB_DIR (explicit).
    //   2. OPENSSL_DIR/lib (openssl-sys convention).
    //   3. `brew --prefix openssl@3`/lib (macOS keg-only default).
    let lib_dir = std::env::var("OPENSSL_LIB_DIR")
        .ok()
        .or_else(|| std::env::var("OPENSSL_DIR").ok().map(|d| format!("{d}/lib")))
        .or_else(brew_openssl_lib_dir);

    if let Some(dir) = lib_dir {
        println!("cargo:rustc-link-search=native={dir}");
    }

    // Same library set PostgreSQL's `--with-ssl=openssl` links.
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=crypto");
}

/// `brew --prefix openssl@3`/lib, if Homebrew has the keg installed.
fn brew_openssl_lib_dir() -> Option<String> {
    let out = Command::new("brew")
        .args(["--prefix", "openssl@3"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let prefix = String::from_utf8(out.stdout).ok()?;
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return None;
    }
    Some(format!("{prefix}/lib"))
}
