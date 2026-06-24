//! Link directives for the libxml2 provider, behind the `with-libxml` feature
//! (PostgreSQL's `--with-libxml` configure option).
//!
//! PostgreSQL discovers libxml2 via `xml2-config`/`pkg-config` at configure
//! time and links `-lxml2`. We mirror that: resolve the library search path
//! through `pkg-config` (module `libxml-2.0`) when available, falling back to
//! `xml2-config --libs`, then emit `-lxml2`. The `extern "C"` block in
//! `provider.rs` binds the SAME system libxml2 PostgreSQL binds — never a
//! pure-Rust reimplementation.
//!
//! When the `with-libxml` feature is OFF (`#ifdef USE_LIBXML` false) the
//! provider binds nothing, so no link directive is emitted.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LIBXML2_LIB_DIR");

    // wasm: no system libxml2 to link (single-user XML paths are inert / the
    // feature is off). Emit no link directive so the wasm linker doesn't seek
    // `-lxml2`.
    if std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("wasm") {
        return;
    }

    if std::env::var_os("CARGO_FEATURE_WITH_LIBXML").is_none() {
        // Feature off => no binding, no link (faithful `#ifdef USE_LIBXML` off).
        return;
    }

    // Resolve the libxml2 library directory.
    //   1. LIBXML2_LIB_DIR (explicit override).
    //   2. pkg-config --libs-only-L libxml-2.0 (only if it yields a dir).
    //   3. xml2-config --libs (PostgreSQL's own discovery tool; the macOS SDK
    //      libxml2 only shows up via the SDK -L it reports, since the system
    //      .dylib lives under the CommandLineTools/Xcode SDK, not /usr/lib).
    let dirs = std::env::var("LIBXML2_LIB_DIR")
        .ok()
        .map(|d| vec![d])
        .filter(|v: &Vec<String>| !v.is_empty())
        .or_else(|| pkg_config_lib_dirs().filter(|v| !v.is_empty()))
        .or_else(xml2_config_lib_dirs)
        .unwrap_or_default();
    for dir in dirs {
        println!("cargo:rustc-link-search=native={dir}");
    }

    // Same library PostgreSQL's `--with-libxml` links.
    println!("cargo:rustc-link-lib=xml2");
}

/// `pkg-config --libs-only-L libxml-2.0` -> the `-L` dirs, if pkg-config knows
/// the module (may be empty when the .pc file uses the default lib dir).
fn pkg_config_lib_dirs() -> Option<Vec<String>> {
    let out = Command::new("pkg-config")
        .args(["--libs-only-L", "libxml-2.0"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let dirs: Vec<String> = s
        .split_whitespace()
        .filter_map(|tok| tok.strip_prefix("-L").map(|d| d.to_string()))
        .collect();
    Some(dirs)
}

/// `xml2-config --libs` -> the `-L` dirs (PostgreSQL's own discovery tool).
fn xml2_config_lib_dirs() -> Option<Vec<String>> {
    let out = Command::new("xml2-config").arg("--libs").output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let dirs: Vec<String> = s
        .split_whitespace()
        .filter_map(|tok| tok.strip_prefix("-L").map(|d| d.to_string()))
        .collect();
    Some(dirs)
}
