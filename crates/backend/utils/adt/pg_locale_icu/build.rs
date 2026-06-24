//! Link directives + ICU version-suffix discovery for the ICU collation
//! provider, behind the `with-icu` feature (PostgreSQL's `--with-icu`).
//!
//! PostgreSQL discovers ICU via `pkg-config` (`icu-uc`, `icu-i18n`) at configure
//! time and links `-licuuc -licui18n -licudata`. We mirror that: resolve the
//! library search path through `pkg-config`, falling back to a Homebrew prefix,
//! then emit the link directives.
//!
//! ICU renames every exported symbol with a major-version suffix (e.g.
//! `ucol_open_78`) unless built with `U_DISABLE_RENAMING`; the C headers paper
//! over this with the `U_ICU_ENTRY_POINT_RENAME` macro. Rust `#[link_name]`
//! needs a string literal, so we detect the major version here and emit it as
//! `cargo:rustc-env=PG_ICU_VERSION_MAJOR`; `ffi.rs` builds the versioned symbol
//! names with `concat!`.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=ICU_LIB_DIR");
    println!("cargo:rerun-if-env-changed=ICU_VERSION_MAJOR");

    if std::env::var_os("CARGO_FEATURE_WITH_ICU").is_none() {
        // Feature off => no binding, no link (faithful `#ifdef USE_ICU` off).
        // ffi.rs still needs a value for the `concat!`, but it is never linked.
        println!("cargo:rustc-env=PG_ICU_VERSION_MAJOR=0");
        return;
    }

    // Resolve the library directory + include prefix.
    let prefix = std::env::var("ICU_LIB_DIR")
        .ok()
        .or_else(pkg_config_libdir)
        .or_else(homebrew_libdir);

    if let Some(dir) = &prefix {
        println!("cargo:rustc-link-search=native={dir}");
        // Homebrew installs unversioned-suffix dylibs in the keg lib dir; make
        // sure the dynamic loader can find them at run time too.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }

    // Same libraries PostgreSQL's `--with-icu` links.
    println!("cargo:rustc-link-lib=icui18n");
    println!("cargo:rustc-link-lib=icuuc");
    println!("cargo:rustc-link-lib=icudata");

    let major = detect_version_major().unwrap_or(0);
    println!("cargo:rustc-env=PG_ICU_VERSION_MAJOR={major}");
}

/// `pkg-config --variable=libdir icu-uc` -> the ICU library directory.
fn pkg_config_libdir() -> Option<String> {
    with_homebrew_pkgconfig(|| {
        let out = Command::new("pkg-config")
            .args(["--variable=libdir", "icu-uc"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let s = String::from_utf8(out.stdout).ok()?;
        let dir = s.trim();
        (!dir.is_empty()).then(|| dir.to_string())
    })
}

/// Homebrew keg fallback (`/opt/homebrew/opt/icu4c/lib`, `/usr/local/...`).
fn homebrew_libdir() -> Option<String> {
    for base in ["/opt/homebrew/opt/icu4c", "/usr/local/opt/icu4c"] {
        let lib = format!("{base}/lib");
        if std::path::Path::new(&lib).is_dir() {
            return Some(lib);
        }
    }
    None
}

/// `pkg-config --modversion icu-uc` -> the major component (e.g. `78` from
/// `78.3`).
fn detect_version_major() -> Option<u32> {
    with_homebrew_pkgconfig(|| {
        let out = Command::new("pkg-config")
            .args(["--modversion", "icu-uc"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let s = String::from_utf8(out.stdout).ok()?;
        s.trim().split('.').next()?.parse::<u32>().ok()
    })
    .or_else(|| {
        // Fall back to scanning the Homebrew keg's versioned dylib name.
        let dir = homebrew_libdir()?;
        let entries = std::fs::read_dir(dir).ok()?;
        let mut best: Option<u32> = None;
        for e in entries.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            // libicuuc.78.dylib
            if let Some(rest) = name.strip_prefix("libicuuc.") {
                if let Some(num) = rest.split('.').next() {
                    if let Ok(v) = num.parse::<u32>() {
                        best = Some(best.map_or(v, |b| b.max(v)));
                    }
                }
            }
        }
        best
    })
}

/// Run a pkg-config closure with the Homebrew icu4c pkgconfig dir prepended to
/// `PKG_CONFIG_PATH` (Homebrew keeps icu4c keg-only, so its `.pc` files are not
/// on the default search path).
fn with_homebrew_pkgconfig<T>(f: impl FnOnce() -> Option<T>) -> Option<T> {
    let prev = std::env::var_os("PKG_CONFIG_PATH");
    let mut paths: Vec<std::path::PathBuf> = Vec::new();
    for base in ["/opt/homebrew/opt/icu4c", "/usr/local/opt/icu4c"] {
        let pc = format!("{base}/lib/pkgconfig");
        if std::path::Path::new(&pc).is_dir() {
            paths.push(pc.into());
        }
    }
    if let Some(p) = &prev {
        paths.extend(std::env::split_paths(p));
    }
    let joined = std::env::join_paths(paths).ok();
    if let Some(j) = &joined {
        std::env::set_var("PKG_CONFIG_PATH", j);
    }
    let r = f();
    match prev {
        Some(p) => std::env::set_var("PKG_CONFIG_PATH", p),
        None => {
            if joined.is_some() {
                std::env::remove_var("PKG_CONFIG_PATH");
            }
        }
    }
    r
}
