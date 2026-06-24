//! Link directives for the OpenLDAP provider (`libldap` + `liblber`), behind
//! the `with-ldap` feature (PostgreSQL's `--with-ldap`, `#ifdef USE_LDAP`).
//!
//! PostgreSQL links `-lldap -llber` when `--with-ldap` is given. We mirror that.
//! macOS no longer ships a public system OpenLDAP; we link the Homebrew
//! keg-only `openldap` (the same one whose `slapd` the src/test/ldap suite
//! drives). On Linux the system OpenLDAP is used directly. Override the search
//! path with `OPENLDAP_LIB_DIR`.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=OPENLDAP_DIR");
    println!("cargo:rerun-if-env-changed=OPENLDAP_LIB_DIR");

    if std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("wasm") {
        return;
    }

    if std::env::var_os("CARGO_FEATURE_WITH_LDAP").is_none() {
        // Feature off => no binding, no link (faithful `#ifdef USE_LDAP` off).
        return;
    }

    // Resolve the OpenLDAP library directory:
    //   1. OPENLDAP_LIB_DIR (explicit).
    //   2. OPENLDAP_DIR/lib.
    //   3. `brew --prefix openldap`/lib (macOS keg-only default).
    let lib_dir = std::env::var("OPENLDAP_LIB_DIR")
        .ok()
        .or_else(|| std::env::var("OPENLDAP_DIR").ok().map(|d| format!("{d}/lib")))
        .or_else(brew_openldap_lib_dir);

    if let Some(dir) = lib_dir {
        println!("cargo:rustc-link-search=native={dir}");
    }

    // Same libraries PostgreSQL's `--with-ldap` links.
    println!("cargo:rustc-link-lib=ldap");
    println!("cargo:rustc-link-lib=lber");
}

/// `brew --prefix openldap`/lib, if Homebrew has the keg installed.
fn brew_openldap_lib_dir() -> Option<String> {
    let out = Command::new("brew").args(["--prefix", "openldap"]).output().ok()?;
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
