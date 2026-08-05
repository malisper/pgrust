// The re2 engine compiles only where libre2 development files exist — but
// absence must never be SILENT (GL-STRDEFECTS-1): a Spencer-only binary is a
// multi-x regression on regexp-heavy shapes and shipped once with zero
// warnings. Loudness contract:
//   * release-family profiles (PROFILE=release: release/fast-profile/dist/
//     dist-prof/dist-pgo/profiling) FAIL the build when RE2 is absent —
//     including under PGRUST_FORCE_NO_RE2, which is a DEV-ONLY hatch: the
//     W2a lane's laptop (night/w2a-parallel-writer @ 351687a38) hit the
//     second failure mode of the old script — pkg-config absent + static
//     libre2.a chosen -> undefined absl symbols at link — and the only
//     escape was FORCE_NO_RE2, i.e. a silently Spencer-only binary; a
//     release-family binary must never be born that way;
//   * dev/debug builds emit a cargo:warning and proceed Spencer-only
//     (FORCE_NO_RE2 or genuinely absent libre2 alike).
// The broken-link mode itself stays loud by nature (the linker fails); the
// artifact-level assertion (scripts/check-re2-linkage.sh) covers BOTH modes
// post-link: a binary without RE2 evidence fails the re2 expectation no
// matter which path dropped it.
// Runtime witnesses ride separately: the postmaster startup line and the
// pgrust.regex_re2_linked preset GUC; scripts/check-re2-linkage.sh greps a
// built (stripped) binary for RE2 string evidence.
//
// On Linux, when the headers are absent and apt-get is available (fleet
// build pods run as root with network), the script installs libre2-dev
// itself — dist builds have no apt knob. libre2 links STATICALLY when
// libre2.a exists so the shipped dist binary carries no runtime .so
// dependency on the bench pods; the static archive's transitive deps (the
// abseil closure newer re2 releases require) come from pkg-config's static
// view where available.
use std::path::{Path, PathBuf};
use std::process::Command;

fn probe_pkgconfig(statik: bool) -> Option<pkg_config::Library> {
    pkg_config::Config::new().statik(statik).cargo_metadata(false).probe("re2").ok()
}

fn header_dirs() -> Vec<PathBuf> {
    ["/usr/include", "/usr/local/include", "/opt/homebrew/include"]
        .iter()
        .map(PathBuf::from)
        .filter(|d| d.join("re2/re2.h").exists())
        .collect()
}

fn try_apt_install() -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    let apt_ok = Command::new("apt-get").arg("--version").output().is_ok();
    if !apt_ok {
        return false;
    }
    let _ = Command::new("apt-get").args(["update", "-qq"]).status();
    matches!(
        Command::new("apt-get").args(["install", "-y", "-qq", "libre2-dev"]).status(),
        Ok(s) if s.success()
    )
}

fn release_family() -> bool {
    // Cargo sets PROFILE from the profile's inherits root: "release" for
    // release and every custom profile inheriting it, "debug" otherwise.
    std::env::var("PROFILE").as_deref() == Ok("release")
}

fn wasm_target() -> bool {
    // Build scripts see the TARGET arch via CARGO_CFG_TARGET_ARCH.
    std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32")
}

// Absent RE2: a hard error on release-family builds, a loud warning on dev.
fn re2_missing(reason: &str) {
    if release_family() {
        panic!(
            "regexp_alt: libre2 not found ({reason}). Release-family profiles \
             (release/fast-profile/dist/dist-pgo/profiling) must not silently ship a \
             Spencer-only regex engine. Install libre2 development files (libre2-dev / \
             brew re2), or set PGRUST_FORCE_NO_RE2=1 to build a Spencer-only binary \
             DELIBERATELY."
        );
    }
    println!(
        "cargo:warning=regexp_alt: libre2 not found ({reason}) — building the \
         Spencer-only regex engine (regex_engine=auto will have no re2 tier)"
    );
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(have_re2)");
    println!("cargo:rerun-if-changed=src/re2_shim.cc");
    println!("cargo:rerun-if-env-changed=PGRUST_FORCE_NO_RE2");
    if wasm_target() {
        // wasm32 carve-out: libre2 is native C++ linked through the cc shim
        // and cannot be built for wasm32-wasip1 at all, so the release-family
        // "must carry RE2" invariant (a ruling about NATIVE shipped binaries)
        // is unsatisfiable here. The wasm module is Spencer-only by necessity
        // — deterministically, for every profile — and wasm/wasm-build.sh
        // documents that as deliberate (it sets PGRUST_FORCE_NO_RE2 for the
        // same reason). Loud, not silent:
        println!(
            "cargo:warning=regexp_alt: wasm32 target — RE2 is unavailable \
             (native C++), building the Spencer-only regex engine"
        );
        return;
    }
    if std::env::var_os("PGRUST_FORCE_NO_RE2").is_some() {
        // Dev-only hatch. A release-family Spencer-only binary must never
        // exist, deliberate or not (coordinator ruling on the W2a evidence).
        if release_family() {
            panic!(
                "regexp_alt: PGRUST_FORCE_NO_RE2 is a dev-only hatch — release-family \
                 profiles (release/fast-profile/dist/dist-pgo/profiling) must carry RE2. \
                 Unset it and install libre2 (and pkg-config, so the static link can \
                 resolve re2's abseil closure), or build a dev profile."
            );
        }
        println!(
            "cargo:warning=regexp_alt: RE2 disabled by PGRUST_FORCE_NO_RE2 — \
             this binary is Spencer-only"
        );
        return;
    }

    let mut pkg = probe_pkgconfig(false);
    let mut headers = header_dirs();
    if pkg.is_none() && headers.is_empty() {
        if !try_apt_install() {
            re2_missing("no pkg-config re2, no re2/re2.h on the header search path");
            return;
        }
        pkg = probe_pkgconfig(false);
        headers = header_dirs();
        if pkg.is_none() && headers.is_empty() {
            re2_missing("apt-get install libre2-dev ran but left no usable re2");
            return;
        }
    }

    let mut build = cc::Build::new();
    build.cpp(true).file("src/re2_shim.cc").flag_if_supported("-std=c++17");
    let mut lib_dirs: Vec<PathBuf> = Vec::new();
    if let Some(lib) = &pkg {
        for p in &lib.include_paths {
            build.include(p);
        }
        lib_dirs.extend(lib.link_paths.iter().cloned());
    }
    for d in &headers {
        build.include(d);
    }
    build.compile("pgr_re2_shim");

    let triplet_dir = format!("/usr/lib/{}-linux-gnu", std::env::consts::ARCH);
    for d in ["/usr/lib", "/usr/local/lib", "/opt/homebrew/lib", triplet_dir.as_str()] {
        lib_dirs.push(PathBuf::from(d));
    }
    // Static linking serves shipped binaries on every OS (no runtime .so/.dylib
    // dependency): prefer libre2.a wherever one is on the search path. Dev macs
    // are unaffected — homebrew ships only the dylib, so they still fall
    // through to dynamic linking; macOS release builds provide a static re2
    // prefix via PKG_CONFIG_PATH (v0.2 macOS artifacts ship this way). When a
    // static archive has an abseil closure (newer re2), it must be resolvable
    // via pkg-config's static view or the link fails loudly.
    let static_dir = lib_dirs.iter().find(|d| d.join("libre2.a").exists());
    match static_dir {
        Some(d) => {
            println!("cargo:rustc-link-search=native={}", d.display());
            println!("cargo:rustc-link-lib=static=re2");
            // Newer re2 releases depend on abseil; the static archive does
            // not carry its deps, so emit pkg-config's static-view closure
            // after libre2.a (link order: dependents first). Older re2
            // (no Requires:) makes this a no-op.
            if let Some(closure) = probe_pkgconfig(true) {
                for p in &closure.link_paths {
                    println!("cargo:rustc-link-search=native={}", p.display());
                }
                for l in &closure.libs {
                    if l != "re2" {
                        println!("cargo:rustc-link-lib={l}");
                    }
                }
            }
        }
        None => {
            for d in lib_dirs.iter().filter(|d| Path::new(d).exists()) {
                println!("cargo:rustc-link-search=native={}", d.display());
            }
            println!("cargo:rustc-link-lib=re2");
        }
    }
    println!("cargo:rustc-cfg=have_re2");
}
