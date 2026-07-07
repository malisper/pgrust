// The re2 engine compiles only where libre2 development files exist.
// EXPERIMENT-ONLY (regex-engine-ab branch): on Linux, when the headers are
// absent and apt-get is available (fleet build pods run as root with
// network), the script installs libre2-dev itself — dist builds have no apt
// knob. Any failure leaves the stub engine (runtime error on
// SET regex_engine=re2); spencer/rust arms are unaffected. libre2 links
// STATICALLY when libre2.a exists so the shipped dist binary carries no
// runtime .so dependency on the bench pods.
use std::path::{Path, PathBuf};
use std::process::Command;

fn probe_pkgconfig() -> Option<pkg_config::Library> {
    pkg_config::Config::new().cargo_metadata(false).probe("re2").ok()
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

fn main() {
    println!("cargo:rustc-check-cfg=cfg(have_re2)");
    println!("cargo:rerun-if-changed=src/re2_shim.cc");
    println!("cargo:rerun-if-env-changed=PGRUST_FORCE_NO_RE2");
    if std::env::var_os("PGRUST_FORCE_NO_RE2").is_some() {
        return;
    }

    let mut pkg = probe_pkgconfig();
    let mut headers = header_dirs();
    if pkg.is_none() && headers.is_empty() {
        if !try_apt_install() {
            return;
        }
        pkg = probe_pkgconfig();
        headers = header_dirs();
        if pkg.is_none() && headers.is_empty() {
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
    let static_dir = lib_dirs.iter().find(|d| d.join("libre2.a").exists());
    match static_dir {
        Some(d) => {
            println!("cargo:rustc-link-search=native={}", d.display());
            println!("cargo:rustc-link-lib=static=re2");
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
