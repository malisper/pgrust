//! Exposes the build's ACTUAL optimization level to `src/lib.rs` as the cfg
//! `pgrust_opt_level = "<N>"`, so `STACK_DEPTH_SCALE` can be keyed on it.
//!
//! Cargo hands build scripts `OPT_LEVEL` for the profile in force (after
//! per-package overrides): "0", "1", "2", "3", "s" or "z". rustc itself
//! exposes no cfg for it, and `cfg!(debug_assertions)` is NOT a proxy: an
//! opt-level >= 1 profile with `debug-assertions = true` (the
//! optimized-with-assertions server shape, bug catalog PR1613-1) has
//! near-optimized frames yet was classified as unoptimized, so the guard
//! enforced the 32x opt-level-0 budget and deep json/jsonb literals ran to
//! end of input (22P02) where C raises 54001 (PR #1613).
//!
//! The raw value is also exported as the `PGRUST_OPT_LEVEL` env so the
//! test-facing `BUILD_OPT_LEVEL` constant can report it.
fn main() {
    let opt_level = std::env::var("OPT_LEVEL").unwrap_or_default();
    println!("cargo:rustc-check-cfg=cfg(pgrust_opt_level, values(\"0\", \"1\", \"2\", \"3\", \"s\", \"z\"))");
    if matches!(opt_level.as_str(), "0" | "1" | "2" | "3" | "s" | "z") {
        println!("cargo:rustc-cfg=pgrust_opt_level=\"{opt_level}\"");
    }
    println!("cargo:rustc-env=PGRUST_OPT_LEVEL={opt_level}");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=OPT_LEVEL");
}
