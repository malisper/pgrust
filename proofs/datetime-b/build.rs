fn main() {
    // Link the vendored C only for native bins/tests (the band-immune
    // divider rows' tested(differential) tier); kani uses --c-lib instead.
    if std::env::var("CARGO_CFG_KANI").is_err() {
        // -ffp-contract=off: pins the native tier to strict (non-fused) FP,
        // matching both Rust's semantics and CBMC's C model. Measured: with
        // clang's default contraction the interval_div tail
        // (span->time/factor + sec_remainder*USECS_PER_SEC) fuses into fma
        // and drifts a few ulp (~1779 of 10.7M native-diff cases, time off
        // by 2-8 usec at 2e16 magnitude). Whether REAL PG (gcc -O2,
        // -ffp-contract=fast on aarch64) contracts there is a GROUND-TRUTH
        // item for the runner lane (geo pg_hypot precedent) — not a rig
        // question.
        cc::Build::new()
            .file("c/pg_datetime_b.c")
            .flag("-fwrapv")
            .flag("-ffp-contract=off")
            .compile("pg_datetime_b");
    }
    println!("cargo:rerun-if-changed=c/pg_datetime_b.c");
}
