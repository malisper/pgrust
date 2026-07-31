// Compile the vendored PostgreSQL C oracles for the differential fuzz
// targets (csrc/README-style provenance headers in each file). Same cc
// pattern as proofs/brin-minmax/build.rs — plain native compile; there is
// no Kani arm here (the fuzz workspace never builds under cargo-kani).
fn main() {
    cc::Build::new()
        .file("csrc/pg_float_io.c")
        .file("csrc/pg_float_math.c")
        .file("csrc/pg_geo_io.c")
        .file("csrc/pg_strfam.c")
        .file("csrc/ryu/d2s.c")
        .file("csrc/ryu/f2s.c")
        .include("csrc/shim")
        .include("csrc/ryu")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        // FP-CONTRACTION CARVE (2026-07-30, found by float_math_diff):
        // clang's default -ffp-contract=on fuses e.g. asind_q1's
        // `90.0 - (acos_x / acos_0_5) * 60.0` into fmsub on arm64, a
        // 1-ulp-different rounding rustc NEVER performs (witness input
        // f64 bits bfe000000000003f, see diff.rs
        // tests::dasind_fp_contraction_witness). Contraction is a
        // C-compiler codegen choice, not float.c semantics — baseline
        // x86-64 PG builds cannot contract (no FMA), while aarch64 gcc
        // defaults contract — so the well-defined oracle for "same
        // wrapper logic over the same libm" is the uncontracted build.
        .flag_if_supported("-ffp-contract=off")
        .compile("pg_difffuzz_oracle");
    println!("cargo:rerun-if-changed=csrc");
}
