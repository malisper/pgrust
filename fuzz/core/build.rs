// Compile the vendored PostgreSQL C oracles for the differential fuzz
// targets (csrc/README-style provenance headers in each file). Same cc
// pattern as proofs/brin-minmax/build.rs — plain native compile; there is
// no Kani arm here (the fuzz workspace never builds under cargo-kani).
fn main() {
    let mut build = cc::Build::new();
    // SANCOV ON THE C ORACLE (NEZHA union-coverage, campaign 2026-07-30):
    // instrument the vendored csrc objects so libFuzzer's retention feedback
    // sees C-side edges too — Rust-side-only feedback discards exactly the
    // inputs likeliest to diverge. Opt-in (PGRUST_FUZZ_CSANCOV=1) rather
    // than keyed off CARGO_CFG_FUZZING: cargo-fuzz builds every workspace
    // dep with the same env, and the flag is meaningless (though harmless)
    // for `cargo test`. Verified linking under cargo +nightly-2026-07-17
    // fuzz build (libFuzzer provides the sancov runtime).
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        build.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    build
        // COMPILE GATE (encode_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_encode_io.c is filled
        // with verbatim vendored C (README-TODO-encode_diff.md step 1).
        // .file("csrc/pg_encode_io.c")
        .file("csrc/pg_float_io.c")
        .file("csrc/pg_float_math.c")
        .file("csrc/pg_geo_io.c")
        .file("csrc/pg_strfam.c")
        .file("csrc/pg_uuid_io.c")
        .file("csrc/pg_mac_io.c")
        .file("csrc/pg_name_io.c")
        .file("csrc/pg_cash_io.c")
        .file("csrc/pg_char.c")
        .file("csrc/pg_bool.c")
        .file("csrc/pg_pseudotypes.c")
        .file("csrc/pg_lsn_oracle.c")
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

    // hashenc_diff oracle (p1-lanee): verbatim src/common + ascii/crc TUs.
    // The src/common files build -DFRONTEND (identical logic; malloc
    // allocator, exactly a real frontend libpgcommon build).
    let mut hashenc = cc::Build::new();
    for f in [
        "base64.c", "md5.c", "sha1.c", "sha2.c", "cryptohash.c", "hmac.c",
        "md5_common.c", "scram-common.c",
    ] {
        hashenc.file(format!("csrc/hashenc/{f}"));
    }
    hashenc
        .define("FRONTEND", None)
        .include("csrc/hashenc/shim")
        .include("csrc/hashenc/include")
        .include("csrc/hashenc")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_hashenc_fe");
    cc::Build::new()
        .file("csrc/hashenc/pg_crc32c_sb8.c")
        .file("csrc/hashenc/pg_crc.c")
        .file("csrc/hashenc/pg_hashenc_ascii.c")
        .file("csrc/hashenc/pg_hashenc_glue.c")
        .include("csrc/hashenc/shim")
        .include("csrc/hashenc/include")
        .include("csrc/hashenc")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_hashenc");

    println!("cargo:rerun-if-changed=csrc");
    println!("cargo:rerun-if-env-changed=PGRUST_FUZZ_CSANCOV");
}
