// Compile the vendored PostgreSQL C oracles for the differential fuzz
// targets (csrc/README-style provenance headers in each file). Same cc
// pattern as proofs/brin-minmax/build.rs — plain native compile; there is
// no Kani arm here (the fuzz workspace never builds under cargo-kani).
fn main() {
    cc::Build::new()
        // COMPILE GATE (encode_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_encode_io.c is filled
        // with verbatim vendored C (README-TODO-encode_diff.md step 1).
        // .file("csrc/pg_encode_io.c")
        .file("csrc/pg_float_io.c")
        .file("csrc/pg_float_math.c")
        .file("csrc/pg_geo_io.c")
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

    // cryptofam_diff oracle (p1-lanef): verbatim 18.3 crypto/hash family,
    // FRONTEND arms (malloc/free, no CHECK_FOR_INTERRUPTS), own shim include
    // tree so the main shim postgres.h never leaks into these units.
    cc::Build::new()
        .file("csrc/cryptofam/md5.c")
        .file("csrc/cryptofam/sha1.c")
        .file("csrc/cryptofam/sha2.c")
        .file("csrc/cryptofam/cryptohash.c")
        .file("csrc/cryptofam/hmac.c")
        .file("csrc/cryptofam/md5_common.c")
        .file("csrc/cryptofam/scram-common.c")
        .file("csrc/cryptofam/base64.c")
        .file("csrc/cryptofam/pg_crc32c_sb8.c")
        .file("csrc/cryptofam/pg_crc.c")
        .file("csrc/cryptofam/pg_diff_cryptofam.c")
        .include("csrc/cryptofam/shim_fe")
        .include("csrc/cryptofam/include")
        .include("csrc/cryptofam")
        .define("FRONTEND", None)
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_cryptofam");

    // tablesfam_diff oracle (p1-lanef): verbatim 18.3 kwlookup/keywords/
    // unicode_category, FRONTEND arms, own shim include tree.
    cc::Build::new()
        .file("csrc/tablesfam/kwlookup.c")
        .file("csrc/tablesfam/keywords.c")
        .file("csrc/tablesfam/unicode_category.c")
        .file("csrc/tablesfam/pg_diff_tablesfam.c")
        .include("csrc/tablesfam/shim_fe")
        .include("csrc/tablesfam/include")
        .include("csrc/tablesfam")
        .define("FRONTEND", None)
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_tablesfam");

    println!("cargo:rerun-if-changed=csrc");
}
