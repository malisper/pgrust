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
        // quote_diff oracle (p1-laner): verbatim 18.3 quote.c core +
        // ruleutils.c quote_identifier; keyword tables extern'd from
        // pg_enc_tables.c / tablesfam (see pg_quote_io.c header).
        .file("csrc/pg_quote_io.c")
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
        .file("csrc/pg_enc_tables.c")
        .file("csrc/ryu/d2s.c")
        .file("csrc/ryu/f2s.c")
        .include("csrc/shim")
        .include("csrc/ryu")
        // pg_enc_tables.c includes the SAME generated kwlist_d.h the
        // shipped keywords crate's build.rs transcribes (table parity by
        // shared source of truth)
        .include("../../crates/common/keywords")
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

    // jsonbfam oracle (p1-lanev, jsonbio_diff): verbatim 18.3 jsonapi.c /
    // wchar.c / stringinfo.c / jsonb_util.c / qsort_arg.c whole-file TUs plus
    // extracted jsonb.c / jsonfuncs.c / numeric.c / json.c / pqformat.c /
    // mbutils.c segments (csrc/jsonbfam/*.inc), against the jsonbfam shim
    // header set — a SEPARATE build because its shim postgres.h would
    // collide with the ryu/float shims of the main oracle lib.
    // float4in/8in_internal are extern'd from pg_float_io.c (main lib).
    let mut jsonbfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        jsonbfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    for f in [
        "pg_jsonbio_io.c", "jsonbfam/jsonapi.c", "jsonbfam/wchar.c",
        "jsonbfam/stringinfo.c", "jsonbfam/jsonb_util.c",
        "jsonbfam/qsort_arg.c",
        // jsonbops_diff extension (p1-lanev): ops/mutate/getfield oracle
        "pg_jsonbops.c", "jsonbfam/jsonb_op.c", "jsonbfam/hashfn.c",
    ] {
        jsonbfam.file(format!("csrc/{f}"));
    }
    jsonbfam
        .include("csrc/jsonbfam/shim")
        .include("csrc/jsonbfam/include")
        .include("csrc")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .compile("pg_difffuzz_jsonbfam");

    // SYMBOL ISOLATION (landing fix, merge/p1-wave1 2026-07-30): three lane
    // oracles (hashenc/p1-lanee, cryptofam/p1-lanef, enc_tables/p1-laneg in
    // the main oracle lib) each vendor the SAME verbatim 18.3 TUs (base64.c,
    // md5.c, pg_crc.c, kwlookup.c, ...) but against DIFFERENT shims (e.g.
    // cryptofam's bytea is a {ptr,len} frame; hashenc's is a real varlena).
    // Linked into one binary the duplicate globals resolve to ONE definition
    // — crc32_bytea segfaulted and pg_diff_scan_keyword_lookup silently
    // cross-bound between laneg's and lanef's oracles. Fix: rename each
    // family's colliding symbols with a family prefix at compile time so
    // every oracle keeps its OWN vendored copy (the per-lane drift-detection
    // property the DUPLICATION LEDGER preserves the targets for).
    const CRYPTO_SHARED_SYMS: &[&str] = &[
        "crc32_bytea", "crc32c_bytea", "pg_comp_crc32c_sb8", "pg_crc32_table",
        "pg_b64_dec_len", "pg_b64_decode", "pg_b64_enc_len", "pg_b64_encode",
        "pg_cryptohash_create", "pg_cryptohash_error", "pg_cryptohash_final",
        "pg_cryptohash_free", "pg_cryptohash_init", "pg_cryptohash_update",
        "pg_hmac_create", "pg_hmac_error", "pg_hmac_final", "pg_hmac_free",
        "pg_hmac_init", "pg_hmac_update",
        "pg_md5_binary", "pg_md5_encrypt", "pg_md5_final", "pg_md5_hash",
        "pg_md5_init", "pg_md5_update",
        "pg_sha1_final", "pg_sha1_init", "pg_sha1_update",
        "pg_sha224_final", "pg_sha224_init", "pg_sha224_update",
        "pg_sha256_final", "pg_sha256_init", "pg_sha256_update",
        "pg_sha384_final", "pg_sha384_init", "pg_sha384_update",
        "pg_sha512_final", "pg_sha512_init", "pg_sha512_update",
        "scram_build_secret", "scram_ClientKey", "scram_H",
        "scram_SaltedPassword", "scram_ServerKey",
    ];
    // hashenc family: also isolate the two symbols it shares with the main
    // oracle lib (laneg's enc_tables vendors its own base64 + strlcpy).
    const HASHENC_EXTRA_SYMS: &[&str] = &["ascii_safe_strlcpy"];

    // hashenc_diff oracle (p1-lanee): verbatim src/common + ascii/crc TUs.
    // The src/common files build -DFRONTEND (identical logic; malloc
    // allocator, exactly a real frontend libpgcommon build).
    let mut hashenc = cc::Build::new();
    for s in CRYPTO_SHARED_SYMS.iter().chain(HASHENC_EXTRA_SYMS) {
        hashenc.define(s, format!("hashenc_impl_{s}").as_str());
    }
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
    let mut hashenc_glue = cc::Build::new();
    for s in CRYPTO_SHARED_SYMS.iter().chain(HASHENC_EXTRA_SYMS) {
        hashenc_glue.define(s, format!("hashenc_impl_{s}").as_str());
    }
    hashenc_glue
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
    // cryptofam_diff oracle (p1-lanef): verbatim 18.3 crypto/hash family,
    // FRONTEND arms (malloc/free, no CHECK_FOR_INTERRUPTS), own shim include
    // tree so the main shim postgres.h never leaks into these units.
    let mut cryptofam = cc::Build::new();
    for s in CRYPTO_SHARED_SYMS {
        cryptofam.define(s, format!("cryptofam_{s}").as_str());
    }
    cryptofam
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
    let mut tablesfam = cc::Build::new();
    for s in [
        "ScanKeywordLookup", "ScanKeywords",
        "pg_diff_scan_keyword_lookup", "pg_diff_get_scan_keyword",
    ] {
        tablesfam.define(s, format!("tablesfam_{s}").as_str());
    }
    tablesfam
        .file("csrc/tablesfam/kwlookup.c")
        .file("csrc/tablesfam/keywords.c")
        .file("csrc/tablesfam/unicode_category.c")
        .file("csrc/tablesfam/pg_diff_tablesfam.c")
        .include("csrc/tablesfam/shim_fe")
        .include("csrc/tablesfam/include")
        .include("csrc/tablesfam")
        // kwlist_d.h comes from THE SHIPPED CRATE (not a private copy), so a
        // transcription drift between the crate's generated tables and the C
        // oracle's is a divergence instead of an invisible agreement.
        .include("../../crates/common/keywords")
        .define("FRONTEND", None)
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_tablesfam");

    println!("cargo:rerun-if-changed=csrc");
    println!("cargo:rerun-if-env-changed=PGRUST_FUZZ_CSANCOV");
}
