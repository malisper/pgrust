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
        // COMPILE GATE (define_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_define_io.c is filled
        // with verbatim vendored C (README-TODO-define_diff.md step 1).
        .file("csrc/pg_define_io.c")
        // libfam_diff oracle: verbatim vendored files under csrc/libfam/
        // (whole-file includes; provenance in csrc/pg_libfam_io.c header).
        .file("csrc/pg_libfam_io.c")
        // stub-pin facility (fuzz/STUBS.md): shared thread-local pinned
        // session state (GUC/clock/prng/workmem) + setters the Rust driver
        // (core/src/stubs.rs) calls, + consumer wrappers routing the pinned
        // globals into verbatim vendored consumers by extern call. NOT
        // PostgreSQL source; stdint-only, no family include tree.
        .file("csrc/stubshims/pg_stub_state.c")
        // portfam_diff oracle (p1-microbatch PORTFAM) compiles in its OWN
        // cc::Build below (pg_difffuzz_portfam): it needs the
        // csrc/portfam/{shim,include} tree, whose c.h/postgres.h must not
        // leak into this build's TUs.
        // json_diff oracle lives in the dedicated jsonfam cc::Build below
        // (own shim include tree; pg_jsonfam_-prefixed symbols).
        // arrayfuncs_diff oracle (p1-lanex): verbatim 18.3 arrayfuncs.c core
        // + arrayutils/numutils helpers; pg_afx_-prefixed symbols (see the
        // file header for provenance + shims).
        .file("csrc/pg_arrayfuncs_io.c")
        // oraclefam_diff oracle (p1-laneaj): verbatim 18.3 oracle_compat.c +
        // varlena.c text family + formatting.c asc_* kernels + the mbutils/
        // wchar multibyte walkers behind them (see pg_oraclefam_io.c header).
        .file("csrc/pg_oraclefam_io.c")
        // tzfam_diff oracle (p1-mb-tzfam): verbatim 18.3 strftime.c +
        // tzparser.c + datetime.c ConvertTimeZoneAbbrevs + ts_locale.c
        // t_is* macros (see pg_tzfam_io.c header for provenance + shims).
        // RESTORED (p1-mb-contribc, 2026-08-01): the p1-microbatch-1 union
        // merge kept the three family TUs under csrc/ but dropped their
        // build.rs registrations — tzfam/miscfam/netfam targets could not
        // link at main.
        .file("csrc/pg_tzfam_io.c")
        // miscfam_diff oracle (p1-mb-miscfam): verbatim 18.3 cmdtag.c +
        // pg_class.c errdetail_relkind + earthdistance.c +
        // pg_rusage.c show + xlogstats.c + common/stringinfo.c core
        // (see pg_miscfam_io.c header; cmdtaglist.h vendored under
        // csrc/miscfam/tcop/).
        .file("csrc/pg_miscfam_io.c")
        .include("csrc/miscfam")
        // netfam_diff oracle (p1-mb-netfam): verbatim 18.3 ifaddr.c pure
        // core + pqformat.c + pqformat.h inlines + common/stringinfo.c
        // behind nf_-renames (see pg_netfam_io.c header for provenance +
        // the encoding/putmessage seam shims).
        .file("csrc/pg_netfam_io.c")
        // hstore_diff oracle (p1-mb-contribc): verbatim 18.3 contrib/hstore
        // hstore_io.c + hstore_op.c cores + the array/stringinfo/pqformat/
        // json machinery behind them, hst_-prefixed (see pg_hstorefam_io.c
        // header for provenance, shims and the records/SRF/jsonb/gist/gin
        // carves).
        .file("csrc/pg_hstorefam_io.c")
        // wparser_diff oracle (p1-mb-contribc): verbatim 18.3
        // wparser_def.c tokenizer half (lines 33-1935, through prsd_end),
        // wpd_-prefixed; encoding walkers resolved against the verbatim
        // wfam_ copies in pg_wcharfam.c (see the file header for
        // provenance, shims and the ts_headline carve).
        .file("csrc/pg_wparserfam_io.c")
        // libfam_diff oracle: verbatim vendored files under csrc/libfam/
        // (whole-file includes; provenance in csrc/pg_libfam_io.c header).
        // RESTORED (p1-mb-contribc, 2026-08-01): dropped by the same union
        // merge as the tzfam/miscfam/netfam registrations above.
        // DEDUPED (single registration above): // DEDUPED (single registration above): .file("csrc/pg_libfam_io.c")
        // DEDUPED (single registration above): .file("csrc/pg_tzfam_io.c")
        // COMPILE GATE (array_userfuncs_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_array_userfuncs_io.c is filled
        // with verbatim vendored C (README-TODO-array_userfuncs_diff.md step 1).
        .file("csrc/pg_array_userfuncs_io.c")
        // rowtypes_diff oracle (p1-laneai): verbatim 18.3 rowtypes.c bodies +
        // heaptuple.c/tupdesc.c/datum.c/stringinfo.c/pqformat.c machinery,
        // assembled per the pg_rowtypes_io.c header; hashfn extern'd from
        // pg_mac_io.c.
        .file("csrc/pg_rowtypes_io.c")
        // tupaccess_diff oracle (p1-tupaccess): verbatim 18.3 heaptuple.c /
        // tupdesc.c / attmap.c / tupconvert.c (+ datum.c/name.c helpers),
        // assembled per the pg_tupaccess_io.c header; assertions LIVE in
        // that TU (verify_compact_attribute audits); hashfn extern'd from
        // pg_mac_io.c.
        .file("csrc/pg_tupaccess_io.c")
        // range + multirange oracles, ONE translation unit: this file
        // #includes csrc/pg_rangetypes_io.c (which is therefore NOT listed
        // here — listing both would define every pg_diff_* entry twice).
        // multirangetypes.c calls fourteen rangetypes.c statics plus the
        // shared typcache mock / arena / ereport shim; see the structure
        // section of pg_multirangetypes_io.c's header for why including beats
        // re-vendoring or extern-promoting them.
        .file("csrc/pg_multirangetypes_io.c")
        // regexp_diff oracle (p1-laneag): csrc/pg_regexp_io.c compiles in its
        // OWN cc::Build below (pg_difffuzz_regexfam) together with the
        // verbatim vendored Spencer engine under csrc/regexfam/ — it needs
        // that family's shim include tree (regexfam postgres.h etc.), which
        // must not leak into the files of THIS build. Gate satisfied
        // 2026-07-31: every SCAFFOLD-TODO #error site is filled.
        // like_diff oracle (p1-laneag): verbatim 18.3 like.c core with
        // like_match.c pasted once per stamping (see pg_like_io.c header).
        .file("csrc/pg_like_io.c")
        // quote_diff oracle (p1-laner): verbatim 18.3 quote.c core +
        // ruleutils.c quote_identifier; keyword tables extern'd from
        // pg_enc_tables.c / tablesfam (see pg_quote_io.c header).
        .file("csrc/pg_quote_io.c")
        // COMPILE GATE (hashfn_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_hashfn_io.c is filled
        // with verbatim vendored C (README-TODO-hashfn_diff.md step 1).
        .file("csrc/pg_hashfn_io.c")
        // COMPILE GATE (arrayutils_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_arrayutils_io.c is filled
        // with verbatim vendored C (README-TODO-arrayutils_diff.md step 1).
        .file("csrc/pg_arrayutils_io.c")
        // COMPILE GATE (pg_prng_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_pg_prng_io.c is filled
        // with verbatim vendored C (README-TODO-pg_prng_diff.md step 1).
        .file("csrc/pg_pg_prng_io.c")
        // miscfam_diff oracle (p1-mb-miscfam): verbatim 18.3 cmdtag.c +
        // pg_class.c errdetail_relkind + earthdistance.c +
        // pg_rusage.c show + xlogstats.c + common/stringinfo.c core
        // (see pg_miscfam_io.c header; cmdtaglist.h vendored under
        // csrc/miscfam/tcop/).
        // DEDUPED (single registration above): // DEDUPED (single registration above): .file("csrc/pg_miscfam_io.c")
        // DEDUPED (single registration above): .file("csrc/pg_miscfam_io.c")
        // contriba_diff oracle (p1-mb-contriba): verbatim 18.3
        // fuzzystrmatch.c + dmetaphone.c + daitch_mokotoff.c (generated
        // chart under csrc/contribafam/) + levenshtein.c both expansions +
        // isn.c with its range-table headers (csrc/contribafam/). Fully
        // self-shimmed TU; every extern it exports is pg_ca_-prefixed and
        // all vendored bodies are file-static (see the file header).
        .file("csrc/pg_contribafam_io.c")
        // netfam_diff oracle (p1-mb-netfam): verbatim 18.3 ifaddr.c pure
        // core + pqformat.c + pqformat.h inlines + common/stringinfo.c
        // behind nf_-renames (see pg_netfam_io.c header for provenance +
        // the encoding/putmessage seam shims).
        // DEDUPED (single registration above): // DEDUPED (single registration above): .file("csrc/pg_netfam_io.c")
        // DEDUPED (single registration above): .file("csrc/pg_netfam_io.c")
        // COMPILE GATE (encode_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_encode_io.c is filled
        // with verbatim vendored C (README-TODO-encode_diff.md step 1).
        // .file("csrc/pg_encode_io.c")
        .file("csrc/pg_float_io.c")
        .file("csrc/pg_float_math.c")
        .file("csrc/pg_float_agg_check.c")
        .file("csrc/pg_geo_io.c")
        .file("csrc/pg_strfam.c")
        // numutils_diff oracle (p1-laneaj): verbatim 18.3 numutils.c
        .file("csrc/pg_numutils.c")
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
        .include("csrc/pgdt")
        .include("csrc")
        .include("csrc/miscfam")
        .include("csrc/ryu")
        // libfam_diff: verbatim lib/ headers + reduced port/common/utils
        // headers (appended LAST so existing include resolution is
        // unchanged; no other main-build TU includes these paths)
        .include("csrc/libfam/include")
        // pg_enc_tables.c includes the SAME generated kwlist_d.h the
        // shipped keywords crate's build.rs transcribes (table parity by
        // shared source of truth)
        .include("../../crates/common/keywords")
        // libfam_diff: verbatim lib/ headers + reduced port/common/utils
        // headers (appended LAST so existing include resolution is
        // unchanged; no other main-build TU includes these paths)
        .include("csrc/libfam/include")
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
        // SANCOV ON THE C SIDE (NEZHA finding, fuzzuproof-crate skill):
        // under cargo-fuzz (cfg(fuzzing) set) instrument the vendored
        // oracle objects too, so corpus retention is UNION coverage —
        // Rust-side-only feedback discards exactly the inputs likeliest
        // to diverge. No-op for plain cargo build/test.
        .flag_if_supported(
            if std::env::var_os("CARGO_CFG_FUZZING").is_some() {
                "-fsanitize=fuzzer-no-link"
            } else {
                "-fno-strict-aliasing" // harmless repeat when not fuzzing
            },
        )
        // COMPILE GATE (tsrank_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_tsrank_io.c is filled
        // with verbatim vendored C (README-TODO-tsrank_diff.md step 1).
        // .file("csrc/pg_tsrank_io.c")
        // tsvector_core_diff oracle (p1-laneae): runtime shims + driver
        // entries in pg_tsvector_core_io.c; VERBATIM 18.3 C under
        // csrc/tsvec/ (tsvector.c, tsvector_parser.c, tsvector_op.c with
        // labeled carve blocks, pg_qsort/qsort_arg for tie-order parity).
        .file("csrc/pg_tsvector_core_io.c")
        .file("csrc/tsvec/tsvector.c")
        .file("csrc/tsvec/tsvector_parser.c")
        .file("csrc/tsvec/tsvector_op.c")
        .file("csrc/tsvec/qsort.c")
        .file("csrc/tsvec/qsort_arg.c")
        // tsvec oracle header web: AFTER csrc/shim so ryu/other oracles
        // keep resolving "postgres.h" to the shim one; the tsvec TUs find
        // their own postgres.h/c.h same-directory (quote-include rule).
        .include("csrc/tsvec/include")
        .compile("pg_difffuzz_oracle");

    // wcharfam oracle (p1-laneah): verbatim 18.3 wchar.c + encnames.c +
    // mbutils.c pure extracts, own include dir (its c.h shim must not leak
    // into sibling TUs); every extern symbol is macro-renamed wfam_* inside
    // pg_wcharfam.c itself, so no symbol-isolation defines are needed here.
    let mut wcharfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        wcharfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    wcharfam
        .file("csrc/pg_wcharfam.c")
        // LINK FIX (p1-microbatch, 2026-08-01): pg_wcharfam.c's vendored
        // mbutils extract calls pg_wchar_strlen, whose upstream definition
        // lives in src/backend/utils/mb/wstrncmp.c — a TU this family never
        // vendored. Plain `cargo test` never caught it (macOS -dead_strip
        // discards the unreferenced cone), but EVERY cargo-fuzz target
        // failed to link with "Undefined symbols: _pg_wchar_strlen",
        // including already-landed ones. Vendored verbatim below.
        .file("csrc/wcharfam/wstrncmp.c")
        .include("csrc/wcharfam")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_wcharfam");
    println!("cargo:rerun-if-changed=csrc/pg_wcharfam.c");
    println!("cargo:rerun-if-changed=csrc/wcharfam");
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
    // SYMBOL ISOLATION vs pg_hashfn_io.c (p1-laneh): jsonbfam/hashfn.c is a
    // second verbatim hashfn.c compile; GNU ld hard-errors on the duplicate
    // definitions (multiple definition of `string_hash` …) while Apple ld64
    // tolerates them — the exact wave-1 first-definition-wins hazard, and
    // the reason every Linux mutants rail at the wave-3 train sha refused
    // to build. Rename every extern this TU exports (macro renames like
    // mbconv's bsearch carve).
    for s in [
        "hash_bytes", "hash_bytes_extended", "hash_bytes_uint32",
        "hash_bytes_uint32_extended", "string_hash", "tag_hash",
        "uint32_hash",
    ] {
        jsonbfam.define(s, format!("jbfam_{s}").as_str());
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

    // numericfam oracle (p1-laneu): the whole vendored 18.3 numeric.c
    // #include'd into pg_numeric_oracle.c (cref_numeric precedent) plus the
    // vendored common/hashfn.c. OWN cc::Build with ONLY the numericfam
    // vendor include dir: its postgres.h shim must never shadow (or be
    // shadowed by) csrc/shim/postgres.h in the main oracle lib.
    let mut numericfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        numericfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // wave-3 train sweep: numericfam's verbatim hashfn.c exports collide
    // with pg_hashfn_io.c (p1-laneh) under one binary — same class as the
    // jsonbfam/hashfn.c rename below (GNU ld hard error on Linux rails).
    for s in [
        "hash_bytes", "hash_bytes_extended", "hash_bytes_uint32",
        "hash_bytes_uint32_extended", "string_hash", "tag_hash",
        "uint32_hash",
    ] {
        numericfam.define(s, format!("nfam_{s}").as_str());
    }
    numericfam
        .file("csrc/numericfam/pg_numeric_oracle.c")
        .file("csrc/numericfam/vendor/common/hashfn.c")
        .include("csrc/numericfam/vendor")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-but-set-variable")
        .flag_if_supported("-Wno-unused-function")
        .compile("pg_difffuzz_numericfam");

    // hashenc_diff oracle (p1-lanee): verbatim src/common + ascii/crc TUs.
    // The src/common files build -DFRONTEND (identical logic; malloc
    // allocator, exactly a real frontend libpgcommon build).
    //
    // LINK-ORDER LAW (p1-laneaj, 2026-07-31): the glue archive REFERENCES
    // symbols the fe archive PROVIDES, and cargo passes native static libs
    // to the linker in emission order. Strict left-to-right linkers
    // (binutils ld on Linux stable builds) require referencer-before-
    // provider, so glue MUST be compiled/emitted before fe. macOS ld64 and
    // the cargo-fuzz nightly link path tolerate either order, which is why
    // laptop `cargo test` never caught the inversion.
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

    // json_diff oracle (p1-laneab): whole-TU verbatim 18.3 common/jsonapi.c +
    // common/stringinfo.c plus the json.c/jsonfuncs.c extraction in
    // pg_json_io.c, compiled against its OWN shim include tree
    // (csrc/jsonfam/include; NOT csrc/shim — the two postgres.h shims must
    // never cross). Exported symbols are pg_jsonfam_-prefixed inside the
    // sources (see jsonfam/include/postgres.h), so no cross-lane collisions.
    let mut jsonfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        jsonfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    jsonfam
        .file("csrc/pg_json_io.c")
        .file("csrc/jsonfam/jsonapi.c")
        .file("csrc/jsonfam/stringinfo.c")
        .include("csrc/jsonfam/include")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_jsonfam");
    // mbconv_diff oracle (p1-lanez): the SAME vendored 18.3 conversion-proc
    // C the proofs/mbconv Kani family solves against (conv.c engines +
    // all 25 conversion_procs modules + Unicode radix maps), compiled
    // NATIVELY from its home in proofs/mbconv/c — one source of truth, no
    // csrc copy to drift. The PROOF_EREPORT_FLAG convention (pg_mbconv.h:
    // error => set pg_mbconv_err class + return -1) doubles as the native
    // errcode-class capture plane. Renames: pg_utf_mblen/pg_utf8_islegal
    // collide with pg_name_io.c's verbatim copies; bsearch is the header's
    // CBMC linear-scan model and must not shadow libc bsearch for the rest
    // of the binary.
    let mut mbconv = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        mbconv.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    for s in ["pg_utf_mblen", "pg_utf8_islegal", "bsearch"] {
        mbconv.define(s, format!("mbconv_impl_{s}").as_str());
    }
    for f in [
        "pg_mbconv_common.c", "pg_conv_check.c", "pg_conv_cyrillic_mic.c",
        "pg_conv_euc_cn_mic.c", "pg_conv_euc_jp_sjis.c", "pg_conv_euc_kr_mic.c",
        "pg_conv_euc_tw_big5.c", "pg_conv_euc2004_sjis2004.c",
        "pg_conv_latin_mic.c", "pg_conv_latin2_win1250.c",
        "pg_conv_utf8_big5.c", "pg_conv_utf8_cyrillic.c", "pg_conv_utf8_euc_cn.c",
        "pg_conv_utf8_euc_jp.c", "pg_conv_utf8_euc_kr.c", "pg_conv_utf8_euc_tw.c",
        "pg_conv_utf8_euc2004.c", "pg_conv_utf8_gb18030.c", "pg_conv_utf8_gbk.c",
        "pg_conv_utf8_iso8859_1.c", "pg_conv_utf8_iso8859.c", "pg_conv_utf8_johab.c",
        "pg_conv_utf8_sjis.c", "pg_conv_utf8_sjis2004.c", "pg_conv_utf8_uhc.c",
        "pg_conv_utf8_win.c",
    ] {
        mbconv.file(format!("../../proofs/mbconv/c/{f}"));
    }
    mbconv.file("csrc/mbconv_glue.c");
    mbconv
        .define("PG_MBCONV_TLS", None) // thread-local err flag + glue accessors
        .include("../../proofs/mbconv/c")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-O2")
        .compile("pg_difffuzz_mbconv");
    println!("cargo:rerun-if-changed=../../proofs/mbconv/c");
    // jsonpath_diff oracle (p1-laneaa): verbatim 18.3 jsonpath.c + generated
    // gram/scan (bison 2.3 / flex 2.6.4, checked in — no tools needed at
    // build time) + the full 18.3 regex engine + numeric/formatting/support
    // extracts, against its OWN shim include tree (csrc/jsonpath/include).
    // Generic-named exported symbols get a family prefix so this family
    // keeps its own vendored copies next to every other oracle family
    // (same isolation rationale as CRYPTO_SHARED_SYMS above).
    const JSONPATH_SHARED_SYMS: &[&str] = &[
        "appendBinaryStringInfo", "appendBinaryStringInfoNT",
        "appendStringInfo", "appendStringInfoChar", "appendStringInfoSpaces",
        "appendStringInfoString", "appendStringInfoVA", "destroyStringInfo",
        "enlargeStringInfo", "initStringInfo", "initStringInfoExt",
        "makeStringInfo", "makeStringInfoExt", "resetStringInfo",
        "errcode", "errdetail", "errdetail_internal", "errhint", "errmsg",
        "errmsg_internal",
        "escape_json", "escape_json_with_len", "exprType",
        "GetDatabaseEncoding", "GetDatabaseEncodingName",
        "lappend", "list_make1_impl", "list_make2_impl", "makeString",
        "numeric_in", "numeric_out", "numeric_uminus",
        "datetime_format_has_tz",
        "pg_ascii_tolower", "pg_ascii_toupper", "pg_char_and_wchar_strncmp",
        "pg_mb2wchar_with_len", "pg_mblen", "pg_mblen_cstr", "pg_mblen_range",
        "pg_mblen_unbounded", "pg_mblen_with_len",
        "pg_newlocale_from_collation", "pg_server_to_client",
        "pg_set_regex_collation",
        "pg_strcasecmp", "pg_strncasecmp", "pg_strtoint32",
        "pg_strtoint32_safe", "pg_tolower", "pg_toupper",
        "pg_unicode_to_server", "pg_unicode_to_server_noerror",
        "pg_utf_mblen", "pg_utf8_islegal",
        "pq_begintypsend", "pq_copymsgbytes", "pq_endtypsend",
        "pq_getmsgbytes", "pq_getmsgint", "pq_getmsgtext", "pq_sendtext",
        "psprintf", "pvsnprintf",
        // jsonpathexec_diff additions (p1-laneaa, adt/jsonpath_exec): the
        // exec oracle extends this family; generic-named exports that other
        // oracle families also vendor (float8in_internal in pg_float_io.c,
        // hash_any in pg_mac_io.c) or could plausibly grow get the same
        // jporcl_ prefix so each family keeps its OWN vendored copy.
        "float8in_internal", "hash_any", "hash_any_extended",
        "cstring_to_text", "cstring_to_text_with_len", "text_to_cstring",
        "varstr_cmp", "parse_bool", "parse_bool_with_len",
        "int4in", "int8in", "pg_ltoa", "pg_ultoa_n",
        "pg_strtoint64", "pg_strtoint64_safe", "qsort_arg",
        "RE_compile_and_cache", "RE_compile_and_execute",
        // p1-microbatch CI-build fix (2026-07-31): the jsonpath family's
        // vendored Spencer engine (csrc/jsonpath/regex/) exports the same
        // five entry points as the regexp family's engine (csrc/regexfam/,
        // which must keep the unprefixed names — pg_regexp_io.c calls them
        // directly). Linux ld hard-errors on the duplicate definitions and
        // every CI cluster fuzz build at the tip died (`cargo fuzz build` builds
        // ALL targets); macOS ld tolerated it, which is why local builds
        // passed. Same nm-sweep remedy as the wave-3 train sweep below.
        "pg_regcomp", "pg_regexec", "pg_regerror", "pg_regfree",
        "pg_reg_getcolor",
        "construct_array_builtin", "ArrayGetIntegerTypmods",
        "MemoryContextSwitchTo", "AllocSetContextCreate",
        "MemoryContextResetOnly", "MemoryContextDelete",
        "MemoryContextSetIdentifier", "MemoryContextSetParent",
        "CurrentMemoryContext", "TopMemoryContext",
        "ExecEvalExpr", "exprTypmod", "init_MultiFuncCall",
        "per_MultiFuncCall", "format_type_be", "pnstrdup",
        "pg_strncoll", "pg_server_to_any", "session_timezone",
        "parse_datetime", "JsonEncodeDateTime", "timestamp2tm", "j2date",
        "DetermineTimeZoneOffset", "AdjustTimeForTypmod",
        "AdjustTimestampForTypmod", "anytime_typmod_check",
        "anytimestamp_typmod_check", "date_cmp_timestamp_internal",
        "date_cmp_timestamptz_internal",
        "timestamp_cmp_timestamptz_internal",
        "hash_numeric", "hash_numeric_extended", "hashchar",
        "hashcharextended", "jsonb_in", "numeric_eq", "numeric_cmp",
        "int64_to_numeric",
        // wave-3 train sweep (2026-07-31): these exports of the jsonpath
        // family's minimal numeric extract and exec-env sentinels collided
        // with the numericfam oracle (p1-laneu) and the dtio oracle
        // (p1-lanel2) once all three lanes shared one binary — dtio calls
        // were binding jsonpathexec's ABORTING datetime-carve sentinel
        // stubs. Every duplicate found by the nm sweep gets the prefix.
        "float4_numeric", "float8_numeric", "int2_numeric", "int4_numeric",
        "int8_numeric", "numeric_abs", "numeric_add_opt_error",
        "numeric_ceil", "numeric_div_opt_error", "numeric_floor",
        "numeric_int4_opt_error", "numeric_int8_opt_error",
        "numeric_is_inf", "numeric_is_nan", "numeric_mod_opt_error",
        "numeric_mul_opt_error", "numeric_sub_opt_error", "numeric_trunc",
        "numerictypmodin",
        "date_timestamptz", "timestamp_date", "timestamp_time",
        "timestamptz_date", "timestamptz_time", "timestamptz_timetz",
    ];
    let mut jsonpath = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        jsonpath.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    for s in JSONPATH_SHARED_SYMS {
        jsonpath.define(s, format!("jporcl_{s}").as_str());
    }
    for f in [
        "jsonpath.c", "jsonpath_gram.c", "jsonpath_scan.c",
        "pg_numeric_min.c", "pg_formatting_min.c", "pg_stringinfo.c",
        "pg_support_min.c", "pg_jsonpath_env.c",
        "regex/regcomp.c", "regex/regerror.c", "regex/regfree.c",
        // jsonpathexec_diff (p1-laneaa, adt/jsonpath_exec): verbatim
        // jsonpath_exec.c + jsonb_util.c + regexec.c, the pg_jsonb_min.c
        // extract file, qsort_arg, and the exec env/driver entries.
        "jsonpath_exec.c", "jsonb_util.c", "pg_jsonb_min.c",
        "pg_qsort_arg.c", "pg_jsonpath_exec_env.c",
        "regex/regexec.c",
    ] {
        jsonpath.file(format!("csrc/jsonpath/{f}"));
    }
    jsonpath
        .include("csrc/jsonpath/include")
        .include("csrc/jsonpath")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_jsonpath");
    // regexp_diff oracle (p1-laneag): the VERBATIM 18.3 Spencer regex engine
    // (csrc/regexfam/, own shim include tree — regcomp.c/regexec.c #include
    // their regc_*/rege_* siblings, so only the five top-level engine TUs
    // compile; regprefix.c added for the regexp_fixed_prefix arm) + the
    // regexp.c/varlena.c wrapper oracle pg_regexp_io.c.
    // Separate build so the regexfam shim postgres.h/mb tree never shadows
    // the main oracle lib's csrc/shim headers (and vice versa). Cross-family
    // mb-helper symbols carry a pg_regexfam_ prefix (see the shim headers) —
    // the same isolation the CRYPTO_SHARED_SYMS renames provide above.
    let mut regexfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        regexfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    regexfam
        // glibc gates locale_t and the isw*_l family behind _GNU_SOURCE
        // (regc_pg_locale.c references them in branches dead under the
        // pinned C collation, but they must compile); no-op on macOS.
        .define("_GNU_SOURCE", None)
        .file("csrc/regexfam/regcomp.c")
        .file("csrc/regexfam/regexec.c")
        .file("csrc/regexfam/regerror.c")
        .file("csrc/regexfam/regfree.c")
        .file("csrc/regexfam/regprefix.c")
        .file("csrc/regexfam/pg_regexfam_glue.c")
        .file("csrc/pg_regexp_io.c")
        .include("csrc/regexfam")
        .include("csrc/regexfam/include")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_regexfam");

    println!("cargo:rerun-if-changed=csrc");
    println!("cargo:rerun-if-env-changed=PGRUST_FUZZ_CSANCOV");
    // timestamp_diff oracle (p1-laney): verbatim 18.3 timestamp.c SQL-entry
    // bodies over the SAME vendored datetime.c/date.c core as p1-lanel's
    // datetime-family oracle (csrc/pg_datetime_verbatim.inc). Compiled as
    // its own TU with a tsdiff_impl_ prefix rename of every global (the
    // hashenc/cryptofam symbol-isolation precedent) so both lanes' oracles
    // keep their own vendored copies when they land together.
    const TSDIFF_SHARED_SYMS: &[&str] = &[
        "AdjustTimeForTypmod", "AdjustTimestampForTypmod", "anytime_typmod_check", "anytimestamp_typmod_check",
        "ClearTimeZoneAbbrevCache", "date_in", "date_out", "date_timestamptz",
        "date2isoweek", "date2isoyear", "date2isoyearday", "date2j",
        "date2timestamptz_opt_overflow", "DateOrder", "DateStyle", "DateTimeParseError",
        "day_tab", "days", "DecodeDateTime", "DecodeInterval",
        "DecodeISO8601Interval", "DecodeSpecial", "DecodeTimeOnly", "DecodeTimezone",
        "DecodeTimezoneAbbrev", "DecodeTimezoneName", "DecodeTimezoneNameToTz", "DecodeUnits",
        "DetermineTimeZoneAbbrevOffset", "DetermineTimeZoneOffset", "downcase_identifier", "downcase_truncate_identifier",
        "dt2time", "EncodeDateOnly", "EncodeDateTime", "EncodeInterval",
        "EncodeSpecialDate", "EncodeSpecialTimestamp", "EncodeTimeOnly", "extract_interval",
        "extract_timestamp", "extract_timestamptz", "float_time_overflows", "GetCurrentDateTime",
        "GetCurrentTimeUsec", "GetEpochTime", "int64_div_fast_to_numeric", "int64_to_numeric",
        "interval_avg", "interval_avg_combine", "interval_avg_deserialize", "interval_avg_serialize",
        "interval_div", "interval_in", "interval_justify_days", "interval_justify_hours",
        "interval_justify_interval", "interval_larger", "interval_mi", "interval_mul",
        "interval_out", "interval_part", "interval_pl", "interval_recv",
        "interval_scale", "interval_send", "interval_smaller", "interval_sum",
        "interval_time", "interval_trunc", "interval_um", "interval2itm",
        "IntervalStyle", "isoweek2date", "isoweek2j", "isoweekdate2date",
        "itm2interval", "itmin2interval", "j2date", "j2day",
        "make_date", "make_interval", "make_time", "make_timestamp",
        "make_timestamptz", "make_timestamptz_at_timezone", "months", "mul_d_interval",
        "numeric_add_opt_error", "numeric_div_opt_error", "numeric_sub_opt_error", "ParseDateTime",
        "pg_diff_datetime_tzset_name", "pg_diff_datetime_tzset_nongmt", "pg_dt_strlcpy", "pg_dt_tzset_name",
        "pg_dt_tzset_nongmt", "pg_get_timezone_offset", "pg_gmtime", "pg_interpret_timezone_abbrev",
        "pg_localtime", "pg_next_dst_boundary", "pg_timezone_abbrev_is_known", "pg_tolower",
        "pg_toupper", "pg_ts_numchain", "pg_tzset", "pg_ultoa_n",
        "pg_ultostr", "pg_ultostr_zeropad", "session_timezone", "strtoint",
        "TimestampDifference", "TimestampDifferenceMilliseconds",
        "TimestampDifferenceExceeds", "TimestampDifferenceExceedsSeconds",
        "time_in", "time_mi_interval", "time_out", "time_overflows",
        "time_part", "time_pl_interval", "time2tm", "timestamp_age",
        "timestamp_bin", "timestamp_cmp_internal", "timestamp_date", "timestamp_in",
        "timestamp_izone", "timestamp_larger", "timestamp_mi", "timestamp_mi_interval",
        "timestamp_out", "timestamp_part", "timestamp_pl_interval", "timestamp_recv",
        "timestamp_scale", "timestamp_send", "timestamp_smaller", "timestamp_time",
        "timestamp_trunc", "timestamp2timestamptz_opt_overflow", "timestamp2tm", "timestamptz_age",
        "timestamptz_bin", "timestamptz_date", "timestamptz_in", "timestamptz_izone",
        "timestamptz_mi_interval", "timestamptz_out", "timestamptz_part", "timestamptz_pl_interval",
        "timestamptz_recv", "timestamptz_send", "timestamptz_time", "timestamptz_timetz",
        "timestamptz_trunc", "timestamptz_trunc_zone", "timetz_in", "timetz_mi_interval",
        "timetz_out", "timetz_pl_interval", "timetz2tm", "tm2time",
        "tm2timestamp", "tm2timetz", "ValidateDate",
    ];
    let mut tsdiff = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        tsdiff.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    for s in TSDIFF_SHARED_SYMS {
        tsdiff.define(s, format!("tsdiff_impl_{s}").as_str());
    }
    tsdiff
        .file("csrc/pg_timestamp_io.c")
        .include("csrc/shim")
        .include("csrc/pgdt")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .compile("pg_difffuzz_tsdiff");

    // datetime_io_diff oracle (p1-lanel; gate cleared: all paste sites
    // filled, see csrc/pg_datetime_io_io.c header for provenance + pinned
    // environment). OWN TU since the lane merge: its verbatim `strtoint`
    // (src/common/string.c) collides with pg_strfam.c's copy inside one
    // cc::Build — GNU ld rejects the duplicate (CI cluster
    // fuzz-campaign-1785532267; Apple's ld resolved it silently). Renamed
    // per the hashenc/cryptofam symbol-isolation precedent so each lane
    // keeps its OWN vendored copy.
    let mut dtio = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        dtio.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    dtio.define("strtoint", "dtio_impl_strtoint");
    // wave-3 train sweep: dtio's vendored numutils/numeric extracts collide
    // with the main oracle lib's pg_numutils.o and numericfam's oracle under
    // one binary (GNU ld hard-errors; ld64 first-definition-wins silently).
    for s in [
        "pg_ultoa_n", "pg_ultostr", "pg_ultostr_zeropad",
        "int64_to_numeric", "int64_div_fast_to_numeric",
    ] {
        dtio.define(s, format!("dtio_impl_{s}").as_str());
    }
    dtio.file("csrc/pg_datetime_io_io.c")
        .include("csrc/shim")
        .include("csrc/pgdt")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .compile("pg_difffuzz_dtio");

    // datetime_closeout_diff oracle (p1-lanel2): extract_date /
    // time_part_common(retnumeric) / timetz_part_common / date skip-support
    // over the SAME vendored datetime.c/date.c core (pg_datetime_verbatim.inc)
    // as the lanel and laney oracles. Own TU, dtclo_impl_ prefix rename of
    // the same shared-global list (plus extract_date, which only this TU
    // vendors — renamed anyway so a future lane vendoring it cannot silently
    // cross-bind).
    let mut dtclo = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        dtclo.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // pg_tzset_offset: a laney-prelude global NOT in TSDIFF_SHARED_SYMS
    // (laney's TU is the lone definer on its branch); this TU copies that
    // prelude, so it must rename its copy — GNU ld rejected the duplicate
    // on the CI cluster (fuzz-campaign-1785532267; Apple's ld tolerated it
    // locally, the known Linux-only link trap).
    for s in TSDIFF_SHARED_SYMS.iter().chain(&["extract_date", "pg_tzset_offset"]) {
        dtclo.define(s, format!("dtclo_impl_{s}").as_str());
    }
    dtclo
        .file("csrc/pg_datetime_closeout.c")
        .include("csrc/shim")
        .include("csrc/pgdt")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .compile("pg_difffuzz_dtclo");

    // portfam_diff oracle (p1-microbatch PORTFAM: pg_bitutils, crc32c,
    // pgstrcasecmp, pg_path, bufmask). OWN cc::Build: its shim c.h /
    // postgres.h / postgres_fe.h tree (csrc/portfam/shim) must never shadow
    // — or be shadowed by — csrc/shim's, and its verbatim pg_bitutils.h /
    // pg_crc32c.h / storage headers are a full vendored include tree.
    // RESTORED (p1-mb-contribc, 2026-08-01): dropped by the p1-microbatch-1
    // union merge together with the tzfam/miscfam/netfam/libfam
    // registrations above — portfam_diff could not link at main.
    //
    // SYMBOL ISOLATION: several oracle families already vendor pg_crc.c,
    // pg_crc32c_sb8.c and friends (hashenc, cryptofam). Every extern this
    // family's TUs export is renamed portfam_* at compile time so the
    // duplicate definitions never cross-bind under one binary (the Linux
    // GNU-ld hard-error class that Apple ld64 silently tolerates locally).
    // strlcpy is renamed too: the platform libc supplies one on macOS/BSD.
    const PORTFAM_SYMS: &[&str] = &[
        // pg_bitutils.c / pg_popcount_aarch64.c
        "pg_leftmost_one_pos", "pg_rightmost_one_pos", "pg_number_of_ones",
        "pg_popcount32", "pg_popcount64", "pg_popcount_optimized",
        "pg_popcount_masked_optimized",
        // pg_crc32c_sb8.c / pg_crc.c
        "pg_comp_crc32c_sb8", "pg_crc32_table", "crc32_bytea", "crc32c_bytea",
        // pgstrcasecmp.c
        "pg_strcasecmp", "pg_strncasecmp", "pg_toupper", "pg_tolower",
        "pg_ascii_toupper", "pg_ascii_tolower",
        // (strlcpy is renamed inside csrc/portfam/shim/c.h instead — a
        // command-line -D loses to Apple <string.h>'s _FORTIFY re-#define.)
        // path.c
        "has_drive_prefix", "first_dir_separator", "first_path_var_separator",
        "last_dir_separator", "make_native_path", "cleanup_path",
        "join_path_components", "canonicalize_path", "canonicalize_path_enc",
        "path_contains_parent_reference", "path_is_relative_and_below_cwd",
        "path_is_prefix_of_path", "get_progname", "make_absolute_path",
        "get_share_path", "get_etc_path", "get_include_path",
        "get_pkginclude_path", "get_includeserver_path", "get_lib_path",
        "get_pkglib_path", "get_locale_path", "get_doc_path", "get_html_path",
        "get_man_path", "get_home_path", "get_parent_directory",
        // bufmask.c
        "mask_page_lsn_and_checksum", "mask_page_hint_bits",
        "mask_unused_space", "mask_lp_flags", "mask_page_content",
    ];
    // radixtree_diff oracle (p1-mb-lib) compiles in its OWN cc::Build:
    // its shim utils/memutils.h (context-aware) must not be shadowed by
    // csrc/libfam/include's macro-based memutils.h shim (both are found as
    // "utils/memutils.h"), and its headers must not leak into other TUs.
    let mut radixtree = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        radixtree.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    radixtree
        .file("csrc/pg_radixtree_io.c")
        .include("csrc/shim")
        .include("csrc/radixtree/include")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .warnings(false)
        .compile("pg_difffuzz_radixtree");

    let mut portfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        portfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    for s in PORTFAM_SYMS {
        portfam.define(s, format!("portfam_{s}").as_str());
    }
    for f in [
        "pg_portfam_io.c",
        "portfam/pg_bitutils.c",
        "portfam/pg_popcount_aarch64.c",
        "portfam/pg_crc32c_sb8.c",
        "portfam/pg_crc.c",
        "portfam/pgstrcasecmp.c",
        "portfam/path.c",
        "portfam/strlcpy.c",
        "portfam/bufmask.c",
    ] {
        portfam.file(format!("csrc/{f}"));
    }
    portfam
        // path.c's FRONTEND arm: identical pure-path logic; the arms that
        // differ live only in make_absolute_path's OOM/cwd error legs, which
        // the driver never calls (cwd-reading carve).
        .define("FRONTEND", None)
        .include("csrc/portfam/shim")
        .include("csrc/portfam/include")
        .include("csrc/portfam")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-function")
        .compile("pg_difffuzz_portfam");
    println!("cargo:rerun-if-changed=csrc/pg_portfam_io.c");
    println!("cargo:rerun-if-changed=csrc/portfam");
    // contribb_diff oracle (p1-mb-contribb): verbatim 18.3 contrib/seg +
    // contrib/cube non-GiST bodies (csrc/pg_contribb_io.c) plus the
    // GENERATED flex/bison parser TUs committed under csrc/contribb/
    // (bison 2.3 / flex 2.6.4 over the verbatim vendored grammars; see the
    // provenance banners). Own cc::Build: the family needs its own shim
    // include tree (csrc/contribb/include postgres.h etc.), which must not
    // leak into the main build's files. float4in/float8in/float8out_internal
    // resolve against pg_float_io.c in the main build (extern, one verbatim
    // definition per symbol).
    //
    // -funsigned-char: plain-char signedness is implementation-defined and
    // PG inherits the platform default; the campaign's oracle of record is
    // the CI cluster Linux/aarch64 build where char is UNSIGNED (the pgrust port
    // also chose u8 for SEG's sigd/ext bytes). Without the pin a macOS
    // (signed-char) local build of seg_cmp's sigd comparisons diverges from
    // the ratified oracle for sigd >= 128.
    let mut contribb = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        contribb.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // -O2 PIN (found by contribb_diff, 2026-08-01): under cargo-fuzz the
    // profile opt-level is 3 and clang -O3 vectorizes cube.c's
    // distance loops in a way that changes distance_1D's NaN semantics
    // (scalar IEEE: every comparison with a NaN coordinate is false ->
    // 0.0 contribution; the -O3 code propagates the NaN payload instead —
    // witness: 53-dim point with coord0 = 0xFFF70000000000FC, C gave
    // 0xFFFF0000000000FC where -O1/-O2 and Rust give +Inf). Production
    // PostgreSQL builds at -O2, so the -O2 behavior IS the oracle.
    contribb.opt_level(2);
    contribb
        .file("csrc/pg_contribb_io.c")
        .file("csrc/contribb/segparse.c")
        .file("csrc/contribb/segscan.c")
        .file("csrc/contribb/cubeparse.c")
        .file("csrc/contribb/cubescan.c")
        .include("csrc/contribb/include")
        .include("csrc/contribb")
        .flag("-funsigned-char")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-ffp-contract=off")
        .compile("pg_difffuzz_contribb");
    println!("cargo:rerun-if-changed=csrc/pg_contribb_io.c");
    println!("cargo:rerun-if-changed=csrc/contribb");

    // nodesfam_diff oracle (p1-nodes): verbatim 18.3 node walkers —
    // outfuncs.c / readfuncs.c / copyfuncs.c (+ equalfuncs.c as the
    // C-side structural-equality witness) with read/value/list/bitmapset,
    // datum.c, stack_depth.c and the generated node-support files
    // (gen_node_support.pl et al., committed under csrc/nodesfam/gen with
    // provenance; csrc/nodesfam/assemble.sh re-vendors the whole family).
    // Own cc::Build: the family vendors the real src/include closure
    // (csrc/nodesfam/include) + a fabricated pg_config shim
    // (csrc/nodesfam/shim), which must never leak into other families.
    //
    // SYMBOL ISOLATION: every extern this family exports (207 symbols:
    // bms_*, list machinery, stringToNode/nodeToString/copyObjectImpl/
    // equal, palloc shims, pg_snprintf, pg_bitutils, ...) is renamed
    // ndf_* at compile time from csrc/nodesfam/rename_syms.txt — several
    // (pg_popcount64, the stringinfo layer, palloc) already have verbatim
    // definitions in other family archives, and NETFAM already owns the
    // `nf_`/`pg_nf_` prefixes (its stringinfo copies collided on the first
    // build: macOS ld64 only WARNS on duplicate symbols, GNU ld on the CI cluster
    // hard-errors, so this had to be caught before submitting). Driver
    // entries use the unique pg_ndf_ prefix.
    let mut nodesfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        nodesfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    let rename_syms = std::fs::read_to_string("csrc/nodesfam/rename_syms.txt")
        .expect("csrc/nodesfam/rename_syms.txt");
    for s in rename_syms.lines().map(str::trim).filter(|s| !s.is_empty()) {
        nodesfam.define(s, format!("ndf_{s}").as_str());
    }
    // -O2 PIN: production PostgreSQL builds at -O2; keep the oracle there
    // (same rationale as the contribb pin above).
    nodesfam.opt_level(2);
    nodesfam.file("csrc/pg_nodesfam_io.c");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    for f in std::fs::read_dir("csrc/nodesfam/src").expect("csrc/nodesfam/src") {
        let p = f.expect("dirent").path();
        if p.extension().is_some_and(|e| e == "c") {
            // strlcpy/strlcat: only where libc lacks them (glibc < 2.38);
            // on macOS the SDK both declares and fortify-macroizes them.
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            if (name == "strlcpy.c" || name == "strlcat.c") && target_os != "linux" {
                continue;
            }
            nodesfam.file(p);
        }
    }
    nodesfam
        // -funsigned-char PIN (same class contribb pinned): outfuncs' datum
        // writer prints each byval byte as `(int) *s++` off a `char *`, so
        // plain-char SIGNEDNESS decides whether byte 0xFF prints `-1` or
        // `255`. The campaign's oracle of record is the CI cluster Linux/aarch64
        // build where char is UNSIGNED — which is also what the pgrust port
        // (u8) produces. Without the pin, a macOS (signed-char) local oracle
        // reports a false OUT-TEXT divergence on every high datum byte.
        .flag("-funsigned-char")
        .include("csrc/nodesfam/shim")
        .include("csrc/nodesfam/gen")
        .include("csrc/nodesfam/include")
        .include("csrc/nodesfam/src")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-function")
        .compile("pg_difffuzz_nodesfam");
    println!("cargo:rerun-if-changed=csrc/pg_nodesfam_io.c");
    println!("cargo:rerun-if-changed=csrc/nodesfam");
}
