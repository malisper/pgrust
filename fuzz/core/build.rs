// Compile the vendored PostgreSQL C oracles for the differential fuzz
// targets (csrc/README-style provenance headers in each file). Same cc
// pattern as proofs/brin-minmax/build.rs — plain native compile; there is
// no Kani arm here (the fuzz workspace never builds under cargo-kani).
// ORACLE-ASAN OPT-IN (task #143, Michael-approved, 2026-08-03): arm
// -fsanitize=address (+ frame pointers + debug info for symbolized reports)
// on selected C ORACLE cc::Builds. Without instrumenting the C TUs, ASan
// only intercepts malloc, so OOB/UAF performed BY the vendored C surfaces
// (if at all) as value divergences or garbage-PC faults — the spellfam
// rationale, extended family-by-family (wcharfam + regexfam first; the
// tree-wide csrc/ pass is task #84). STRICTLY OPT-IN via PGRUST_ORACLE_ASAN=1
// — or its fleet alias PGRUST_FUZZ_CASAN=1, the name the fleet runner's
// --casan path exports (run-fuzz-campaign.sh; the runner has no generic env
// passthrough, so the enablement accepts the established fleet interface
// rather than forking 8 fleet branches; task #143 addendum) — AND a
// cargo-fuzz build (CARGO_CFG_FUZZING): the ASan RUNTIME comes from
// cargo-fuzz's Rust-side -Zsanitizer=address link, so a plain `cargo test`
// build must never gain objects whose __asan_* references nothing resolves.
// With both envs unset the default campaign build is UNCHANGED.
// SIDE-CHANNEL DISCIPLINE (asan-is-side-channel ruling): an ASan abort is a
// C-oracle memory FINDING — the fleet runner counts sanitizer artifacts
// separately from divergences; it never becomes a differential verdict.
fn oracle_asan_armed() -> bool {
    let opted_in = ["PGRUST_ORACLE_ASAN", "PGRUST_FUZZ_CASAN"]
        .iter()
        .any(|name| std::env::var_os(name).is_some_and(|v| v == "1"));
    opted_in && std::env::var_os("CARGO_CFG_FUZZING").is_some()
}

// -fsanitize=address for a C oracle TU that will link against the ASan
// runtime cargo-fuzz's Rust side brings (-Zsanitizer=address ->
// librustc_rt.asan). macOS quirk (task #143, observed Apple clang 17.0.0 /
// clang-1700 vs rustc nightly-2026-07-17): Apple clang's ASan module ctor
// references ___asan_version_mismatch_check_apple_clang_1700, which rustc's
// bundled UPSTREAM-LLVM runtime does not export (it has ..._v8 only), so the
// mixed link dies with undefined symbols — this broke EVERY macOS cargo-fuzz
// link of decoder_fuzz once the wcharfam/spellfam TUs were armed. Dropping
// the guard (-mllvm -asan-guard-against-version-mismatch=0) makes the mix
// link; the instrumentation<->runtime interface is version-stable in
// practice, and the lane's must-fail control (planted heap OOB caught with a
// full report) is the proof it actually catches. Linux/fleet builds (clang
// proper) are unaffected: their objects reference ..._v8 directly.
fn asan_c_flags(b: &mut cc::Build) {
    b.flag("-fsanitize=address");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        b.flag("-mllvm").flag("-asan-guard-against-version-mismatch=0");
    }
}

fn arm_oracle_asan(b: &mut cc::Build) {
    asan_c_flags(b);
    b.flag("-fno-omit-frame-pointer").flag("-g");
}

fn main() {
    println!("cargo:rerun-if-env-changed=PGRUST_ORACLE_ASAN");
    println!("cargo:rerun-if-env-changed=PGRUST_FUZZ_CASAN");
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
        // COMPILE GATE (timeline_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_timeline_io.c is filled
        // with verbatim vendored C (README-TODO-timeline_diff.md step 1).
        // .file("csrc/pg_timeline_io.c")
        // guc_file_diff oracle compiles in its OWN cc::Build below
        // (pg_difffuzz_gucfile): family-local csrc/gucfile shim tree.
        // spgbox_diff oracle: verbatim geo_spgist.c + spgproc.c + geo_ops.c
        // box relations + pg_hypot + sort_template pg_qsort instantiation
        // (provenance in csrc/pg_spgbox_io.c header).
        .file("csrc/pg_spgbox_io.c")
        // guc_units_diff oracle: verbatim guc.c unit tables + parse/convert
        // functions (provenance in csrc/pg_guc_units_io.c header).
        .file("csrc/pg_guc_units_io.c")
        // spgquad_diff oracle: verbatim spgquadtreeproc.c + spgproc.c +
        // geo_ops.c point relations + pg_hypot (provenance in
        // csrc/pg_spgquad_io.c header).
        .file("csrc/pg_spgquad_io.c")
        // tsm_system_time_diff oracle (assembled by csrc/gen/assemble_tsmtime.sh).
        .file("csrc/pg_tsm_system_time_io.c")
        // tablesample_diff oracle (assembled by csrc/gen/assemble_tsmpl.sh).
        .file("csrc/pg_tablesample_io.c")
        // instrument_diff oracle (assembled by csrc/gen/assemble_instrbe.sh).
        .file("csrc/pg_instrbe_io.c")
        // COMPILE GATE (tsm_system_rows_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_tsm_system_rows_io.c is filled
        // with verbatim vendored C (README-TODO-tsm_system_rows_diff.md step 1).
        .file("csrc/pg_tsm_system_rows_io.c")
        // COMPILE GATE (define_diff, scaffold.py): uncomment ONLY after every
        // SCAFFOLD-TODO #error paste site in csrc/pg_define_io.c is filled
        // with verbatim vendored C (README-TODO-define_diff.md step 1).
        .file("csrc/pg_define_io.c")
        // oracle-serialization holder check (fuzz plumbing; see the
        // file header + scripts/lint-oracle-serial.py)
        .file("csrc/pg_oracle_guard.c")
        // trgm_diff oracle (p1-trgm): verbatim 18.3 trgm_op.c + locale/case
        // units (see pg_trgm_io.c header for provenance + shims); mblen +
        // pg_utf_mblen resolve to the wfam_ copies in pg_wcharfam.c, pg_u_*
        // to tablesfam's unicode_category.c, unicode_strlower to the
        // whole-file vendored csrc/trgmfam/unicode_case.c below.
        .file("csrc/pg_trgm_io.c")
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
        // spellfam_diff oracle (p1-spell) compiles in its OWN cc::Build below
        // (pg_difffuzz_spellfam): it is built WITH -fsanitize=address so the
        // vendored spell.c's memory-safety defects are ATTRIBUTED instead of
        // surfacing as garbage-PC BUS faults (see that build for the rationale).
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
        // stub:snapshot + stub:encoding constructed-state builder shims
        // (fuzz/stub-constructed; both-sides contracts in
        // src/stub_snapshot.rs / src/stub_encoding.rs)
        .file("csrc/pg_stub_snapshot.c")
        .file("csrc/pg_stub_encoding.c")
        // stub:syscache-row constructed-state builder shim (supplied catalog
        // rows + SearchSysCacheN interception + verbatim lsyscache
        // consumers; contract in src/stub_syscache.rs / fuzz/STUBS.md)
        .file("csrc/pg_stub_syscache.c")
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
        // varlena campaign oracles (lane p1-lanes, 2026-07-31): all three
        // scaffold paste sites filled with verbatim 18.3 C — gates open.
        .file("csrc/pg_vltext_io.c")
        .file("csrc/pg_vlbytea_io.c")
        .file("csrc/pg_vlmisc_io.c")
        // quote_diff oracle (p1-laner): verbatim 18.3 quote.c core +
        // ruleutils.c quote_identifier; keyword tables extern'd from
        // pg_enc_tables.c / tablesfam (see pg_quote_io.c header).
        .file("csrc/pg_quote_io.c")
        // fmt_dch_diff + fmt_num_diff oracle (p1-lanek): verbatim formatting.c
        // DCH+NUM+engine slice, single TU (the NUM SQL entries call the static
        // NUM_processor/NUM_cache there); csrc/pg_fmt_num_io.c is intentionally
        // NOT compiled — see fuzz/core/src/fmt_num_diff.rs header.
        .file("csrc/pg_fmt_dch_io.c")
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
        // scalarxid_diff + snapio_diff oracles (p1-lanep scalar/xid-tid batch):
        .file("csrc/pg_snapio_io.c")
        .file("csrc/pg_scalarxid_io.c")
        .file("csrc/pg_scalarxid_datum.c")
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
        .file("csrc/pg_int_io.c")
        .file("csrc/pg_network_io.c")
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
        // fmt_dch numeric-deps rename (p1-queryjumble fleet-link fix,
        // 2026-08-01): csrc/pg_numeric_deps_18_3.inc (verbatim numeric.c
        // extract #included by pg_fmt_dch_io.c, landed 20f9593a88) exports
        // these unprefixed, colliding with the numericfam family oracle's
        // verbatim numeric.c under one bin — ld.lld duplicate-symbol hard
        // error on every fleet fuzz build (first witnessed by the
        // queryjumble_diff campaign; macOS ld tolerated it by member-pull
        // luck). Same wave-3 rename pattern as the hashfn.c cases.
        .define("numeric_in", "fmtdch_numeric_in")
        .define("numeric_out", "fmtdch_numeric_out")
        .define("numeric_out_sci", "fmtdch_numeric_out_sci")
        .define("numeric_round", "fmtdch_numeric_round")
        .define("numeric_mul", "fmtdch_numeric_mul")
        .define("numeric_mul_opt_error", "fmtdch_numeric_mul_opt_error")
        .define("numeric_power", "fmtdch_numeric_power")
        .define("numeric_int4_opt_error", "fmtdch_numeric_int4_opt_error")
        .define("int64_to_numeric", "fmtdch_int64_to_numeric")
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_oracle");

    // tsvec oracle family (p1-laneae, tsvector_core_diff + tsrank_diff):
    // OWN cc::Build (landing-train reconcile): on the lane the shared build
    // had few include dirs, but main's shared build now carries csrc/pgdt +
    // csrc first, whose fmgr.h/postgres.h collide with the tsvec header web
    // (redefinition of varlena/int32/Datum). The tsvec TUs resolve their own
    // postgres.h/c.h same-directory (quote-include rule); pg_ts*_io.c pull
    // "tsvec/postgres.h" relative to csrc/.
    let mut tsvec = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        tsvec.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // FAMILY SYMBOL ISOLATION (central symfix lane, 2026-08-01): the tsvec
    // family landed with unprefixed verbatim-C exports colliding under GNU
    // ld with incumbent oracle TUs (pg_arrayutils_io.c: ArrayGetNItems/
    // ArrayGetNItemsSafe; pg_int_io.c/pg_fmt_dch_io.c: pq_getmsgint/
    // pg_mblen_cstr/pg_mblen_range). Apple ld64 only warns; ld.lld
    // hard-errors (gram_core job -2ab6-60592). Build-level macro renames
    // (jsonbfam precedent) — safe here because this cc::Build is the whole
    // family, so definitions in pg_ts*_io.c and uses in tsvec/*.c rename
    // consistently. tsvio_ = symbols defined by pg_tsvector_core_io.c,
    // tsrio_ = symbols defined by pg_tsrank_io.c. C bodies stay verbatim.
    for (s, r) in [
        ("pq_getmsgint", "tsvio_pq_getmsgint"),
        ("pg_mblen_cstr", "tsvio_pg_mblen_cstr"),
        ("pg_mblen_range", "tsvio_pg_mblen_range"),
        ("ArrayGetNItems", "tsrio_ArrayGetNItems"),
        ("ArrayGetNItemsSafe", "tsrio_ArrayGetNItemsSafe"),
        // oracle-integrity sweep (task #98): the family's verbatim
        // port/qsort.c + qsort_arg.c exported UNPREFIXED pg_qsort /
        // qsort_arg / pg_qsort_strcmp — the tidbitmap link-race class
        // (whichever archive the linker visits first silently supplies
        // every other family's sort). Rename family-wide; bodies stay
        // verbatim, tsvec/postgres.h's `#define qsort pg_qsort` chains
        // into the rename so every verbatim call site follows.
        ("pg_qsort", "tsv_pg_qsort"),
        ("qsort_arg", "tsv_qsort_arg"),
        ("pg_qsort_strcmp", "tsv_pg_qsort_strcmp"),
    ] {
        tsvec.define(s, r);
    }
    tsvec
        // VERBATIM 18.3 C under csrc/tsvec/ (tsvector.c, tsvector_parser.c,
        // tsvector_op.c with labeled carve blocks, tsrank.c byte-identical,
        // pg_qsort/qsort_arg for tie-order parity); runtime shims + driver
        // entries in pg_tsvector_core_io.c / pg_tsrank_io.c.
        .file("csrc/pg_tsvector_core_io.c")
        .file("csrc/pg_tsrank_io.c")
        .file("csrc/tsvec/tsvector.c")
        .file("csrc/tsvec/tsvector_parser.c")
        .file("csrc/tsvec/tsvector_op.c")
        .file("csrc/tsvec/tsrank.c")
        .file("csrc/tsvec/qsort.c")
        .file("csrc/tsvec/qsort_arg.c")
        .include("csrc/tsvec/include")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        // SANCOV ON THE C SIDE under cargo-fuzz: union coverage retention
        // (same rationale as the shared oracle build above).
        .flag_if_supported(
            if std::env::var_os("CARGO_CFG_FUZZING").is_some() {
                "-fsanitize=fuzzer-no-link"
            } else {
                "-fno-strict-aliasing"
            },
        )
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_tsvec");
    println!("cargo:rerun-if-changed=csrc/pg_tsvector_core_io.c");
    println!("cargo:rerun-if-changed=csrc/pg_tsrank_io.c");
    println!("cargo:rerun-if-changed=csrc/tsvec");

    // wcharfam oracle (p1-laneah): verbatim 18.3 wchar.c + encnames.c +
    // mbutils.c pure extracts, own include dir (its c.h shim must not leak
    // into sibling TUs); every extern symbol is macro-renamed wfam_* inside
    // pg_wcharfam.c itself, so no symbol-isolation defines are needed here.
    let mut wcharfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        wcharfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // ASan ON THIS SHARED TU (Michael's ruling 2026-08-01, blocker-TUs only —
    // NOT the deferred tree-wide pass, task #84). p1-spell's 10M floor dies on a
    // C-side wild control transfer (ASan SEGV, pc AND sp garbage, no artifact
    // writable); instrumenting p1-spell's own TU did not attribute it, which
    // localizes the offending write to a shared TU that spell.c calls — this
    // one first (every spell.c parser walk goes through wfam_pg_mblen* /
    // wfam_pg_mb2wchar_with_len). Gated on CARGO_CFG_FUZZING so only cargo-fuzz
    // builds (which link an ASan runtime) are affected; plain `cargo test` is
    // unchanged. SHARED-TU NOTICE: this TU is also linked by tzfam_diff,
    // wparser_diff, hstore_diff and the wcharfam target, so those oracles get
    // rebuilt with ASan too and may surface their OWN latent C-side memory bugs.
    // Their corpora are deliberately NOT replayed here (that is task #84).
    if std::env::var_os("CARGO_CFG_FUZZING").is_some() {
        asan_c_flags(&mut wcharfam);
    }
    // ORACLE-ASAN OPT-IN (task #143): frame pointers + debug info so the
    // (already-armed-under-fuzzing) ASan reports on this TU symbolize to
    // file:line. See the gate above main().
    if oracle_asan_armed() {
        arm_oracle_asan(&mut wcharfam);
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_jsonbfam");

    // ltree_diff oracle (p1-ltree-t74, task #74): ONE standalone TU that
    // #includes the banked verbatim 18.3 contrib/ltree family
    // (csrc/ltreefam/), every extern lt_/pg_lt_-prefixed via in-file
    // defines (hstorefam precedent; nm census in the lane report). Its
    // shim include dir provides EMPTY header names only — it must never
    // be added to the shared builder above (it would shadow other
    // oracles' real headers, which is exactly what broke libfam when the
    // first registration attempt did so). mblen resolves against the
    // verbatim wfam_ copies in pg_wcharfam.c; hash_any against the
    // pg_mac_io.c hashfn exports; provenance + shims in the TU header.
    let mut ltreefam = cc::Build::new();
    ltreefam.file("csrc/pg_ltreefam_io.c");
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        ltreefam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    ltreefam
        .include("csrc/ltreefam/shim")
        .include("csrc/ltreefam")
        .include("csrc")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-value")
        .flag_if_supported("-Wno-comment")
        .flag_if_supported("-Wno-sign-compare")
        .flag_if_supported("-Wno-unused-but-set-parameter")
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_ltreefam");
    println!("cargo:rerun-if-changed=csrc/pg_ltreefam_io.c");
    println!("cargo:rerun-if-changed=csrc/ltreefam");

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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_cryptofam");

    // crypt_be_diff oracle (p1-wavea): verbatim 18.3 backend/libpq/crypt.c
    // (minus get_role_password, census carve) + the auth-scram.c secret
    // entry points + whole verbatim saslprep.c/unicode_norm.c, assembled by
    // csrc/gen/assemble_cryptbe.sh. FRONTEND arms for the common/ files;
    // crypto/base64/scram primitives are NOT re-vendored — the
    // CRYPTO_SHARED_SYMS renames below bind this family's references to the
    // cryptofam_* copies compiled above (single copy per the
    // duplicate-export rule). Every symbol this family EXPORTS carries the
    // pg_cryptbe_ prefix so ld.lld never sees a duplicate.
    let mut cryptbe = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        cryptbe.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    for s in CRYPTO_SHARED_SYMS {
        // scram_build_secret returns a raw-malloc'd string (FRONTEND arm of
        // the vendored scram-common.c) that the verbatim crypt.c callers
        // drop; route this TU's references through the arena-tracking shim
        // in pg_cryptbe_io.c instead of the bare cryptofam_ symbol.
        if *s == "scram_build_secret" {
            cryptbe.define(s, "pg_cryptbe_scram_build_secret");
            continue;
        }
        cryptbe.define(s, format!("cryptofam_{s}").as_str());
    }
    for s in [
        "get_password_type", "encrypt_password", "md5_crypt_verify",
        "plain_crypt_verify", "parse_scram_secret", "pg_be_scram_build_secret",
        "scram_verify_plain_password", "pg_saslprep", "unicode_normalize",
        "unicode_is_normalized_quickcheck", "pg_utf_mblen", "pg_utf8_islegal",
        "pg_is_ascii",
    ] {
        cryptbe.define(s, format!("pg_cryptbe_{s}").as_str());
    }
    cryptbe
        .file("csrc/cryptbe/pg_cryptbe_io.c")
        .file("csrc/cryptbe/saslprep.c")
        .file("csrc/cryptbe/unicode_norm.c")
        .include("csrc/cryptbe/include")
        .include("csrc/cryptofam/shim_fe")
        .include("csrc/cryptofam/include")
        .define("FRONTEND", None)
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-Wno-unused-function")
        .compile("pg_difffuzz_cryptbe");

    // trgm_diff oracle (p1-trgm): the builtin-provider Unicode lowercase
    // engine, whole-file VERBATIM src/common/unicode_case.c @ 62d6c7d3df,
    // under its own shim include tree (csrc/trgmfam/include). Exported
    // symbols are trgmf_-prefixed via the shim postgres.h; pg_u_* resolve
    // at link to tablesfam's verbatim unicode_category.c.
    let mut trgmfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        trgmfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    trgmfam
        .file("csrc/trgmfam/unicode_case.c")
        .include("csrc/trgmfam/include")
        .warnings(false)
        .compile("pg_difffuzz_trgmfam");

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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_tablesfam");

    // tsq family (p1-laneaf, rewritten by task #135): tsquery_core_diff +
    // tsqrw_diff oracles. Verbatim backend TUs (csrc/tsq/*.c +
    // csrc/tsqrw/*.c, PostgreSQL 18.3 @ 62d6c7d3df) over the csrc/tsq/shim
    // environment; tsquery_util.c compiles EXACTLY ONCE here for both
    // targets. pg_crc32_table comes from the hashenc unit; pg_diff_errcode
    // from the main unit.
    let mut tsq = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        tsq.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // FAMILY SYMBOL ISOLATION (same convention as the tsvec unit below):
    // the tsq shim/TUs export upstream-named helpers that the tsvec family
    // (and pg_strncasecmp, the main oracle unit) also define. Apple ld64
    // silently binds to whichever archive it searches first, so WITHOUT
    // these renames tsqueryrecv's pq_getmsgstring resolved into the TSVEC
    // unit and its error path longjmp'd through the tsvec jmp_buf that was
    // never armed on this thread (SIGSEGV in _longjmp, found by the corpus
    // replay gate on macOS; ld.lld would have hard-errored instead).
    tsq.define("appendBinaryStringInfo", "tsq_appendBinaryStringInfo")
        .define("close_tsvector_parser", "tsq_close_tsvector_parser")
        .define("cstring_to_text_with_len", "tsq_cstring_to_text_with_len")
        .define("gettoken_tsvector", "tsq_gettoken_tsvector")
        .define("init_tsvector_parser", "tsq_init_tsvector_parser")
        .define("initStringInfo", "tsq_initStringInfo")
        .define("pq_begintypsend", "tsq_pq_begintypsend")
        .define("pq_endtypsend", "tsq_pq_endtypsend")
        .define("pq_getmsgstring", "tsq_pq_getmsgstring")
        .define("pq_sendint16", "tsq_pq_sendint16")
        .define("pq_sendint32", "tsq_pq_sendint32")
        .define("reset_tsvector_parser", "tsq_reset_tsvector_parser")
        .define("tsCompareString", "tsq_tsCompareString")
        .define("pg_strncasecmp", "tsq_pg_strncasecmp")
        // task #141: DATA symbol isolation. The shim's arena sentinel
        // (pg_tsq_shim.c `MemoryContext CurrentMemoryContext = NULL`) was
        // the LAST unprefixed CurrentMemoryContext definition beside the
        // trgm oracle's non-NULL copy — which one every importer bound
        // was link-composition dependent (the trgm landing gate caught
        // the NULL copy winning inside trgmrxfam's dynahash). Def and all
        // uses compile in this cc::Build, so the rename is self-contained.
        .define("CurrentMemoryContext", "tsq_CurrentMemoryContext");
    tsq.file("csrc/tsq/tsquery.c")
        // verbatim pg_qsort instantiation (tie-order parity: port.h maps
        // qsort -> pg_qsort in every backend TU; libc qsort tie order is
        // scalar-visible through QTNSort — see csrc/tsq/qsort.c header)
        .file("csrc/tsq/qsort.c")
        .file("csrc/tsq/tsquery_op.c")
        .file("csrc/tsq/tsquery_cleanup.c")
        .file("csrc/tsq/tsvector_parser.c")
        .file("csrc/tsq/ts_locale_excerpt.c")
        .file("csrc/tsq/tsvector_op_excerpt.c")
        .file("csrc/tsq/shim/pg_tsq_shim.c")
        .file("csrc/tsqrw/tsquery_util.c")
        .file("csrc/tsqrw/tsquery_rewrite.c")
        .file("csrc/pg_tsquery_core_io.c")
        .file("csrc/pg_tsqrw_io.c")
        // legacy-CRC table (pushValue valcrc / tsqueryrecv): the hashenc
        // unit's copy is symbol-prefixed (hashenc_impl_*), so this unit
        // carries the verbatim table under its upstream name (shim TU
        // header documents the extraction).
        .file("csrc/tsq/shim/pg_crc_table.c")
        .include("csrc/tsq/shim")
        .include("csrc/tsq/include")
        .include("csrc/tsqrw/include")
        // the guard header lives at csrc/ root (entry files are in csrc/).
        .include("csrc")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_tsq");

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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // oracle-integrity sweep (task #98): pg_qsort joins the family
        // rename set — csrc/jsonpath/pg_qsort.c instantiates the verbatim
        // port/qsort.c sort_template as (jporcl_)pg_qsort, and the
        // `qsort` define below routes the verbatim engine's qsort() calls
        // to it (port.h parity: the backend's qsort IS pg_qsort).
        "pg_qsort",
        "RE_compile_and_cache", "RE_compile_and_execute",
        // p1-microbatch fleet-build fix (2026-07-31): the jsonpath family's
        // vendored Spencer engine (csrc/jsonpath/regex/) exports the same
        // five entry points as the regexp family's engine (csrc/regexfam/).
        // Linux ld hard-errors on the duplicate definitions and
        // every fleet fuzz build at the tip died (`cargo fuzz build` builds
        // ALL targets); macOS ld tolerated it, which is why local builds
        // passed. Same nm-sweep remedy as the wave-3 train sweep below.
        // (p1-regexcore merge: the regexfam wrapper copy is now rxo_-renamed
        // — pg_regexp_io.c compiles in that same cc::Build so its calls
        // rename consistently; the PRISTINE names now belong to the
        // regex_diff engine copy under csrc/regexfam/vendor/.)
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
    // oracle-integrity sweep (task #98): the backend's qsort IS pg_qsort
    // (port.h line 478 `#define qsort pg_qsort`), and that define is part
    // of every verbatim body's real header closure. Without it the
    // family's vendored regex engine (regex/regc_nfa.c) bound LIBC qsort
    // — the spgkdtree wrong-oracle class. jporcl_pg_qsort is the verbatim
    // sort_template instantiation in csrc/jsonpath/pg_qsort.c.
    jsonpath.define("qsort", "jporcl_pg_qsort");
    for f in [
        "jsonpath.c", "jsonpath_gram.c", "jsonpath_scan.c",
        "pg_numeric_min.c", "pg_formatting_min.c", "pg_stringinfo.c",
        "pg_support_min.c", "pg_jsonpath_env.c",
        "regex/regcomp.c", "regex/regerror.c", "regex/regfree.c",
        // jsonpathexec_diff (p1-laneaa, adt/jsonpath_exec): verbatim
        // jsonpath_exec.c + jsonb_util.c + regexec.c, the pg_jsonb_min.c
        // extract file, qsort_arg, and the exec env/driver entries.
        "jsonpath_exec.c", "jsonb_util.c", "pg_jsonb_min.c",
        "pg_qsort_arg.c", "pg_qsort.c", "pg_jsonpath_exec_env.c",
        "regex/regexec.c",
    ] {
        jsonpath.file(format!("csrc/jsonpath/{f}"));
    }
    jsonpath
        // weak strlcpy compat (pre-2.38 glibc fleet pods; see the TU header)
        .file("csrc/pg_strlcpy_compat.c")
        .include("csrc/jsonpath/include")
        .include("csrc/jsonpath")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
    // spellfam_diff oracle (p1-spell): its OWN cc::Build so it can be
    // ASan-INSTRUMENTED. The rest of csrc/ is compiled without
    // -fsanitize=address, which means ASan cannot see out-of-bounds or
    // uninitialized accesses performed BY the vendored C — it only intercepts
    // malloc, so C-side memory-safety bugs show up (if at all) as value
    // divergences or as unattributable garbage-PC BUS faults. This lane hit
    // exactly that: verbatim spell.c carries three memory-safety defects
    // (upstream tasks #80 NULL AffixData slot, #81 uninitialized
    // char flag[BUFSIZ], #83 CompoundAffix terminator OOB write), and a 10M
    // floor run died with `ASan BUS, nested bug in the same thread` and no
    // artifact. Instrumenting this TU turns those into precise reports at their
    // true site. Rationale + campaign-wide implication:
    // scratchpad/needs-decode/TASK-83-SEVERITY.md (Q1).
    let mut spellfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        spellfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // Only under cargo-fuzz (which links the ASan runtime); a plain
    // `cargo test` build must not gain a sanitizer the harness cannot resolve.
    if std::env::var_os("CARGO_CFG_FUZZING").is_some() {
        asan_c_flags(&mut spellfam);
    }
    spellfam
        .file("csrc/pg_spellfam_io.c")
        .include("csrc")
        .warnings(false)
        .compile("pg_difffuzz_spellfam");

    let mut regexfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        regexfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // ORACLE-ASAN OPT-IN (task #143): see the gate above main().
    if oracle_asan_armed() {
        arm_oracle_asan(&mut regexfam);
    }
    // TWO ENGINE COPIES, ISOLATED (p1-regexcore merge): the regex_diff
    // oracle below links the byte-identical vendored engine under
    // csrc/regexfam/vendor/ with DIFFERENT shims (armed stack guard in
    // pg_regexfam.c vs this tree's static-inline `return false` in
    // include/miscadmin.h; real builtin-locale provider vs glue's aborting
    // stubs). Without renames the linker satisfies BOTH drivers from
    // whichever archive it opens first — regex_diff would silently run on
    // THIS unarmed copy (the shallow-plane class covcap-fleet documents) or
    // lld hard-errors on the duplicate (fleet job -1785599452 died on
    // pg_reg_getcolor after a hand-listed subset; macOS ld64's archive-pull
    // semantics mask it locally). So this family's copy of every
    // cross-archive-colliding symbol is renamed rxo_* (command-line defines
    // only; vendored files stay byte-identical). Colliding sets: the engine
    // publics (both engine copies; the jsonpath family's third copy is
    // jporcl_-renamed), and pg_newlocale_from_collation /
    // unicode_{upper,lower}case_simple (glue stubs vs
    // pg_difffuzz_regexlocale real definitions).
    for s in [
        // engine publics (also defined by the vendor/ copy below) — the
        // COMPLETE exported-global set of the shared engine TUs, verified
        // by nm-intersecting the built archives.
        "pg_regcomp", "pg_regexec", "pg_regerror", "pg_regfree", "pg_regprefix",
        "pg_reg_getcolor",
        "pg_set_regex_collation",
        // glue stubs (also defined for real by pg_difffuzz_regexlocale)
        "pg_newlocale_from_collation",
        "unicode_uppercase_simple", "unicode_lowercase_simple",
    ] {
        regexfam.define(s, format!("rxo_{s}").as_str());
    }
    regexfam
        // glibc gates locale_t and the isw*_l family behind _GNU_SOURCE
        // (regc_pg_locale.c references them in branches dead under the
        // pinned C collation, but they must compile); no-op on macOS.
        .define("_GNU_SOURCE", None)
        // oracle-integrity sweep (task #98): the backend's qsort IS
        // pg_qsort (port.h line 478); without this define the verbatim
        // engine bodies (regc_nfa.c sortins/sortouts/carc_cmp sorts)
        // bound LIBC qsort — the spgkdtree wrong-oracle class.
        // regexfam_pg_qsort = verbatim sort_template instantiation in
        // csrc/regexfam/pg_regexfam_qsort.c.
        .define("qsort", "regexfam_pg_qsort")
        .file("csrc/regexfam/pg_regexfam_qsort.c")
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_regexfam");

    // regex_diff oracle (p1-regexcore): verbatim REL_18_3 Spencer engine, a
    // byte-for-byte copy of bench/cref/regex_vendor plus regprefix.c /
    // regexport.c (fetched verbatim at the Stamp-18.3 upstream sha, see
    // csrc/regexfam/pg_regexfam.c header). Upstream TU structure kept:
    // regcomp.c (+regc_* includes), regexec.c (+rege_dfa.c), regfree.c,
    // regerror.c, regprefix.c, regexport.c each compile separately — the
    // compile and exec sides both define a `struct vars`. C collation only;
    // BUILTIN/LIBC/ICU locale arms compile against aborting vendor stubs.
    // This copy KEEPS the pristine pg_* symbol names (regexlocale's probe
    // and pg_regexfam.c's armed stack guard bind to them); the regexp_diff
    // family's copy above is the rxo_-renamed one, jsonpath's is jporcl_.
    let mut regexcorefam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        regexcorefam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // ORACLE-ASAN OPT-IN (task #143): see the gate above main().
    if oracle_asan_armed() {
        arm_oracle_asan(&mut regexcorefam);
    }
    regexcorefam
        // oracle-integrity sweep (task #98): the backend's qsort IS
        // pg_qsort (port.h line 478); the verbatim engine bodies
        // (regc_nfa.c sortins/sortouts via regcomp.c) must bind the
        // verbatim sort, not libc's. rxocore_pg_qsort = verbatim
        // sort_template instantiation in pg_regexfam_vendor_qsort.c.
        .define("qsort", "rxocore_pg_qsort")
        .file("csrc/regexfam/pg_regexfam_vendor_qsort.c")
        .file("csrc/regexfam/vendor/regcomp.c")
        .file("csrc/regexfam/vendor/regexec.c")
        .file("csrc/regexfam/vendor/regfree.c")
        .file("csrc/regexfam/vendor/regerror.c")
        .file("csrc/regexfam/vendor/regprefix.c")
        .file("csrc/regexfam/vendor/regexport.c")
        .file("csrc/regexfam/pg_regexfam.c")
        .include("csrc/regexfam/vendor")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_regexcorefam");

    // regex locale probe (p1-regexcore): standalone verbatim regc_pg_locale.c
    // with REAL builtin tables (recompiled unicode_category.c under the
    // lprobe_ prefix so tablesfam's unprefixed copy keeps its own object;
    // unicode_case.c is unique to this lib). See pg_regexfam_locale.c.
    let mut regexlocale = cc::Build::new();
    // ORACLE-ASAN OPT-IN (task #143): csrc/regexfam family member (the
    // engine's locale probe); see the gate above main().
    if oracle_asan_armed() {
        arm_oracle_asan(&mut regexlocale);
    }
    for s in [
        "pg_set_regex_collation",
        // pg_wchar_utf8.c export (also linked unprefixed in the tsvec
        // family archive — nm-sweep hit, ld.lld duplicate-hard-error class)
        "pg_utf_mblen",
        // unicode_category.c externs (also linked unprefixed in tablesfam)
        "unicode_category", "unicode_category_string", "unicode_category_abbrev",
        "pg_u_prop_alphabetic", "pg_u_prop_lowercase", "pg_u_prop_uppercase",
        "pg_u_prop_cased", "pg_u_prop_case_ignorable", "pg_u_prop_white_space",
        "pg_u_prop_hex_digit", "pg_u_prop_join_control",
        "pg_u_isdigit", "pg_u_isalpha", "pg_u_isalnum", "pg_u_isword",
        "pg_u_isupper", "pg_u_islower", "pg_u_isgraph", "pg_u_isprint",
        "pg_u_ispunct", "pg_u_isspace", "pg_u_isxdigit", "pg_u_isblank",
        "pg_u_iscntrl",
    ] {
        regexlocale.define(s, format!("lprobe_{s}").as_str());
    }
    regexlocale
        .file("csrc/regexfam/pg_regexfam_locale.c")
        .file("csrc/regexfam/localereal/unicode_case.c")
        .file("csrc/regexfam/localereal/pg_wchar_utf8.c")
        .file("csrc/tablesfam/unicode_category.c")
        .include("csrc/regexfam/localereal")
        .include("csrc/tablesfam/include")
        .include("csrc/regexfam/vendor")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_regexlocale");

    // trgm_diff arm 9 oracle (p1-trgm phase B): verbatim 18.3 trgm_regexp.c
    // (csrc/pg_trgm_regexp_io.c, generator-assembled) + WHOLE-FILE verbatim
    // order-bearing infrastructure under csrc/trgmrxfam/ (dynahash — its
    // hash_seq_search iteration order is semantics for packGraph; PG qsort —
    // penalty-comparator ties are real; list.c; hashfn tag_hash;
    // pg_bitutils; regexport). Own cc build: the trgmrxfam shim include
    // tree supplies postgres.h etc., and csrc/regexfam(+include) supplies
    // the regex engine headers (the engine OBJECTS come from the regexfam
    // build above; only pg_reg_* introspection compiles here). Every
    // vendored extern is renamed trgmrx_* (hashfn/list/dynahash/qsort
    // symbols collide with other oracle TUs' unprefixed vendored copies,
    // e.g. pg_hashfn_io.c's hash_bytes).
    let mut trgmrxfam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        trgmrxfam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    const TRGMRX_SHARED_SYMS: &[&str] = &[
        // dynahash.c
        "hash_create", "hash_destroy", "hash_stats", "hash_get_num_entries",
        "hash_search", "hash_search_with_hash_value", "get_hash_value",
        "hash_update_hash_key", "hash_seq_init",
        "hash_seq_init_with_hash_value", "hash_seq_search", "hash_seq_term",
        "hash_freeze", "hash_estimate_size", "hash_select_dirsize",
        "hash_get_shared_size", "AtEOXact_HashTables",
        "AtEOSubXact_HashTables", "string_hash", "tag_hash", "uint32_hash",
        // hashfn.c
        "hash_bytes", "hash_bytes_extended", "hash_bytes_uint32",
        "hash_bytes_uint32_extended", "bitmap_hash", "bitmap_match",
        // list.c (the subset list.c defines; unreferenced ones are inert)
        "lappend", "lappend_int", "lappend_oid", "lappend_xid",
        "list_concat", "list_concat_copy", "list_copy", "list_copy_head",
        "list_copy_tail", "list_copy_deep", "list_delete",
        "list_delete_ptr", "list_delete_int", "list_delete_oid",
        "list_delete_first", "list_delete_last", "list_delete_first_n",
        "list_delete_nth_cell", "list_delete_cell", "list_free",
        "list_free_deep", "list_insert_nth", "list_insert_nth_int",
        "list_insert_nth_oid", "list_member", "list_member_ptr",
        "list_member_int", "list_member_oid", "list_member_xid",
        "list_append_unique", "list_append_unique_ptr",
        "list_append_unique_int", "list_append_unique_oid",
        "list_concat_unique", "list_concat_unique_ptr",
        "list_concat_unique_int", "list_concat_unique_oid",
        "list_intersection", "list_intersection_int", "list_difference",
        "list_difference_ptr", "list_difference_int", "list_difference_oid",
        "list_union", "list_union_ptr", "list_union_int", "list_union_oid",
        "list_sort", "list_deduplicate_oid", "list_make1_impl",
        "list_make2_impl", "list_make3_impl", "list_make4_impl",
        "list_make5_impl", "new_head_cell", "new_tail_cell", "lcons",
        "lcons_int", "lcons_oid", "list_nth_cell",
        // pg_bitutils.c
        "pg_leftmost_one_pos", "pg_rightmost_one_pos", "pg_number_of_ones",
        "pg_popcount32_slow", "pg_popcount64_slow", "pg_popcount_slow",
        "pg_popcount_masked_slow", "pg_popcount32", "pg_popcount64",
        "pg_popcount_optimized", "pg_popcount_masked_optimized",
        // qsort.c (strlcpy is renamed at source level in the shim
        // postgres.h -- Apple's fortified string.h owns the bare name).
        // pg_qsort_strcmp: defined directly by the vendored qsort.c (not
        // derived from the pg_qsort token), so it needs its own rename —
        // an unprefixed export is a link race (task #98 guard).
        "pg_qsort", "pg_qsort_strcmp",
        // ---- task #141 (fleet blocker): DATA symbols + residual escapes.
        // The #98 sweep renamed only functions; these two shim GLOBALS
        // stayed unprefixed. pg_trgm_regexp_io.c defines them non-NULL
        // (&trgmrx_cxt_token) while the tsq shim defines an unprefixed
        // NULL CurrentMemoryContext — which definition dynahash.o binds
        // became LINK-COMPOSITION dependent, and beside main's archives
        // the NULL copy won: MemoryContextIsValid(CurrentDynaHashCxt)
        // SIGABRT in dynahash.c:293 before any family init ran.
        "CurrentMemoryContext", "TopMemoryContext",
        // shim stubs + shmem/size helpers (dynahash deps) + list.c
        // exports the #98 function sweep missed.
        "GetCurrentTransactionNestLevel", "copyObjectImpl", "equal",
        "add_size", "mul_size", "my_log2", "ShmemAllocNoError",
        "list_truncate", "list_int_cmp", "list_oid_cmp",
        // regexport.c introspection set: this family's vendored copy
        // collides with the regexcorefam archive's PRISTINE-named copy
        // (csrc/regexfam/vendor/ owns the pristine names — see the
        // jsonpath family comment above). Duplicate definitions across
        // archives are the ld.lld hard-error class (blocker #91): macOS
        // ld64 tolerates them, the fleet linker does not. Def and all
        // call sites compile in THIS cc::Build, so the -D rename stays
        // self-consistent; the family keeps its OWN vendored copy per
        // campaign doctrine.
        "pg_reg_getnumstates", "pg_reg_getinitialstate",
        "pg_reg_getfinalstate", "pg_reg_getnumoutarcs", "pg_reg_getoutarcs",
        "pg_reg_getnumcolors", "pg_reg_colorisbegin", "pg_reg_colorisend",
        "pg_reg_getnumcharacters", "pg_reg_getcharacters",
        // NOT renamed, deliberately: pg_regcomp/pg_regerror/pg_regfree
        // (single definer = regexcorefam's pristine engine, the intended
        // cross-archive binding — "the engine OBJECTS come from the
        // regexfam build"), the pg_diff_trgm_* driver API (Rust-facing,
        // already family-namespaced), and libc/bsearch (C-parity).
    ];
    for s in TRGMRX_SHARED_SYMS {
        trgmrxfam.define(s, format!("trgmrx_{s}").as_str());
    }
    trgmrxfam
        .define("_GNU_SOURCE", None)
        .file("csrc/trgmrxfam/dynahash.c")
        .file("csrc/trgmrxfam/list.c")
        .file("csrc/trgmrxfam/hashfn.c")
        .file("csrc/trgmrxfam/pg_bitutils.c")
        .file("csrc/trgmrxfam/qsort.c")
        .file("csrc/trgmrxfam/strlcpy.c")
        .file("csrc/trgmrxfam/regexport.c")
        .file("csrc/pg_trgm_regexp_io.c")
        .include("csrc/trgmrxfam/include")
        .include("csrc/regexfam")
        .include("csrc/regexfam/include")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_trgmrxfam");

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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_tsdiff");

    // datetime_io_diff oracle (p1-lanel; gate cleared: all paste sites
    // filled, see csrc/pg_datetime_io_io.c header for provenance + pinned
    // environment). OWN TU since the lane merge: its verbatim `strtoint`
    // (src/common/string.c) collides with pg_strfam.c's copy inside one
    // cc::Build — GNU ld rejects the duplicate (fleet
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
    // on the fleet (fuzz-campaign-1785532267; Apple's ld tolerated it
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
    // the fleet Linux/aarch64 build where char is UNSIGNED (the pgrust port
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
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
    // build: macOS ld64 only WARNS on duplicate symbols, GNU ld on the fleet
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
    // task #142: the vendored port/strlcpy.c + strlcat.c below (Linux-only
    // compiles) exported BARE strong strlcpy/strlcat — strlcpy duplicating
    // the jsonpath family's deliberate WEAK compat copy
    // (csrc/pg_strlcpy_compat.c) in a second archive. macOS ld64 masks the
    // class and the macOS census can't even see it (the files are skipped
    // off-Linux), but on the fleet linkers which copy every reference binds
    // is link-composition dependent. ndf_-rename them like every other
    // nodesfam export (rename_syms.txt machinery above); the definitions,
    // the lone call site (strerror.c) and port.h's !HAVE_DECL_STRLCPY
    // declarations all compile in THIS cc::Build, so the rename is
    // self-consistent. Linux-gated: on macOS the files are skipped (libc
    // owns the names) and Apple's fortified <string.h> re-#defines would
    // clobber a command-line -D anyway (the csrc/portfam precedent).
    if target_os == "linux" {
        for s in ["strlcpy", "strlcat"] {
            nodesfam.define(s, format!("ndf_{s}").as_str());
        }
    }
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
        // `255`. The campaign's oracle of record is the fleet Linux/aarch64
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
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_nodesfam");
    println!("cargo:rerun-if-changed=csrc/pg_nodesfam_io.c");
    println!("cargo:rerun-if-changed=csrc/nodesfam");

    // pgcryptofam_diff oracle (p1-pgcryptofam): verbatim 18.3
    // contrib/pgcrypto crypt()/gen_salt()/armor family — px-crypt.c
    // dispatch over crypt-{des,md5,blowfish,sha,gensalt}.c plus
    // pgp-armor.c — with its OWN copies of the src/common hash primitives
    // (md5/sha1/sha2/cryptohash/base64, copied from the already-verbatim
    // cryptofam tree) and the src/common string layer
    // (stringinfo/psprintf/string) + src/port
    // (snprintf/pgstrcasecmp/strlcpy). Own cc::Build: its shim postgres.h
    // (arena palloc + setjmp ereport channel recording sqlstate/elevel/
    // NOTICE text) must never shadow any other family's.
    //
    // SYMBOL ISOLATION: every extern the vendored TUs export is renamed
    // pgcryptofam_* at compile time (the hashenc/cryptofam precedent) —
    // the hash primitives and the stringinfo/printf layers all have
    // verbatim twins in other family archives, and GNU ld on the fleet
    // hard-errors on duplicates that Apple ld64 resolves silently.
    // Driver entries carry the unique pg_diff_pgcryptofam_ prefix in
    // source; shim plumbing is pgcryptofam_-named in source.
    const PGCRYPTOFAM_SYMS: &[&str] = &[
        // contrib/pgcrypto public surface
        "px_crypt", "px_gen_salt", "px_crypt_md5", "px_crypt_shacrypt",
        "px_crypt_des", "_crypt_blowfish_rn",
        "_crypt_gensalt_traditional_rn", "_crypt_gensalt_extended_rn",
        "_crypt_gensalt_md5_rn", "_crypt_gensalt_blowfish_rn",
        "_crypt_gensalt_sha256_rn", "_crypt_gensalt_sha512_rn",
        "pgp_armor_encode", "pgp_armor_decode", "pgp_extract_armor_headers",
        // px.c
        "px_THROW_ERROR", "px_strerror", "px_memset", "px_resolve_alias",
        "px_set_debug_handler", "px_debug", "px_find_combo",
        // px-hmac.c + scansup.c (digest()/hmac() arms)
        "px_find_hmac",
        "downcase_truncate_identifier", "downcase_identifier",
        "truncate_identifier", "scanner_isspace",
        "pg_database_encoding_max_length", "pg_mbcliplen",
        // shim-owned but PG-named (provider mocks + crypto-mode plumbing)
        "px_find_digest", "px_find_cipher", "CheckFIPSMode",
        "CheckBuiltinCryptoMode", "builtin_crypto_enabled",
        "pg_strong_random", "pg_mblen_cstr",
        // src/common/stringinfo.c
        "makeStringInfo", "makeStringInfoExt", "initStringInfo",
        "initStringInfoExt", "resetStringInfo", "appendStringInfo",
        "appendStringInfoVA", "appendStringInfoString",
        "appendStringInfoChar", "appendStringInfoSpaces",
        "appendBinaryStringInfo", "appendBinaryStringInfoNT",
        "enlargeStringInfo", "destroyStringInfo",
        // src/common/psprintf.c
        "psprintf", "pvsnprintf",
        // src/common/string.c
        "pg_str_endswith", "strtoint", "pg_clean_ascii", "pg_is_ascii",
        "pg_strip_crlf",
        // src/port/pgstrcasecmp.c
        "pg_strcasecmp", "pg_strncasecmp", "pg_toupper", "pg_tolower",
        "pg_ascii_toupper", "pg_ascii_tolower",
        // src/port/snprintf.c (strlcpy is renamed inside shim/postgres.h
        // instead — a command-line -D loses to Apple <string.h>'s
        // _FORTIFY re-#define, the csrc/portfam precedent)
        "pg_vsnprintf", "pg_snprintf", "pg_vsprintf", "pg_sprintf",
        "pg_vfprintf", "pg_fprintf", "pg_vprintf", "pg_printf",
        "pg_strfromd",
    ];
    let mut pgcryptofam = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        pgcryptofam.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    // CRYPTO_SHARED_SYMS covers this family's own copies of the verbatim
    // hash primitives (md5/sha/cryptohash/b64 + the hmac/scram/crc names
    // it does not compile — harmless extra defines).
    for s in CRYPTO_SHARED_SYMS.iter().chain(PGCRYPTOFAM_SYMS) {
        pgcryptofam.define(s, format!("pgcryptofam_{s}").as_str());
    }
    // -O2 PIN: production PostgreSQL builds at -O2; keep the oracle there
    // (same rationale as the contribb/nodesfam pins above).
    pgcryptofam.opt_level(2);
    for f in [
        // driver entries + harness plumbing
        "pg_diff_pgcryptofam.c",
        "pgcryptofam_shim.c",
        // whole-TU verbatim inclusions exporting file statics
        "wrap_crypt_des.c",
        "wrap_crypt_md5.c",
        "wrap_crypt_blowfish.c",
        "wrap_crypt_gensalt.c",
        // verbatim TUs compiled directly
        "vendor/px-crypt.c",
        "vendor/crypt-sha.c",
        "vendor/pgp-armor.c",
        "vendor/px.c",
        "vendor/px-hmac.c",
        "vendor/scansup.c",
        "vendor/stringinfo.c",
        "vendor/psprintf.c",
        "vendor/string.c",
        "vendor/snprintf.c",
        "vendor/pgstrcasecmp.c",
        "vendor/strlcpy.c",
        "vendor/md5.c",
        "vendor/md5_common.c",
        "vendor/sha1.c",
        "vendor/sha2.c",
        "vendor/cryptohash.c",
        "vendor/base64.c",
    ] {
        pgcryptofam.file(format!("csrc/pgcryptofam/{f}"));
    }
    pgcryptofam
        // -funsigned-char PIN (contribb/nodesfam class): plain-char
        // signedness is implementation-defined; the oracle of record is
        // the fleet Linux/aarch64 build where char is UNSIGNED. Without
        // it a macOS (signed-char) local build diverges on salt/password
        // bytes >= 0x80 (crypt-des ascii_to_bin comparisons, blowfish
        // BF_atoi64 indexing, crypt-sha signed-char promotion).
        .flag("-funsigned-char")
        .include("csrc/pgcryptofam/shim")
        .include("csrc/pgcryptofam/vendor/include")
        .include("csrc/pgcryptofam")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-ffp-contract=off")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-function")
        // Oracle-guard holder check (csrc/pg_oracle_guard.h): release-
        // effective in every build.rs compile of the oracle TUs.
        .define("PG_ORACLE_GUARD_CHECKS", None)
        .compile("pg_difffuzz_pgcryptofam");
    println!("cargo:rerun-if-changed=csrc/pgcryptofam");
    // guc_file_diff oracle (lane p1-wavef): verbatim 18.3 guc-file.l
    // (whole-file vendored copy under csrc/gucfile/) compiled from its
    // committed flex-2.6.4 output guc-file.c, plus the driver TU
    // pg_guc_file_io.c (verbatim guc_name_compare + ereport/arena shims).
    // OWN cc::Build: csrc/gucfile/postgres.h is a family-local shim tree
    // that must not leak into sibling TUs. Every export is gucf_/pg_gucf_
    // prefixed (or flex's own GUC_yy prefix) — see the shim header.
    let mut gucfile = cc::Build::new();
    if std::env::var_os("PGRUST_FUZZ_CSANCOV").is_some_and(|v| v == "1") {
        gucfile.flag("-fsanitize-coverage=inline-8bit-counters,pc-table");
    }
    gucfile
        .file("csrc/pg_guc_file_io.c")
        .file("csrc/gucfile/guc-file.c")
        // scanner is %option 8bit; fleet oracle of record is unsigned-char
        // aarch64 Linux — pin like contribb/nodesfam so a macOS signed-char
        // local build cannot manufacture false divergences on \200-\377.
        .flag("-funsigned-char")
        .include("csrc/gucfile")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-function")
        .compile("pg_difffuzz_gucfile");
    println!("cargo:rerun-if-changed=csrc/pg_guc_file_io.c");
    println!("cargo:rerun-if-changed=csrc/gucfile");

    enforce_sort_symbol_hygiene();
    enforce_cross_archive_definition_uniqueness();
}

/// Oracle-integrity guard (task #98): FAIL the build when any oracle
/// archive traffics in an UNPREFIXED sort/compare symbol.
///
/// Two defect classes this catches, both shipped this week:
///  - tidbitmap class: an archive EXPORTS an unprefixed qsort_arg /
///    pg_qsort — then LINK ORDER silently decides which implementation
///    every other family's verbatim body gets.
///  - spgkdtree class: a verbatim PG body left with an UNDEFINED
///    unprefixed qsort — the backend's qsort IS pg_qsort (port.h
///    `#define qsort pg_qsort`), so binding libc silently changes
///    tie order exactly where tie-order fidelity is load-bearing.
///
/// Runs on every profile (build.rs is release-effective by construction)
/// and fails LOUD if it cannot run (no fail-open).
fn enforce_sort_symbol_hygiene() {
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    // Unprefixed names banned as DEFINED externals (collision/link-race
    // class) and as UNDEFINED references (libc-binding class). bsearch is
    // banned only as a DEFINED external: PG itself calls libc bsearch, so
    // a `U bsearch` is C-parity, but an archive EXPORTING `bsearch` would
    // hijack it process-wide.
    const BAN_ALWAYS: &[&str] = &[
        "qsort", "qsort_arg", "qsort_interruptible", "pg_qsort",
        "pg_qsort_strcmp", "med3", "qsort_med3", "qsort_arg_med3",
        "pg_qsort_med3", "qsort_swap", "qsort_arg_swap", "pg_qsort_swap",
        "qsort_swapn", "qsort_arg_swapn", "pg_qsort_swapn",
        // oracle-sort re-sweep (task #98 follow-up): the rest of the
        // ordering-sensitive libc surface. qsort_r/mergesort/heapsort are
        // alternate libc sorts (never the backend's); strcoll/strcoll_l/
        // wcscoll/strxfrm are locale collation (the campaign compares in C
        // locale and PG backend text compare is varstr_cmp/pg_strcoll, never
        // a bare oracle-TU strcoll); strcasecmp/strncasecmp: the backend and
        // src/common call pg_strcasecmp/pg_strncasecmp (hand-rolled, ASCII,
        // locale-free) — grep of vendor backend+common+contrib shows ZERO
        // bare strcasecmp call sites, so any reference here is a shim
        // divergence, not C-parity.
        "qsort_r", "mergesort", "heapsort",
        "strcoll", "strcoll_l", "wcscoll", "strxfrm",
        "strcasecmp", "strncasecmp",
    ];
    const BAN_DEFINED_ONLY: &[&str] = &["bsearch"];

    let mut archives: Vec<std::path::PathBuf> = std::fs::read_dir(&out_dir)
        .expect("read OUT_DIR")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().is_some_and(|x| x == "a")
                && p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("libpg_difffuzz_"))
        })
        .collect();
    archives.sort();
    assert!(
        !archives.is_empty(),
        "sort-symbol guard: no libpg_difffuzz_*.a found in OUT_DIR — guard would be vacuous"
    );

    let nm = ["nm", "llvm-nm"]
        .iter()
        .find(|c| {
            std::process::Command::new(*c)
                .arg("--version")
                .output()
                .is_ok()
        })
        .expect("sort-symbol guard: neither `nm` nor `llvm-nm` available; refusing to fail open");

    let mut violations = Vec::new();
    for a in &archives {
        let out = std::process::Command::new(nm)
            .arg("-g") // external symbols only; TU-local (static) sorts are fine
            .arg("-o")
            .arg(a)
            .output()
            .unwrap_or_else(|e| panic!("sort-symbol guard: {nm} failed on {}: {e}", a.display()));
        assert!(
            out.status.success(),
            "sort-symbol guard: {nm} exited nonzero on {}",
            a.display()
        );
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            // formats: "<archive>:<obj>: <addr> <TYPE> <name>" or
            //          "<archive>:<obj>:          U <name>"  (GNU: "obj:...")
            let mut it = line.split_whitespace().rev();
            let (Some(name), Some(kind)) = (it.next(), it.next()) else { continue };
            if kind.len() != 1 {
                continue;
            }
            let bare = name.strip_prefix('_').unwrap_or(name); // Mach-O underscore
            let defined = kind != "U";
            let banned = BAN_ALWAYS.contains(&bare)
                || (defined && BAN_DEFINED_ONLY.contains(&bare));
            if banned {
                violations.push(format!(
                    "{}: {} `{}` ({})",
                    a.file_name().unwrap().to_string_lossy(),
                    if defined { "EXPORTS" } else { "REFERENCES undefined" },
                    bare,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "\n== oracle sort-symbol hygiene violations (task #98 guard) ==\n\
         Every oracle archive must keep sort/compare symbols family-prefixed;\n\
         an unprefixed export is a link race (tidbitmap class) and an\n\
         unprefixed undefined qsort binds LIBC where the backend means\n\
         pg_qsort (spgkdtree class). Offenders:\n{}\n",
        violations.join("\n")
    );
    // Evidence, not just a verdict (task #141): NAME the archives scanned
    // so a green line proves WHAT it covered — "30 archives clean" with no
    // roster cannot show that a given family was ever audited.
    println!(
        "cargo:warning=sort-symbol guard: {} archives clean: {}",
        archives.len(),
        archives
            .iter()
            .map(|a| a.file_name().unwrap().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(" ")
    );
}

/// Cross-archive duplicate-definition guard (task #142): FAIL the build
/// when the same global symbol is DEFINED in more than one oracle archive.
///
/// macOS ld64 resolves cross-archive duplicates silently (whichever member
/// the linker pulls first supplies every importer), so local builds always
/// link; GNU ld / ld.lld on the fleet hard-error on the strong-strong case
/// (blocker #91 class), and the member-pull cases silently bind EVERY
/// family to ONE copy — the tidbitmap/wave-3 first-definition-wins class
/// the per-family symbol-prefix renames above exist to prevent. Found live
/// by task #142: nodesfam's Linux-only vendored port/strlcpy.c exported a
/// bare strong `strlcpy` beside the jsonpath family's WEAK compat copy
/// (csrc/pg_strlcpy_compat.c) — invisible to any macOS nm census because
/// the nodesfam copy only compiles when target_os = linux.
///
/// Weak definitions COUNT as definitions here: Mach-O `nm -g` cannot
/// distinguish them from strong ones anyway, and two weak copies are still
/// a first-wins race. Anything intentionally defined in more than one
/// archive must be allowlisted WITH a justification.
///
/// Runs on every profile and fails LOUD if it cannot run (no fail-open),
/// same contract as enforce_sort_symbol_hygiene above.
fn enforce_cross_archive_definition_uniqueness() {
    // Intentional cross-archive duplicate definitions: (symbol, why).
    // NEAR-EMPTY is the healthy state — every family keeps its own prefixed
    // copies (CRYPTO_SHARED_SYMS et al.) precisely so that no two archives
    // export the same name. Cross-archive *imports* (e.g. cryptbe binding
    // cryptofam_* one-copy primitives, trgmrxfam binding regexcorefam's
    // pristine engine) are references, not definitions, and never trip
    // this guard.
    const ALLOWED_DUPLICATE_DEFS: &[(&str, &str)] = &[(
        // (task #143 addendum, 2026-08-03) COMPILER-EMITTED, Mach-O only:
        // LLVM's ASan pass (InstrumentGlobalsMachO) plants a COMMON-linkage
        // `__asan_globals_registered` bookkeeping global in EVERY
        // -fsanitize=address TU; nm reports it `C` and the linker merges
        // tentative definitions by design — identical runtime bookkeeping in
        // each copy, no first-definition-wins race. Every ASan-armed oracle
        // archive therefore defines it on macOS: wcharfam + spellfam on any
        // cargo-fuzz build (their standing CARGO_CFG_FUZZING arming — this
        // entry is what keeps plain macOS `cargo fuzz build` green), plus
        // the regexfam family under PGRUST_ORACLE_ASAN/PGRUST_FUZZ_CASAN.
        // The ELF path (fleet) uses start/stop section symbols instead and
        // never emits it, so this allowlists nothing on the fleet linker.
        "___asan_globals_registered",
        "ASan Mach-O common-linkage bookkeeping global, one per instrumented TU",
    )];

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let mut archives: Vec<std::path::PathBuf> = std::fs::read_dir(&out_dir)
        .expect("read OUT_DIR")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().is_some_and(|x| x == "a")
                && p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("libpg_difffuzz_"))
        })
        .collect();
    archives.sort();
    assert!(
        !archives.is_empty(),
        "duplicate-definition guard: no libpg_difffuzz_*.a found in OUT_DIR — guard would be vacuous"
    );

    let nm = ["nm", "llvm-nm"]
        .iter()
        .find(|c| {
            std::process::Command::new(*c)
                .arg("--version")
                .output()
                .is_ok()
        })
        .expect("duplicate-definition guard: neither `nm` nor `llvm-nm` available; refusing to fail open");

    // Mach-O prefixes EVERY C symbol with '_'; ELF prefixes none (though C
    // identifiers like _crypt_blowfish_rn may legitimately START with one),
    // so strip exactly one leading underscore only for Apple targets.
    let apple = std::env::var("CARGO_CFG_TARGET_VENDOR").as_deref() == Ok("apple");

    // symbol -> archives defining it (per-archive dedup: a symbol defined
    // by two objects of the SAME archive is the linker's own intra-archive
    // problem, not the cross-archive race this guard bans).
    let mut definers: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for a in &archives {
        let out = std::process::Command::new(nm)
            .arg("-g") // external symbols only
            .arg("-o")
            .arg(a)
            .output()
            .unwrap_or_else(|e| {
                panic!("duplicate-definition guard: {nm} failed on {}: {e}", a.display())
            });
        assert!(
            out.status.success(),
            "duplicate-definition guard: {nm} exited nonzero on {}",
            a.display()
        );
        let archive_name = a.file_name().unwrap().to_string_lossy().into_owned();
        let mut defined = std::collections::BTreeSet::new();
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            // formats: "<archive>:<obj>: <addr> <TYPE> <name>" or
            //          "<archive>:<obj>:          U <name>"
            let mut it = line.split_whitespace().rev();
            let (Some(name), Some(kind)) = (it.next(), it.next()) else { continue };
            if kind.len() != 1 {
                continue;
            }
            // 'U' = undefined reference; 'w'/'v' = weak UNDEFINED (a weak
            // reference without a default definition). Everything else nm
            // -g prints is a global definition (T/D/B/R/S/C/W/V/...).
            if matches!(kind, "U" | "w" | "v") {
                continue;
            }
            let bare = if apple {
                name.strip_prefix('_').unwrap_or(name)
            } else {
                name
            };
            defined.insert(bare.to_owned());
        }
        for sym in defined {
            definers.entry(sym).or_default().push(archive_name.clone());
        }
    }

    let violations: Vec<String> = definers
        .iter()
        .filter(|(sym, archs)| {
            archs.len() > 1
                && !ALLOWED_DUPLICATE_DEFS
                    .iter()
                    .any(|(allowed, _why)| *allowed == sym.as_str())
        })
        .map(|(sym, archs)| format!("`{}` defined in: {}", sym, archs.join(", ")))
        .collect();
    assert!(
        violations.is_empty(),
        "\n== cross-archive duplicate-definition violations (task #142 guard) ==\n\
         A global defined in two oracle archives is a fleet link failure\n\
         (GNU ld/ld.lld duplicate-symbol hard error) or a silent first-\n\
         definition-wins race that macOS ld64 masks. Give each family its\n\
         own prefixed copy (the CRYPTO_SHARED_SYMS/-D rename convention) or,\n\
         if the share is intentional, allowlist it WITH justification in\n\
         ALLOWED_DUPLICATE_DEFS. Offenders:\n{}\n",
        violations.join("\n")
    );
    // Evidence, not just a verdict (task #141 convention): name the roster.
    println!(
        "cargo:warning=duplicate-definition guard: {} archives, no unallowed cross-archive duplicates ({} allowlisted)",
        archives.len(),
        ALLOWED_DUPLICATE_DEFS.len()
    );
}
