use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::DictInitData;
use ::types_error::{PgResult, ERRCODE_CONFIG_FILE_ERROR};

use crate::dict_ispell::{dispell_init, dispell_lexize, DictISpell};

fn opts<'m>(mcx: Mcx<'m>, pairs: &[(&str, &str)]) -> PgVec<'m, (PgVec<'m, u8>, PgVec<'m, u8>)> {
    let mut v = PgVec::new_in(mcx);
    for (k, val) in pairs {
        let mut kb = PgVec::new_in(mcx);
        kb.extend_from_slice(k.as_bytes());
        let mut vb = PgVec::new_in(mcx);
        vb.extend_from_slice(val.as_bytes());
        v.push((kb, vb));
    }
    v
}

fn static_mcx() -> Mcx<'static> {
    ::pg_locale::set_default_locale_c_for_tests();
    let ctx: &'static ::mcx::MemoryContext =
        Box::leak(Box::new(::mcx::MemoryContext::new("spell-test")));
    ctx.mcx()
}

fn try_dict(mcx: Mcx<'static>, dictfile: &str, afffile: &str) -> PgResult<DictISpell> {
    let init = DictInitData {
        mcx,
        drop_fn: core::cell::Cell::new(None),
        dict_options: opts(mcx, &[("dictfile", dictfile), ("afffile", afffile)]),
        int_options: {
            let mut v = PgVec::new_in(mcx);
            v.push(None);
            v.push(None);
            v
        },
    };
    dispell_init(&init)
}

fn make_dict(mcx: Mcx<'static>, dictfile: &str, afffile: &str) -> Result<DictISpell, String> {
    try_dict(mcx, dictfile, afffile).map_err(|e| e.message().to_string())
}

fn lexize(mcx: Mcx<'static>, d: &DictISpell, word: &str) -> Option<Vec<String>> {
    dispell_lexize(mcx, d, word.as_bytes())
        .unwrap()
        .map(|r| {
            r.0.iter()
                .map(|l| String::from_utf8_lossy(&l.lexeme).into_owned())
                .collect()
        })
}

fn check(mcx: Mcx<'static>, d: &DictISpell, cases: &[(&str, Option<&[&str]>)], failures: &mut Vec<String>, tag: &str) {
    for (word, want) in cases {
        let got = lexize(mcx, d, word);
        let got_ref: Option<Vec<&str>> = got
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());
        let want_vec: Option<Vec<&str>> = want.map(|w| w.to_vec());
        if got_ref != want_vec {
            failures.push(format!("{tag} {word}: got {got:?}, want {want:?}"));
        }
    }
}

// Oracle: expected/tsdicts.out ts_lexize blocks (NULL renders as None).
#[test]
fn tsdicts_ts_lexize_oracle() {
    std::env::set_var(
        "PGRUST_PGSHAREDIR",
        format!("{}/fixtures", env!("CARGO_MANIFEST_DIR")),
    );
    let mcx = static_mcx();
    let mut failures = Vec::new();

    let ispell = make_dict(mcx, "ispell_sample", "ispell_sample").unwrap();
    let common: &[(&str, Option<&[&str]>)] = &[
        ("skies", Some(&["sky"])),
        ("bookings", Some(&["booking", "book"])),
        ("booking", Some(&["booking", "book"])),
        ("foot", Some(&["foot"])),
        ("foots", Some(&["foot"])),
        ("rebookings", Some(&["booking", "book"])),
        ("rebooking", Some(&["booking", "book"])),
        ("rebook", None),
        ("unbookings", Some(&["book"])),
        ("unbooking", Some(&["book"])),
        ("unbook", Some(&["book"])),
        ("footklubber", Some(&["foot", "klubber"])),
        (
            "footballklubber",
            Some(&["footballklubber", "foot", "ball", "klubber", "football", "klubber"]),
        ),
        ("ballyklubber", Some(&["ball", "klubber"])),
        ("footballyklubber", Some(&["foot", "ball", "klubber"])),
    ];
    check(mcx, &ispell, common, &mut failures, "ispell");

    let hunspell = make_dict(mcx, "ispell_sample", "hunspell_sample").unwrap();
    check(mcx, &hunspell, common, &mut failures, "hunspell");

    let long = make_dict(mcx, "hunspell_sample_long", "hunspell_sample_long").unwrap();
    check(mcx, &long, common, &mut failures, "hunspell_long");
    check(
        mcx,
        &long,
        &[
            ("booked", Some(&["book"])),
            ("ballsklubber", Some(&["ball", "klubber"])),
            ("ex-machina", Some(&["ex-", "machina"])),
        ],
        &mut failures,
        "hunspell_long",
    );

    let num = make_dict(mcx, "hunspell_sample_num", "hunspell_sample_num").unwrap();
    check(mcx, &num, common, &mut failures, "hunspell_num");
    check(
        mcx,
        &num,
        &[("sk", Some(&["sky"])), ("booked", Some(&["book"]))],
        &mut failures,
        "hunspell_num",
    );

    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.join("\n"));
}

// Oracle: the affix/dict suitability errors in expected/tsdicts.out.
#[test]
fn tsdicts_bad_pairs_oracle() {
    std::env::set_var(
        "PGRUST_PGSHAREDIR",
        format!("{}/fixtures", env!("CARGO_MANIFEST_DIR")),
    );
    let mcx = static_mcx();

    let err = make_dict(mcx, "ispell_sample", "hunspell_sample_long").err().unwrap();
    assert_eq!(err, "invalid affix alias \"GJUS\"");

    let err = make_dict(mcx, "ispell_sample", "hunspell_sample_num").err().unwrap();
    assert_eq!(err, "invalid affix flag \"SZ\\\"");

    assert!(make_dict(mcx, "hunspell_sample_long", "ispell_sample").is_ok());
    assert!(make_dict(mcx, "hunspell_sample_long", "hunspell_sample_num").is_ok());
    assert!(make_dict(mcx, "hunspell_sample_num", "ispell_sample").is_ok());

    let err = make_dict(mcx, "hunspell_sample_num", "hunspell_sample_long").err().unwrap();
    assert_eq!(err, "invalid affix alias \"302,301,202,303\"");
}

// Oracle: C 18.6 spell.c after upstream 4689ea9ceee3 -- a Hunspell AF alias
// slot must be filled before an affix entry references it, and the whole
// table must be present by end of file (both ERRCODE_CONFIG_FILE_ERROR).
#[test]
fn hunspell_af_alias_table_oracle() {
    std::env::set_var(
        "PGRUST_PGSHAREDIR",
        format!("{}/fixtures", env!("CARGO_MANIFEST_DIR")),
    );
    let mcx = static_mcx();

    let err = try_dict(mcx, "hunspell_af", "hunspell_af_unfilled")
        .err()
        .expect("alias referenced before its AF line must be rejected");
    assert_eq!(err.message(), "invalid affix alias \"2\"");
    assert_eq!(err.sqlstate(), ERRCODE_CONFIG_FILE_ERROR);

    let err = try_dict(mcx, "hunspell_af", "hunspell_af_short")
        .err()
        .expect("AF table shorter than declared must be rejected");
    assert_eq!(err.message(), "number of aliases is less than specified number 3");
    assert_eq!(err.sqlstate(), ERRCODE_CONFIG_FILE_ERROR);

    // An AF line with an empty flag field fills its slot with "" (C stores
    // cpstrdup(""), not NULL), so a complete table referencing it loads.
    let ok = make_dict(mcx, "hunspell_af", "hunspell_af_ok").unwrap();
    assert_eq!(lexize(mcx, &ok, "books"), Some(vec!["book".to_string()]));
}

/// MUST-FAIL CONTROL for the mk_a_node / mk_sp_node recursion guards
/// (build.rs). Both builders advance one recursion level per CHARACTER of the
/// longest affix `repl` (mkANode) or dictionary word (mkSPNode), and verbatim
/// C spell.c carries no check_stack_depth() at either site -- so the depth is
/// driven straight from the .aff/.dict bytes. This port's frames are much
/// larger than the C frames (owning PgVec/Vec locals), so before the guards
/// were added a long affix drove the recursion into the OS guard page and
/// SIGSEGV'd the process. That is what killed the spellfam_diff 10M
/// differential floor four times; CI cluster job
/// pgrust-fuzz-campaign-1785668253-5ec7-84536 @ fd4029967d died at 6.73M/10M
/// execs with an ASan stack-overflow whose trace was 246 identical
/// `tsearch_spell::build::IspellDict::mk_a_node` frames.
///
/// WITHOUT the `check_stack_depth()?` calls this test does not "fail" politely
/// -- it takes the whole test process down with a stack overflow, which is
/// precisely the defect. WITH them the recursion is admitted against the byte
/// bound and the build raises 54001 (statement too complex), which the
/// differential driver already treats as a documented non-surface.
#[test]
fn mk_a_node_deep_affix_raises_54001_not_stack_overflow() {
    let mcx = static_mcx();

    // Arm the guard the way a real backend does (set_stack_base in main() plus
    // the max_stack_depth GUC). libtest gives each test thread a 2 MiB stack,
    // and PG's own admission rule is "stack minus STACK_DEPTH_SLOP", so pin
    // the limit the same way the differential driver does.
    const MAX_KB: i32 = 2048 - (::stack_depth::STACK_DEPTH_SLOP / 1024) as i32;
    ::stack_depth::set_max_stack_depth(MAX_KB);
    ::stack_depth::assign_max_stack_depth(MAX_KB);
    let _ = ::stack_depth::set_stack_base();
    // Production pairing: clamp the scaled budget to libtest's real 2 MiB
    // thread stack so the deep recursion trips 54001, not SIGSEGV.
    ::stack_depth::set_thread_stack_ceiling(2 << 20);

    // Old-ispell-format affix file with ONE suffix entry whose replacement
    // string is long: `repl` length == mk_a_node recursion depth.
    let mut aff = Vec::new();
    aff.extend_from_slice(b"suffixes\nflag Z:\n    . > ");
    aff.extend_from_slice(&b"A".repeat(60_000));
    aff.push(b'\n');

    // ni_import_affixes takes a PATH (tsearch_readline opens it), so stage the
    // fixture as a real file the way the differential driver does.
    let path = std::env::temp_dir().join("pgrust_spell_deep_affix.aff");
    std::fs::write(&path, &aff).expect("stage affix fixture");
    let pathb = path.as_os_str().as_encoded_bytes().to_vec();

    let mut obj = crate::IspellDict::new(mcx);
    obj.ni_start_build().expect("ni_start_build");
    // Parsing itself must not blow up; the recursion happens in ni_sort_affixes.
    if let Err(e) = obj.ni_import_affixes(&pathb) {
        // A parse-level refusal would be an acceptable bound too -- but then
        // this control is vacuous, so say so loudly rather than pass silently.
        panic!(
            "control is VACUOUS: ni_import_affixes rejected the long-repl affix \
             ({:?}), so mk_a_node was never reached; reshape the fixture",
            e.message()
        );
    }
    let err = obj
        .ni_sort_affixes()
        .expect_err("deep mk_a_node recursion must be refused, not overflow the stack");
    assert_eq!(
        err.sqlstate(),
        ::types_error::ERRCODE_STATEMENT_TOO_COMPLEX,
        "expected 54001 statement-too-complex from the recursion guard, got {:?}",
        err.message()
    );
}

// ---- audit-18.6 b167 (fp-tsearch-spell): loader/normalizer witnesses ----

fn fixtures_dir() -> String {
    let dir = format!("{}/fixtures", env!("CARGO_MANIFEST_DIR"));
    std::env::set_var("PGRUST_PGSHAREDIR", &dir);
    dir
}

// spell.c:2165 CheckAffix: `if (keeplen + findlen >= 2 * MAXNORMLEN) return NULL;`
// — an affix whose result would fill C's 512-byte newword buffer is rejected.
// spell_longfind: flag A strips 263 y's (keeplen 249 + findlen 263 = 512 →
// refused), flag B strips 262 (511 → applied); both stems are in the .dict.
#[test]
fn check_affix_refuses_result_at_two_maxnormlen() {
    fixtures_dir();
    let mcx = static_mcx();
    let d = make_dict(mcx, "spell_longfind", "spell_longfind").unwrap();

    let refused = format!("{}a", "x".repeat(249));
    assert_eq!(
        lexize(mcx, &d, &refused),
        None,
        "keeplen + findlen == 2 * MAXNORMLEN must be refused (C returns NULL)"
    );

    let applied = format!("{}a", "z".repeat(249));
    let want = format!("{}{}", "z".repeat(249), "y".repeat(262));
    assert_eq!(
        lexize(mcx, &d, &applied),
        Some(vec![want]),
        "keeplen + findlen == 2 * MAXNORMLEN - 1 must still be applied"
    );
}

// spell.c:525-529 NIImportDictionary: `could not open dictionary file "%s": %m`
// — fopen's errno, not a hardcoded ENOENT text. A directory opens fine under
// fopen(3) and reads as empty (pg_get_line_buf's ferror ends the file).
#[test]
fn loader_open_failure_reports_fopen_errno() {
    let mcx = static_mcx();
    let dir = std::env::temp_dir().join("pgrust_spell_b167_open");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("stage dir");
    let regular = dir.join("plain.dict");
    std::fs::write(&regular, b"book\n").expect("stage plain.dict");

    // ENOTDIR: a path that descends through a regular file (root-proof).
    let notdir = regular.join("x");
    let want_errno = std::fs::File::open(&notdir)
        .expect_err("open through a regular file must fail")
        .raw_os_error()
        .expect("os error");
    let notdir_b = notdir.as_os_str().as_encoded_bytes().to_vec();
    let mut obj = crate::IspellDict::new(mcx);
    obj.ni_start_build().expect("ni_start_build");
    let err = obj
        .ni_import_dictionary(&notdir_b)
        .expect_err("open through a regular file must be refused");
    let prefix = format!("could not open dictionary file \"{}\": ", notdir.display());
    let rendered = err
        .message()
        .strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("message {:?} lacks prefix {prefix:?}", err.message()));
    assert!(
        !rendered.is_empty() && rendered != "%m" && rendered != "No such file or directory",
        "`%m` must render strerror(ENOTDIR), got {rendered:?}"
    );
    assert_eq!(err.saved_errno(), Some(want_errno), "saved errno must be fopen's");
    assert_eq!(err.sqlstate(), ERRCODE_CONFIG_FILE_ERROR);

    // ENOENT still reports ENOENT — through the same errno path.
    let missing = dir.join("missing.dict");
    let want_errno = std::fs::File::open(&missing).expect_err("missing").raw_os_error().unwrap();
    let missing_b = missing.as_os_str().as_encoded_bytes().to_vec();
    let err = obj.ni_import_dictionary(&missing_b).expect_err("missing file");
    assert_eq!(err.saved_errno(), Some(want_errno));
    assert_eq!(
        err.message(),
        format!(
            "could not open dictionary file \"{}\": No such file or directory",
            missing.display()
        )
    );

    // A directory: fopen succeeds, the read fails → empty dictionary, no error.
    let dir_b = dir.as_os_str().as_encoded_bytes().to_vec();
    obj.ni_import_dictionary(&dir_b)
        .expect("a directory reads as an empty dictionary file, as under fopen/fgets");
    assert_eq!(obj.spell.len(), 0);
}

// ts_locale.c tsearch_readline_callback: every error raised while a config
// file is open carries `line N of configuration file "<path>": "<line>"` —
// the line text included for loader errors (spell_badentry: parse_affentry
// "syntax error" on line 3; spell_badflag: the FLAG-value error on line 2)
// and omitted for an error inside tsearch_readline itself (spell_badenc:
// the encoding violation on line 3).
#[test]
fn loader_errors_carry_readline_context() {
    let fixtures = fixtures_dir();
    let mcx = static_mcx();
    let path = |name: &str| format!("{fixtures}/tsearch_data/{name}");

    let err = try_dict(mcx, "spell_plain", "spell_badentry").err().expect("syntax error");
    assert_eq!(err.message(), "syntax error");
    assert_eq!(err.sqlstate(), ERRCODE_CONFIG_FILE_ERROR);
    assert_eq!(
        err.context(),
        Some(
            format!(
                "line 3 of configuration file \"{}\": \"    [^Y] > 1S\n\"",
                path("spell_badentry.affix")
            )
            .as_str()
        )
    );

    let err = try_dict(mcx, "spell_plain", "spell_badflag").err().expect("bad FLAG value");
    assert_eq!(
        err.message(),
        "Ispell dictionary supports only \"default\", \"long\", and \"num\" flag values"
    );
    assert_eq!(
        err.context(),
        Some(
            format!(
                "line 2 of configuration file \"{}\": \"FLAG bogus\n\"",
                path("spell_badflag.affix")
            )
            .as_str()
        )
    );

    let err = try_dict(mcx, "spell_plain", "spell_badenc").err().expect("encoding violation");
    assert!(
        err.message().starts_with("invalid byte sequence for encoding"),
        "{:?}",
        err.message()
    );
    assert_eq!(
        err.context(),
        Some(
            format!(
                "line 3 of configuration file \"{}\"",
                path("spell_badenc.affix")
            )
            .as_str()
        ),
        "an error inside tsearch_readline has no line text (C dares not print it)"
    );
}
