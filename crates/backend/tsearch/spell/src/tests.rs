use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::DictInitData;

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

fn make_dict(mcx: Mcx<'static>, dictfile: &str, afffile: &str) -> Result<DictISpell, String> {
    let init = DictInitData {
        mcx,
        dict_options: opts(mcx, &[("dictfile", dictfile), ("afffile", afffile)]),
        int_options: {
            let mut v = PgVec::new_in(mcx);
            v.push(None);
            v.push(None);
            v
        },
    };
    dispell_init(&init).map_err(|e| e.message().to_string())
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

/// MUST-FAIL CONTROL for the mk_a_node / mk_sp_node recursion guards
/// (build.rs). Both builders advance one recursion level per CHARACTER of the
/// longest affix `repl` (mkANode) or dictionary word (mkSPNode), and verbatim
/// C spell.c carries no check_stack_depth() at either site -- so the depth is
/// driven straight from the .aff/.dict bytes. This port's frames are much
/// larger than the C frames (owning PgVec/Vec locals), so before the guards
/// were added a long affix drove the recursion into the OS guard page and
/// SIGSEGV'd the process. That is what killed the spellfam_diff 10M
/// differential floor four times; fleet job
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
