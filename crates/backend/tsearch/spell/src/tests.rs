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
