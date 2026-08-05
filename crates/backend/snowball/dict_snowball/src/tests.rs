use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::DictInitData;

use crate::dict::{dsnowball_init, dsnowball_lexize, DictSnowball};

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
        Box::leak(Box::new(::mcx::MemoryContext::new("dict-snowball-test")));
    ctx.mcx()
}

fn lexize_one(mcx: Mcx<'static>, d: &DictSnowball, word: &str) -> Option<String> {
    let res = dsnowball_lexize(mcx, d, word.as_bytes()).unwrap();
    res.0
        .first()
        .map(|l| String::from_utf8_lossy(&l.lexeme).into_owned())
}

#[test]
fn english_stem_oracle() {
    std::env::set_var(
        "PGRUST_PGSHAREDIR",
        format!("{}/fixtures", env!("CARGO_MANIFEST_DIR")),
    );
    let mcx = static_mcx();
    let init = DictInitData {
        mcx,
        dict_options: opts(mcx, &[("language", "english"), ("stopwords", "english")]),
        int_options: {
            let mut v = PgVec::new_in(mcx);
            v.push(None);
            v.push(None);
            v
        },
    };
    let d = dsnowball_init(&init).unwrap();

    // (input, stem) pairs read off expected/{tstypes,tsdicts,tsearch}.out
    // to_tsvector('english', ...) results.
    let pairs: &[(&str, &str)] = &[
        ("rebel", "rebel"),
        ("spaceships", "spaceship"),
        ("spaceship", "spaceship"),
        ("striking", "strike"),
        ("strike", "strike"),
        ("hidden", "hidden"),
        ("base", "base"),
        ("bases", "base"),
        ("called", "call"),
        ("often", "often"),
        ("pronounced", "pronounc"),
        ("common", "common"),
        ("mistake", "mistak"),
        ("write", "write"),
        ("instead", "instead"),
        ("plural", "plural"),
        ("right", "right"),
        ("form", "form"),
        ("usually", "usual"),
        ("abbreviation", "abbrevi"),
        ("new", "new"),
        ("star", "star"),
        ("qwerty", "qwerti"),
        ("readline", "readlin"),
        ("wow", "wow"),
        ("empire", "empir"),
        ("evil", "evil"),
        ("first", "first"),
        ("galactic", "galact"),
        ("victory", "victori"),
        ("won", "won"),
        ("supernova", "supernova"),
        ("books", "book"),
        ("booking", "book"),
    ];
    let mut failures = Vec::new();
    for (input, want) in pairs {
        let got = lexize_one(mcx, &d, input);
        if got.as_deref() != Some(*want) {
            failures.push(format!("{input}: got {got:?}, want {want}"));
        }
    }
    assert!(failures.is_empty(), "stem mismatches:\n{}", failures.join("\n"));

    // Every stop word lexizes to a present-but-empty result.
    let stop = std::fs::read_to_string(format!(
        "{}/fixtures/tsearch_data/english.stop",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let mut n = 0;
    for w in stop.lines().map(str::trim).filter(|w| !w.is_empty()) {
        let res = dsnowball_lexize(mcx, &d, w.as_bytes()).unwrap();
        assert!(res.0.is_empty(), "stopword {w} not dropped");
        n += 1;
    }
    assert!(n > 100, "stopword corpus unexpectedly small: {n}");

    // Long tokens pass through lowercased, unstemmed.
    let long = "A".repeat(1001);
    let got = lexize_one(mcx, &d, &long).unwrap();
    assert_eq!(got, "a".repeat(1001));
}
