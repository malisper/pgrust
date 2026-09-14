use ::mcx::{Mcx, PgVec};
use ::ts_locale::dict_api::DictInitData;
use ::wchar::{pg_enc, PG_KOI8R, PG_LATIN1, PG_LATIN2, PG_UTF8};

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
        drop_fn: core::cell::Cell::new(None),
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

// (language, [(word, expected stem)]) — expected values are `ts_lexize` output
// from PostgreSQL 18.4 (Homebrew) over bare snowball dictionaries (no
// stopwords), captured in a UTF8/locale-C database; the Snowball automatons
// are unchanged across 18.x patch releases. Single-word stemming is
// deterministic (no tie-order hazard).
#[rustfmt::skip]
const UTF8_ORACLE: &[(&str, &[(&str, &str)])] = &[
    ("arabic", &[("المدرسة", "مدرس"), ("جميلة", "جميل"), ("كتابة", "كتاب"), ("مكتبة", "مكتب"), ("والكتاب", "والكتاب"), ("يكتبون", "يكتب")]),
    ("armenian", &[("գրքերը", "գրքերը"), ("երեխաները", "երեխ"), ("մեծագույն", "մեծագույ"), ("սիրում", "սիր"), ("քաղաքում", "քաղա")]),
    ("basque", &[("egunero", "egun"), ("emakumeak", "ema"), ("etxeak", "etxe"), ("handienak", "handi"), ("liburuak", "liburu")]),
    ("catalan", &[("cantava", "cant"), ("cases", "case"), ("importants", "import"), ("nacions", "nacion"), ("treballant", "treball")]),
    ("danish", &[("arbejdede", "arbejded"), ("bøgerne", "bøg"), ("københavnske", "københavnsk"), ("størstedelen", "størstedel"), ("venligst", "ven")]),
    ("dutch", &[("belangrijkste", "belangrijkst"), ("fietsen", "fiets"), ("gewerkt", "gewerkt"), ("huizen", "huiz"), ("kinderen", "kinder")]),
    ("estonian", &[("kirjutamine", "kirjutamise"), ("lastele", "last"), ("raamatud", "raama"), ("suuremad", "suure"), ("töötavad", "tööta")]),
    ("finnish", &[("kirjoja", "kirj"), ("nopeasti", "nopeast"), ("opiskelijoille", "opiskelij"), ("suurimmat", "suurim"), ("taloissa", "talo")]),
    ("french", &[("chevaux", "cheval"), ("continuité", "continu"), ("majestueusement", "majestu"), ("mangeait", "mang"), ("nationales", "national"), ("travaillons", "travaillon")]),
    ("german", &[("arbeiteten", "arbeitet"), ("häuser", "haus"), ("kinder", "kind"), ("schönsten", "schon"), ("wissenschaftliche", "wissenschaft")]),
    ("greek", &[("βιβλία", "βιβλ"), ("γράφοντας", "γραφ"), ("εργάζονται", "εργαζ"), ("μεγαλύτερος", "μεγαλ"), ("παιδιά", "πα")]),
    ("hindi", &[("किताबें", "किताब"), ("चलना", "चल"), ("बड़ा", "बड़"), ("लड़कियों", "लड़क"), ("हिन्दी", "हिन्द")]),
    ("hungarian", &[("dolgozunk", "dolgoz"), ("gyerekeknek", "gyerek"), ("házakban", "ház"), ("könyvek", "könyv"), ("legnagyobb", "legnagyobb")]),
    ("indonesian", &[("buku-buku", "buku-bu"), ("membaca", "baca"), ("menuliskan", "ulis"), ("pekerjaan", "kerja"), ("terbesar", "besar")]),
    ("irish", &[("bhfeidhm", "feidhm"), ("leabhair", "leabhair"), ("oibríonn", "oibríonn"), ("páistí", "páistí"), ("scríbhneoireacht", "scríbhneoir")]),
    ("italian", &[("bellissimo", "bellissim"), ("cavalli", "cavall"), ("lavorando", "lavor"), ("mangiavano", "mang"), ("nazionale", "nazional")]),
    ("lithuanian", &[("didžiausias", "did"), ("dirbame", "dirb"), ("knygos", "knyg"), ("rašymas", "rašym"), ("vaikams", "vaik")]),
    ("nepali", &[("किताबहरू", "किताब"), ("गरेको", "गर"), ("ठूलो", "ठूलो"), ("लेखेर", "लेखेर"), ("विद्यालयमा", "विद्यालय")]),
    ("norwegian", &[("arbeidet", "arbeid"), ("barna", "barn"), ("bøkene", "bøk"), ("størst", "størst"), ("vennlig", "venn")]),
    ("porter", &[("conditional", "condit"), ("happiness", "happi"), ("relational", "relat"), ("running", "run"), ("vietnamization", "vietnam")]),
    ("portuguese", &[("cavalos", "caval"), ("felicidade", "felic"), ("grandes", "grand"), ("nacionais", "nacion"), ("trabalhando", "trabalh")]),
    ("romanian", &[("copiilor", "cop"), ("cărțile", "cărț"), ("frumoasă", "frumoas"), ("lucrează", "lucr"), ("scriind", "scriind")]),
    ("russian", &[("большие", "больш"), ("детям", "дет"), ("книги", "книг"), ("письменность", "письмен"), ("работают", "работа")]),
    ("serbian", &[("деци", "dec"), ("књиге", "knjig"), ("највећи", "najveć"), ("писање", "pisanj"), ("радимо", "radi")]),
    ("spanish", &[("caballos", "caball"), ("felicidad", "felic"), ("grandísimo", "grandisim"), ("nacionales", "nacional"), ("trabajando", "trabaj")]),
    ("swedish", &[("arbetade", "arbet"), ("barnen", "barn"), ("böckerna", "böck"), ("största", "störst"), ("vänligen", "vän")]),
    ("tamil", &[("எழுதுகிறேன்", "எழுது"), ("குழந்தைகளுக்கு", "குழந்தைகள்"), ("புத்தகங்கள்", "புத்தகம்"), ("பெரிய", "பெரி"), ("வேலைகள்", "வேலை")]),
    ("turkish", &[("büyükler", "büyük"), ("evlerimizde", "ev"), ("kitaplar", "kitap"), ("çalışıyorlar", "çalışıyor"), ("çocuklara", "çocuk")]),
    ("yiddish", &[("ארבעטן", "ארב"), ("ביכער", "ביכ"), ("געשריבן", "שרײב"), ("גרעסטע", "גרע"), ("קינדער", "קינד")]),
];

// Same oracle protocol, run in LATIN1 / LATIN2 / KOI8R databases so the
// single-byte automatons (not the UTF-8 ones) are the modules under test.
// Byte literals are the words/stems in the database encoding.
#[rustfmt::skip]
const LATIN1_ORACLE: &[(&str, &[u8], &[u8])] = &[
        ("french", b"chevaux", b"cheval"), // chevaux -> cheval
        ("french", b"mangeait", b"mang"), // mangeait -> mang
        ("french", b"nationales", b"national"), // nationales -> national
        ("french", b"majestueusement", b"majestu"), // majestueusement -> majestu
        ("french", b"continuit\xe9", b"continu"), // continuité -> continu
        ("french", b"travaillons", b"travaillon"), // travaillons -> travaillon
        ("german", b"h\xe4user", b"haus"), // häuser -> haus
        ("german", b"arbeiteten", b"arbeitet"), // arbeiteten -> arbeitet
        ("german", b"sch\xf6nsten", b"schon"), // schönsten -> schon
        ("german", b"wissenschaftliche", b"wissenschaft"), // wissenschaftliche -> wissenschaft
        ("german", b"kinder", b"kind"), // kinder -> kind
        ("spanish", b"caballos", b"caball"), // caballos -> caball
        ("spanish", b"trabajando", b"trabaj"), // trabajando -> trabaj
        ("spanish", b"nacionales", b"nacional"), // nacionales -> nacional
        ("spanish", b"grand\xedsimo", b"grandisim"), // grandísimo -> grandisim
        ("spanish", b"felicidad", b"felic"), // felicidad -> felic
];
#[rustfmt::skip]
const LATIN2_ORACLE: &[(&str, &[u8], &[u8])] = &[
        ("hungarian", b"k\xf6nyvek", b"k\xf6nyv"), // könyvek -> könyv
        ("hungarian", b"h\xe1zakban", b"h\xe1z"), // házakban -> ház
        ("hungarian", b"legnagyobb", b"legnagyobb"), // legnagyobb -> legnagyobb
        ("hungarian", b"dolgozunk", b"dolgoz"), // dolgozunk -> dolgoz
        ("hungarian", b"gyerekeknek", b"gyerek"), // gyerekeknek -> gyerek
];
#[rustfmt::skip]
const KOI8R_ORACLE: &[(&str, &[u8], &[u8])] = &[
        ("russian", b"\xcb\xce\xc9\xc7\xc9", b"\xcb\xce\xc9\xc7"), // книги -> книг
        ("russian", b"\xd2\xc1\xc2\xcf\xd4\xc1\xc0\xd4", b"\xd2\xc1\xc2\xcf\xd4\xc1"), // работают -> работа
        ("russian", b"\xc2\xcf\xcc\xd8\xdb\xc9\xc5", b"\xc2\xcf\xcc\xd8\xdb"), // большие -> больш
        ("russian", b"\xc4\xc5\xd4\xd1\xcd", b"\xc4\xc5\xd4"), // детям -> дет
        ("russian", b"\xd0\xc9\xd3\xd8\xcd\xc5\xce\xce\xcf\xd3\xd4\xd8", b"\xd0\xc9\xd3\xd8\xcd\xc5\xce"), // письменность -> письмен
];

fn init_lang(mcx: Mcx<'static>, lang: &str) -> DictSnowball {
    let init = DictInitData {
        mcx,
        drop_fn: core::cell::Cell::new(None),
        dict_options: opts(mcx, &[("language", lang)]),
        int_options: {
            let mut v = PgVec::new_in(mcx);
            v.push(None);
            v
        },
    };
    dsnowball_init(&init).unwrap_or_else(|e| panic!("init {lang}: {e:?}"))
}

fn lexize_bytes(mcx: Mcx<'static>, d: &DictSnowball, word: &[u8]) -> Option<Vec<u8>> {
    let res = dsnowball_lexize(mcx, d, word).unwrap();
    res.0.first().map(|l| l.lexeme.to_vec())
}

fn check_oracle(enc: pg_enc, table: &[(&str, &[(&[u8], &[u8])])]) {
    ::mbutils::SetDatabaseEncoding(enc).unwrap();
    let mcx = static_mcx();
    let mut failures = Vec::new();
    for (lang, pairs) in table {
        let d = init_lang(mcx, lang);
        for (word, want) in *pairs {
            let got = lexize_bytes(mcx, &d, word);
            if got.as_deref() != Some(*want) {
                failures.push(format!(
                    "{lang}: {}: got {:?}, want {}",
                    String::from_utf8_lossy(word),
                    got.as_deref().map(String::from_utf8_lossy),
                    String::from_utf8_lossy(want),
                ));
            }
        }
    }
    assert!(failures.is_empty(), "stem mismatches:\n{}", failures.join("\n"));
}

fn by_lang<'a>(table: &'a [(&'a str, &'a [u8], &'a [u8])]) -> Vec<(&'a str, Vec<(&'a [u8], &'a [u8])>)> {
    let mut out: Vec<(&str, Vec<(&[u8], &[u8])>)> = Vec::new();
    for (lang, w, s) in table {
        match out.last_mut() {
            Some((l, v)) if l == lang => v.push((w, s)),
            _ => out.push((lang, vec![(w, s)])),
        }
    }
    out
}

// All 29 shipped Snowball languages against the PostgreSQL oracle, UTF-8 leg.
#[test]
fn all_languages_stem_utf8_oracle() {
    ::mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
    let mcx = static_mcx();
    let mut failures = Vec::new();
    for (lang, pairs) in UTF8_ORACLE {
        let d = init_lang(mcx, lang);
        for (word, want) in *pairs {
            let got = lexize_bytes(mcx, &d, word.as_bytes());
            if got.as_deref() != Some(want.as_bytes()) {
                failures.push(format!(
                    "{lang}: {word}: got {:?}, want {want}",
                    got.as_deref().map(String::from_utf8_lossy)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "stem mismatches:\n{}", failures.join("\n"));
}

#[test]
fn latin1_stems_oracle() {
    let t = by_lang(LATIN1_ORACLE);
    let t: Vec<_> = t.iter().map(|(l, v)| (*l, v.as_slice())).collect();
    check_oracle(PG_LATIN1, &t);
}

#[test]
fn latin2_hungarian_stems_oracle() {
    let t = by_lang(LATIN2_ORACLE);
    let t: Vec<_> = t.iter().map(|(l, v)| (*l, v.as_slice())).collect();
    check_oracle(PG_LATIN2, &t);
}

#[test]
fn koi8r_russian_stems_oracle() {
    let t = by_lang(KOI8R_ORACLE);
    let t: Vec<_> = t.iter().map(|(l, v)| (*l, v.as_slice())).collect();
    check_oracle(PG_KOI8R, &t);
}

// Every dict_snowball.c stemmer_modules language initializes under UTF-8 and
// returns a lexeme (ts_lexize shape) for a plain ASCII token.
#[test]
fn all_languages_initialize_utf8() {
    ::mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
    let mcx = static_mcx();
    for (lang, _) in UTF8_ORACLE {
        let d = init_lang(mcx, lang);
        let got = lexize_bytes(mcx, &d, b"test");
        assert!(got.is_some(), "{lang}: no lexeme for ascii token");
    }
    // english is in the modules table too (not in UTF8_ORACLE).
    let d = init_lang(mcx, "english");
    assert_eq!(lexize_bytes(mcx, &d, b"books").as_deref(), Some(b"book".as_slice()));
}

// dict_snowball.c:253-260: an error after the Language option reclaims the
// stemmer (dictCtx); the Rust environment must be closed, not leaked.
#[test]
fn failed_init_closes_located_stemmer() {
    use core::sync::atomic::Ordering::SeqCst;
    let mcx = static_mcx();
    let before = crate::dict::STEMMERS_CLOSED.load(SeqCst);
    let init = DictInitData {
        mcx,
        drop_fn: core::cell::Cell::new(None),
        dict_options: opts(mcx, &[("language", "english"), ("bogus", "x")]),
        int_options: {
            let mut v = PgVec::new_in(mcx);
            v.push(None);
            v.push(None);
            v
        },
    };
    let Err(err) = dsnowball_init(&init) else { panic!("init with a bogus option succeeded") };
    assert_eq!(err.message(), "unrecognized Snowball parameter: \"bogus\"");
    assert!(crate::dict::STEMMERS_CLOSED.load(SeqCst) > before);
}
