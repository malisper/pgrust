use crate::parser::{tparser_get, tparser_init};

fn setup() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    pg_locale::set_database_ctype_is_c(true);
}

fn tokenize(input: &str) -> Vec<(i32, String)> {
    let ctx = mcx::MemoryContext::new("wparser-test");
    let mcx = ctx.mcx();
    let bytes = input.as_bytes();
    let mut prs = tparser_init(mcx, bytes.as_ptr(), bytes.len()).unwrap();
    let mut out = Vec::new();
    while tparser_get(&mut prs).unwrap() {
        out.push((
            prs.type_,
            String::from_utf8_lossy(prs.token_bytes()).into_owned(),
        ));
    }
    out
}

// Expected stream from C 18.3: regress expected/tsearch.out, the
// SELECT * FROM ts_parse('default', '345 qwe@efd.r ...') 139-row vector.
static BIG_INPUT_HEAD: &str = concat!(
    "345 qwe@efd.r ' http://www.com/ http://aew.werc.ewr/?ad=qwe&dw ",
    "1aew.werc.ewr/?ad=qwe&dw 2aew.werc.ewr http://3aew.werc.ewr/?ad=qwe&dw ",
    "http://4aew.werc.ewr http://5aew.werc.ewr:8100/?  ad=qwe&dw ",
    "6aew.werc.ewr:8100/?ad=qwe&dw 7aew.werc.ewr:8100/?ad=qwe&dw=%20%32 ",
    "+4.0e-10 qwe qwe qwqwe 234.435 455 5.005 teodor@stack.net ",
    "teodor@123-stack.net 123_teodor@stack.net 123-teodor@stack.net ",
    "qwe-wer asdf <fr>qwer jf sdjk<we hjwer <werrwe> ewr1> ewri2 ",
    "<a href=\"qwe<qwe>\">\n",
    "/usr/local/fff /awdf/dwqe/4325 rewt/ewr wefjn /wqe-324/ewr gist.h ",
    "gist.h.c gist.c. readline 4.2 4.2. 4.2, readline-4.2 readline-4.2. 234\n",
    "<i <b> wow  < jqw <> qwerty",
);

static BIG_EXPECTED: &[(i32, &str)] = &[
    (22, "345"),
    (12, " "),
    (1, "qwe"),
    (12, "@"),
    (19, "efd.r"),
    (12, " ' "),
    (14, "http://"),
    (6, "www.com"),
    (12, "/ "),
    (14, "http://"),
    (5, "aew.werc.ewr/?ad=qwe&dw"),
    (6, "aew.werc.ewr"),
    (18, "/?ad=qwe&dw"),
    (12, " "),
    (5, "1aew.werc.ewr/?ad=qwe&dw"),
    (6, "1aew.werc.ewr"),
    (18, "/?ad=qwe&dw"),
    (12, " "),
    (6, "2aew.werc.ewr"),
    (12, " "),
    (14, "http://"),
    (5, "3aew.werc.ewr/?ad=qwe&dw"),
    (6, "3aew.werc.ewr"),
    (18, "/?ad=qwe&dw"),
    (12, " "),
    (14, "http://"),
    (6, "4aew.werc.ewr"),
    (12, " "),
    (14, "http://"),
    (5, "5aew.werc.ewr:8100/?"),
    (6, "5aew.werc.ewr:8100"),
    (18, "/?"),
    (12, "  "),
    (1, "ad"),
    (12, "="),
    (1, "qwe"),
    (12, "&"),
    (1, "dw"),
    (12, " "),
    (5, "6aew.werc.ewr:8100/?ad=qwe&dw"),
    (6, "6aew.werc.ewr:8100"),
    (18, "/?ad=qwe&dw"),
    (12, " "),
    (5, "7aew.werc.ewr:8100/?ad=qwe&dw=%20%32"),
    (6, "7aew.werc.ewr:8100"),
    (18, "/?ad=qwe&dw=%20%32"),
    (12, " "),
    (7, "+4.0e-10"),
    (12, " "),
    (1, "qwe"),
    (12, " "),
    (1, "qwe"),
    (12, " "),
    (1, "qwqwe"),
    (12, " "),
    (20, "234.435"),
    (12, " "),
    (22, "455"),
    (12, " "),
    (20, "5.005"),
    (12, " "),
    (4, "teodor@stack.net"),
    (12, " "),
    (4, "teodor@123-stack.net"),
    (12, " "),
    (4, "123_teodor@stack.net"),
    (12, " "),
    (4, "123-teodor@stack.net"),
    (12, " "),
    (16, "qwe-wer"),
    (11, "qwe"),
    (12, "-"),
    (11, "wer"),
    (12, " "),
    (1, "asdf"),
    (12, " "),
    (13, "<fr>"),
    (1, "qwer"),
    (12, " "),
    (1, "jf"),
    (12, " "),
    (1, "sdjk"),
    (12, "<"),
    (1, "we"),
    (12, " "),
    (1, "hjwer"),
    (12, " "),
    (13, "<werrwe>"),
    (12, " "),
    (3, "ewr1"),
    (12, "> "),
    (3, "ewri2"),
    (12, " "),
    (13, "<a href=\"qwe<qwe>\">"),
    (12, "\n"),
    (19, "/usr/local/fff"),
    (12, " "),
    (19, "/awdf/dwqe/4325"),
    (12, " "),
    (19, "rewt/ewr"),
    (12, " "),
    (1, "wefjn"),
    (12, " "),
    (19, "/wqe-324/ewr"),
    (12, " "),
    (19, "gist.h"),
    (12, " "),
    (19, "gist.h.c"),
    (12, " "),
    (19, "gist.c"),
    (12, ". "),
    (1, "readline"),
    (12, " "),
    (20, "4.2"),
    (12, " "),
    (20, "4.2"),
    (12, ". "),
    (20, "4.2"),
    (12, ", "),
    (1, "readline"),
    (20, "-4.2"),
    (12, " "),
    (1, "readline"),
    (20, "-4.2"),
    (12, ". "),
    (22, "234"),
    (12, "\n"),
    (12, "<"),
    (1, "i"),
    (12, " "),
    (13, "<b>"),
    (12, " "),
    (1, "wow"),
    (12, "  "),
    (12, "< "),
    (1, "jqw"),
    (12, " "),
    (12, "<> "),
    (1, "qwerty"),
];

#[test]
fn c_reference_token_stream() {
    setup();
    let got = tokenize(BIG_INPUT_HEAD);
    assert_eq!(got.len(), BIG_EXPECTED.len(), "token count");
    for (i, ((gt, gs), (et, es))) in got.iter().zip(BIG_EXPECTED.iter()).enumerate() {
        assert_eq!((gt, gs.as_str()), (et, *es), "token {i}");
    }
}

#[test]
fn simple_words_and_positions() {
    setup();
    assert_eq!(
        tokenize("The Fat Rats"),
        vec![
            (1, "The".into()),
            (12, " ".into()),
            (1, "Fat".into()),
            (12, " ".into()),
            (1, "Rats".into()),
        ]
    );
}

#[test]
fn script_tag_ignore() {
    setup();
    let toks = tokenize("a<script>skip me</script>b");
    let words: Vec<&str> = toks
        .iter()
        .filter(|(t, _)| *t == 1)
        .map(|(_, s)| s.as_str())
        .collect();
    assert_eq!(words, vec!["a", "b"]);
}

#[test]
fn lextype_table() {
    let l = crate::builtins::lextype();
    assert_eq!(l.len(), 23);
    assert_eq!(l[0].lexid, 1);
    assert_eq!(l[0].alias, "asciiword");
    assert_eq!(l[22].lexid, 23);
    assert_eq!(l[22].alias, "entity");
    assert_eq!(l[11].alias, "blank");
    assert_eq!(l[11].descr, "Space symbols");
}

#[test]
fn stoplist_roundtrip() {
    setup();
    let dir = std::env::temp_dir().join(format!("tsloc-test-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("tsearch_data")).unwrap();
    std::fs::write(
        dir.join("tsearch_data/mystop.stop"),
        "the\n\nfat  trailing\nrats\n",
    )
    .unwrap();
    std::env::set_var("PGRUST_PGSHAREDIR", dir.to_str().unwrap());
    let ctx = mcx::MemoryContext::new("stoplist-test");
    let mcx = ctx.mcx();
    // lower=false: str_tolower(DEFAULT_COLLATION_OID) needs the in-server
    // database default locale (init_database_collation).
    let sl = ts_locale::readstoplist(mcx, Some(b"mystop"), false).unwrap();
    assert_eq!(sl.stop.len(), 3);
    assert!(ts_locale::searchstoplist(&sl, b"the"));
    assert!(ts_locale::searchstoplist(&sl, b"fat"));
    assert!(ts_locale::searchstoplist(&sl, b"rats"));
    assert!(!ts_locale::searchstoplist(&sl, b"trailing"));
    assert!(!ts_locale::searchstoplist(&sl, b"dog"));
}
