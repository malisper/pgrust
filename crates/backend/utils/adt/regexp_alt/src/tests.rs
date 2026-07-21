use super::*;
use ::mcx::MemoryContext;
use ::regex_spencer::REG_ADVANCED;

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        postgres_seams::check_for_interrupts::set(|| Ok(()));
    });
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
}

fn auto(p: &str) -> Option<Re2Pattern> {
    setup();
    set_regex_engine(REGEX_ENGINE_AUTO);
    dispatch(p.as_bytes(), REG_ADVANCED, b"clean subject").unwrap()
}

fn replace(p: &str, s: &str, r: &str, start: i32, n: i32) -> String {
    let re = auto(p).expect("pattern should dispatch to re2");
    let cx = MemoryContext::new("test");
    let out =
        replace_text_regexp_re2(cx.mcx(), &re, s.as_bytes(), r.as_bytes(), start, n).unwrap();
    String::from_utf8(out.as_slice().to_vec()).unwrap()
}

const Q29_PAT: &str = r"^https?://(?:www\.)?([^/]+)/.*$";

#[test]
fn q29_shape() {
    setup();
    if !re2_available() {
        return;
    }
    assert_eq!(replace(Q29_PAT, "http://www.example.com/path/x?y=1", r"\1", 0, 1), "example.com");
    assert_eq!(replace(Q29_PAT, "https://sub.host.ru/", r"\1", 0, 1), "sub.host.ru");
    assert_eq!(replace(Q29_PAT, "not-a-url", r"\1", 0, 1), "not-a-url");
    assert_eq!(replace(Q29_PAT, "http://hostonly.com", r"\1", 0, 1), "http://hostonly.com");
    assert_eq!(replace(Q29_PAT, "", r"\1", 0, 1), "");
}

#[test]
fn replacement_escapes() {
    setup();
    if !re2_available() {
        return;
    }
    assert_eq!(replace(r"([a-z]+) ([a-z]+)", "abc def", r"\2 \1", 0, 1), "def abc");
    assert_eq!(replace("a", "xay", r"[\&]", 0, 1), "x[a]y");
    assert_eq!(replace("a", "xay", r"\\", 0, 1), "x\\y");
    // Unknown escape keeps the backslash (PG behavior).
    assert_eq!(replace("a", "xay", r"\z", 0, 1), "x\\zy");
    // Trailing lone backslash.
    assert_eq!(replace("a", "xay", "b\\", 0, 1), "xb\\y");
    // Group that did not participate appends nothing.
    assert_eq!(replace("foo(bar)?", "foo", r"[\1]", 0, 1), "[]");
    // Group index beyond the pattern's groups appends nothing.
    assert_eq!(replace("(f)oo", "foo", r"\9x", 0, 1), "x");
}

#[test]
fn glob_nth_start() {
    setup();
    if !re2_available() {
        return;
    }
    // n == 0: replace all.
    assert_eq!(replace("[0-9]", "a1b2c3", "#", 0, 0), "a#b#c#");
    // n-th match only.
    assert_eq!(replace("[0-9]", "a1b2c3", "#", 0, 2), "a1b#c3");
    // start offset (characters).
    assert_eq!(replace("[0-9]", "a1b2c3", "#", 2, 1), "a1b#c3");
    // Empty matches advance one character.
    assert_eq!(replace("x*", "abc", "-", 0, 0), "-a-b-c-");
    // Multibyte: empty-match advance is per character, not per byte.
    assert_eq!(replace("x*", "é", "-", 0, 0), "-é-");
}

#[test]
fn longest_match_semantics() {
    setup();
    if !re2_available() {
        return;
    }
    // Spencer's all-greedy rule is leftmost-LONGEST; RE2 must run in POSIX
    // longest mode, not Perl leftmost-first, for the classes we admit.
    assert_eq!(replace("a|ab", "abc", "#", 0, 1), "#c");
    assert_eq!(replace("(a+|a)(b?)", "aab", r"[\1|\2]", 0, 1), "[aa|b]");
}

#[test]
fn auto_fails_closed() {
    setup();
    set_regex_engine(REGEX_ENGINE_AUTO);
    // Incompatible constructs classify to Spencer (None), never error.
    for p in [r"(a)\1", r"\w+", "a*?", "[[:alpha:]]"] {
        assert!(dispatch(p.as_bytes(), REG_ADVANCED, b"x").unwrap().is_none(), "{p}");
    }
    // Classifier-admitted but RE2-rejected patterns also fail closed.
    // (POSIX leading-] brackets are rejected upstream by the classifier; use
    // forced mode to confirm compile errors surface only when forced.)
    set_regex_engine(REGEX_ENGINE_SPENCER);
    assert!(dispatch(b"anything(?=x)", REG_ADVANCED, b"x").unwrap().is_none());
}

#[test]
fn auto_fails_closed_on_data() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_AUTO);
    let subjects: &[&[u8]] = &[
        b"\x00",
        b"\x00abc",
        b"abc\x00",
        b"ab\x00cd",
        b"caf\xc3\xa9\x00dcba",
        b"caf\xc3\x00dcba",
        b"caf\xc3",
        b"a\xffb",
        b"\xc3\x28",
        b"\xed\xa0\x80xyz",
        b"\xc0\xafabc",
    ];
    for s in subjects {
        assert!(!subject_compatible(s), "{s:?}");
        assert!(dispatch(b"a", REG_ADVANCED, s).unwrap().is_none(), "{s:?}");
    }
    for s in [&b""[..], b"abc", "café".as_bytes(), b"a\nb\tc"] {
        assert!(subject_compatible(s), "{s:?}");
        assert!(dispatch(b"a", REG_ADVANCED, s).unwrap().is_some(), "{s:?}");
    }
    // The data guard applies after the cached pattern verdict, per subject.
    assert!(dispatch(b"a", REG_ADVANCED, b"a\x00b").unwrap().is_none());
    assert!(dispatch(b"a", REG_ADVANCED, b"ab").unwrap().is_some());
}

#[test]
fn forced_re2_bypasses_data_guard() {
    setup();
    if !re2_available() {
        return;
    }
    // The testing knob exposes raw RE2 byte semantics, NUL data included —
    // this is what lets tests observe the divergence auto guards against.
    set_regex_engine(REGEX_ENGINE_RE2);
    let re = dispatch(b"b.d", REG_ADVANCED, b"ab\x00d!").unwrap();
    set_regex_engine(REGEX_ENGINE_AUTO);
    assert!(re.expect("forced re2 dispatches").is_match(b"ab\x00d!", 0));
}

#[test]
fn forced_re2_errors_name_engine() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_RE2);
    let err = dispatch(br"(a)\1", REG_ADVANCED, b"x").unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("regex_engine=re2"), "{msg}");
    set_regex_engine(REGEX_ENGINE_AUTO);
}

#[test]
fn quoted_mode_is_literal() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_AUTO);
    let re =
        dispatch(br"a.c", ::regex_spencer::REG_QUOTE, b"x").unwrap().expect("quoted dispatches");
    assert!(re.is_match(b"xa.cy", 0));
    assert!(!re.is_match(b"xabcy", 0));
}

#[test]
fn dispatch_decision_is_cached() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_AUTO);
    let a = dispatch(Q29_PAT.as_bytes(), REG_ADVANCED, b"x").unwrap().unwrap();
    let b = dispatch(Q29_PAT.as_bytes(), REG_ADVANCED, b"x").unwrap().unwrap();
    // Same Rc-backed compiled pattern comes back from the cache.
    assert!(Rc::ptr_eq(&a.inner, &b.inner));
    // Spencer verdicts are cached too.
    assert!(dispatch(br"\d", REG_ADVANCED, b"x").unwrap().is_none());
    assert!(dispatch(br"\d", REG_ADVANCED, b"x").unwrap().is_none());
}

// Pattern-program tier differential: for every subset pattern, program-on
// exec must agree with program-off (pure RE2) exec — return value AND every
// group span — across subjects incl. multibyte hosts and give-back shapes.
#[test]
fn pattern_program_vs_re2_differential() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_AUTO);

    let patterns: &[&str] = &[
        Q29_PAT,
        "^",
        "^$",
        "^abc",
        "^abc$",
        "^a?b",
        "^https?://",
        r"^(?:www\.)?([^/]+)$",
        "^([a-z]+)z$",
        "^([a-z]+)bc$",
        "^([^a]+)y$",
        "^([^a]+)[^b]+X$",
        "^[0-9]{4}-[0-9]{2}$",
        "^x*y+z?",
        "^a.*",
        "^a.+$",
        "^([0-9]{2,4})x",
        r"^\.\*",
        "^é?x",
        "^[^,]*,",
    ];
    let subjects: &[&str] = &[
        "",
        "http://www.example.com/path/x?y=1",
        "https://sub.host.ru/",
        "http://hostonly.com",
        "https://пример.рф/страница",
        "http://www/",
        "not-a-url",
        "abc",
        "abcz",
        "abcdef",
        "xéy",
        "xéX",
        "éxé",
        "www.x",
        "www",
        "1234-56",
        "0123x",
        "xxyyzz",
        "a",
        "ab",
        ".*x",
        ",,a,,b,,",
        "line one\nline two\n",
        "café instrument",
    ];

    for p in patterns {
        let re = dispatch(p.as_bytes(), REG_ADVANCED, b"clean").unwrap().unwrap_or_else(|| {
            panic!("pattern {p:?} should dispatch to re2");
        });
        assert!(
            program::compile(p.as_bytes()).is_some(),
            "pattern {p:?} should be in the program subset"
        );
        for s in subjects {
            for nout in [0usize, 1, 2, 10] {
                let mut prog_out = [(-7i64, -7i64); 10];
                let mut re2_out = [(-7i64, -7i64); 10];
                set_regex_pattern_program(true);
                let prog_m = re.exec(s.as_bytes(), 0, &mut prog_out[..nout]);
                set_regex_pattern_program(false);
                let re2_m = re.exec(s.as_bytes(), 0, &mut re2_out[..nout]);
                set_regex_pattern_program(true);
                assert_eq!(prog_m, re2_m, "match verdict diverges: {p:?} on {s:?}");
                if prog_m {
                    assert_eq!(
                        prog_out[..nout],
                        re2_out[..nout],
                        "group spans diverge: {p:?} on {s:?}"
                    );
                }
            }
        }
    }
}

// Pathological backtracking: the step budget must refuse (fall back inside
// exec to RE2, same answer), never wrong-answer or hang. The public exec
// must therefore agree with program-off exec even on budget-tripping input.
#[test]
fn pattern_program_budget_fallback() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_AUTO);
    let p = "^[ab]*[ab]*[ab]*[ab]*[ab]*[ab]*[ab]*[ab]*c$";
    let re = dispatch(p.as_bytes(), REG_ADVANCED, b"clean").unwrap().unwrap();
    assert!(program::compile(p.as_bytes()).is_some());
    let hay = "ab".repeat(64);
    // The raw program refuses (budget) …
    assert_eq!(program::compile(p.as_bytes()).unwrap().exec(hay.as_bytes(), &mut []), None);
    // … and the public exec still answers via RE2, matching program-off.
    set_regex_pattern_program(true);
    let on = re.exec(hay.as_bytes(), 0, &mut []);
    set_regex_pattern_program(false);
    let off = re.exec(hay.as_bytes(), 0, &mut []);
    set_regex_pattern_program(true);
    assert_eq!(on, off);
    assert!(!on);
}

// The tier engages for the exact anchored-URL pattern under auto dispatch, and the
// GUC turns it off without recompiling.
#[test]
fn pattern_program_attaches_for_q29() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_AUTO);
    let re = dispatch(Q29_PAT.as_bytes(), REG_ADVANCED, b"clean").unwrap().unwrap();
    assert!(re.has_program(), "Q29's pattern must compile to a pattern program");
    // Whole-match-tier and alternation patterns must NOT get a program.
    for p in ["^(foo|bar)$", "(a)+", "a|ab"] {
        if let Some(re) = dispatch(p.as_bytes(), REG_ADVANCED, b"clean").unwrap() {
            assert!(!re.has_program(), "{p:?} must not get a program");
        }
    }
}

#[test]
fn guc_backing_is_session_scoped() {
    set_regex_engine(REGEX_ENGINE_SPENCER);
    std::thread::spawn(|| assert_eq!(regex_engine(), REGEX_ENGINE_AUTO)).join().unwrap();
    assert_eq!(regex_engine(), REGEX_ENGINE_SPENCER);
    set_regex_engine(REGEX_ENGINE_AUTO);
}
