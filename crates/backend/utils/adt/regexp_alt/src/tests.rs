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

#[test]
fn guc_backing_is_session_scoped() {
    set_regex_engine(REGEX_ENGINE_SPENCER);
    std::thread::spawn(|| assert_eq!(regex_engine(), REGEX_ENGINE_AUTO)).join().unwrap();
    assert_eq!(regex_engine(), REGEX_ENGINE_SPENCER);
    set_regex_engine(REGEX_ENGINE_AUTO);
}
