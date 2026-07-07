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
    dispatch(p.as_bytes(), REG_ADVANCED).unwrap()
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
        assert!(dispatch(p.as_bytes(), REG_ADVANCED).unwrap().is_none(), "{p}");
    }
    // Classifier-admitted but RE2-rejected patterns also fail closed.
    // (POSIX leading-] brackets are rejected upstream by the classifier; use
    // forced mode to confirm compile errors surface only when forced.)
    set_regex_engine(REGEX_ENGINE_SPENCER);
    assert!(dispatch(b"anything(?=x)", REG_ADVANCED).unwrap().is_none());
}

#[test]
fn forced_re2_errors_name_engine() {
    setup();
    if !re2_available() {
        return;
    }
    set_regex_engine(REGEX_ENGINE_RE2);
    let err = dispatch(br"(a)\1", REG_ADVANCED).unwrap_err();
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
    let re = dispatch(br"a.c", ::regex_spencer::REG_QUOTE).unwrap().expect("quoted dispatches");
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
    let a = dispatch(Q29_PAT.as_bytes(), REG_ADVANCED).unwrap().unwrap();
    let b = dispatch(Q29_PAT.as_bytes(), REG_ADVANCED).unwrap().unwrap();
    // Same Rc-backed compiled pattern comes back from the cache.
    assert!(Rc::ptr_eq(&a.inner, &b.inner));
    // Spencer verdicts are cached too.
    assert!(dispatch(br"\d", REG_ADVANCED).unwrap().is_none());
    assert!(dispatch(br"\d", REG_ADVANCED).unwrap().is_none());
}

#[test]
fn guc_backing_is_session_scoped() {
    set_regex_engine(REGEX_ENGINE_SPENCER);
    std::thread::spawn(|| assert_eq!(regex_engine(), REGEX_ENGINE_AUTO)).join().unwrap();
    assert_eq!(regex_engine(), REGEX_ENGINE_SPENCER);
    set_regex_engine(REGEX_ENGINE_AUTO);
}
