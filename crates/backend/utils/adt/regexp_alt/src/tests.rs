use super::*;
use ::mcx::MemoryContext;
use ::regex_spencer::REG_ADVANCED;
use ::types_core::C_COLLATION_OID;

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        postgres_seams::check_for_interrupts::set(|| Ok(()));
    });
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
}

fn engines() -> Vec<i32> {
    let mut v = vec![REGEX_ENGINE_RUST];
    if cfg!(have_re2) {
        v.push(REGEX_ENGINE_RE2);
    }
    v
}

fn replace(engine: i32, s: &str, p: &str, r: &str, start: i32, n: i32) -> String {
    let cx = MemoryContext::new("test");
    let out = replace_text_regexp_alt(
        cx.mcx(),
        s.as_bytes(),
        p.as_bytes(),
        r.as_bytes(),
        REG_ADVANCED,
        C_COLLATION_OID,
        start,
        n,
        engine,
    )
    .unwrap();
    String::from_utf8(out.as_slice().to_vec()).unwrap()
}

const Q29_PAT: &str = r"^https?://(?:www\.)?([^/]+)/.*$";

#[test]
fn q29_shape() {
    setup();
    for e in engines() {
        assert_eq!(replace(e, "http://www.example.com/path/x?y=1", Q29_PAT, r"\1", 0, 1), "example.com");
        assert_eq!(replace(e, "https://sub.host.ru/", Q29_PAT, r"\1", 0, 1), "sub.host.ru");
        assert_eq!(replace(e, "not-a-url", Q29_PAT, r"\1", 0, 1), "not-a-url");
        assert_eq!(replace(e, "http://hostonly.com", Q29_PAT, r"\1", 0, 1), "http://hostonly.com");
        assert_eq!(replace(e, "", Q29_PAT, r"\1", 0, 1), "");
    }
}

#[test]
fn replacement_escapes() {
    setup();
    for e in engines() {
        assert_eq!(replace(e, "abc def", r"(\w+) (\w+)", r"\2 \1", 0, 1), "def abc");
        assert_eq!(replace(e, "xay", "a", r"[\&]", 0, 1), "x[a]y");
        assert_eq!(replace(e, "xay", "a", r"\\", 0, 1), "x\\y");
        // Unknown escape keeps the backslash (PG behavior).
        assert_eq!(replace(e, "xay", "a", r"\z", 0, 1), "x\\zy");
        // Trailing lone backslash.
        assert_eq!(replace(e, "xay", "a", "b\\", 0, 1), "xb\\y");
        // Group that did not participate appends nothing.
        assert_eq!(replace(e, "foo", "foo(bar)?", r"[\1]", 0, 1), "[]");
        // Group index beyond the pattern's groups appends nothing.
        assert_eq!(replace(e, "foo", "(f)oo", r"\9x", 0, 1), "x");
    }
}

#[test]
fn glob_nth_start() {
    setup();
    for e in engines() {
        // n == 0: replace all.
        assert_eq!(replace(e, "a1b2c3", r"\d", "#", 0, 0), "a#b#c#");
        // n-th match only.
        assert_eq!(replace(e, "a1b2c3", r"\d", "#", 0, 2), "a1b#c3");
        // start offset (characters).
        assert_eq!(replace(e, "a1b2c3", r"\d", "#", 2, 1), "a1b#c3");
        // Empty matches advance one character.
        assert_eq!(replace(e, "abc", "x*", "-", 0, 0), "-a-b-c-");
        // Multibyte: empty-match advance is per character, not per byte.
        assert_eq!(replace(e, "é", "x*", "-", 0, 0), "-é-");
    }
}

#[test]
fn compile_errors_name_engine() {
    setup();
    let cx = MemoryContext::new("test");
    // Backreference: rust engine must refuse with a clear error.
    let err = replace_text_regexp_alt(
        cx.mcx(), b"aa", br"(a)\1", b"x", REG_ADVANCED, C_COLLATION_OID, 0, 1,
        REGEX_ENGINE_RUST,
    )
    .unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("regex_engine=rust"), "{msg}");
}

#[test]
fn guc_backing_is_session_scoped() {
    set_regex_engine(REGEX_ENGINE_RUST);
    std::thread::spawn(|| assert_eq!(regex_engine(), REGEX_ENGINE_SPENCER)).join().unwrap();
    assert_eq!(regex_engine(), REGEX_ENGINE_RUST);
    set_regex_engine(REGEX_ENGINE_SPENCER);
}
