use super::*;

fn items(input: &[u8]) -> Vec<(String, String, Option<i64>)> {
    let ctx = MemoryContext::new("deflist-test");
    let parsed = deserialize_deflist(ctx.mcx(), input).expect("parse");
    let out = parsed
        .iter()
        .map(|i| {
            (
                String::from_utf8_lossy(&i.name).into_owned(),
                String::from_utf8_lossy(&i.value).into_owned(),
                i.int_value,
            )
        })
        .collect();
    out
}

#[test]
fn deflist_quoted_and_unquoted() {
    assert_eq!(
        items(b"synonyms = 'synonym_sample'"),
        vec![("synonyms".into(), "synonym_sample".into(), None)]
    );
    assert_eq!(
        items(b"casesensitive = 1, synonyms = 'x'"),
        vec![
            ("casesensitive".into(), "1".into(), Some(1)),
            ("synonyms".into(), "x".into(), None)
        ]
    );
    assert_eq!(items(b"accept = off"), vec![("accept".into(), "off".into(), None)]);
    assert_eq!(items(b"k = E'a''b'"), vec![("k".into(), "a'b".into(), None)]);
    assert_eq!(items(b"\"Quoted Key\" = \"va\"\"l\""), vec![(
        "Quoted Key".into(),
        "va\"l".into(),
        None
    )]);
    assert_eq!(items(b"  "), vec![]);
}

#[test]
fn deflist_integer_normalization() {
    assert_eq!(items(b"n = 007")[0], ("n".into(), "7".into(), Some(7)));
    assert_eq!(items(b"n = +42")[0], ("n".into(), "42".into(), Some(42)));
    assert_eq!(items(b"n = '1'")[0], ("n".into(), "1".into(), None));
    assert_eq!(items(b"n = 1.5")[0], ("n".into(), "1.5".into(), None));
}

#[test]
fn deflist_bad_format() {
    let ctx = MemoryContext::new("deflist-test");
    for bad in [&b"k v"[..], b"k = 'unterminated", b"k ="] {
        assert!(deserialize_deflist(ctx.mcx(), bad).is_err(), "{:?}", bad);
    }
}

#[test]
fn def_get_boolean_matrix() {
    use ts_locale::dict_api::def_get_boolean;
    assert!(def_get_boolean(b"x", b"1", Some(1)).unwrap());
    assert!(!def_get_boolean(b"x", b"0", Some(0)).unwrap());
    assert!(def_get_boolean(b"x", b"true", None).unwrap());
    assert!(!def_get_boolean(b"x", b"off", None).unwrap());
    assert!(def_get_boolean(b"x", b"2", Some(2)).is_err());
    // Quoted '1' is a String node in C: not accepted.
    assert!(def_get_boolean(b"x", b"1", None).is_err());
    let err = def_get_boolean(b"casesensitive", b"2", Some(2)).unwrap_err();
    assert_eq!(err.message(), "casesensitive requires a Boolean value");
}
