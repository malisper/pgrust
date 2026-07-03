use super::*;

#[test]
fn sizes() {
    for (input, expect) in [
        ("1", 1),
        ("1 kB", 1024),
        ("  +1.5  MB ", 1572864),
        ("-1 GB", -1073741824),
        ("1TB", 1099511627776),
        ("1 PB", 1125899906842624),
        ("1e3 kB", 1024000),
        ("1B", 1),
        ("1 bytes", 1),
        (".5 kb", 512),
    ] {
        assert_eq!(pg_size_bytes(input).unwrap(), expect, "{input}");
    }
}

#[test]
fn errors() {
    assert!(pg_size_bytes("").is_err());
    assert!(pg_size_bytes("kB").is_err());
    let e = pg_size_bytes("1 xB").unwrap_err();
    assert!(e.to_string().contains("invalid size"), "{e}");
    assert!(pg_size_bytes("1 EB").is_err());
}
