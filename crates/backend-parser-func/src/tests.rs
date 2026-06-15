//! Unit tests for backend-parser-func leaf helpers (parse_func.c).

use super::*;

#[test]
fn funcname_signature_string_basic() {
    let s = funcname_signature_string("foo", 0, &[], &[]).unwrap();
    assert_eq!(s, "foo()");
}
