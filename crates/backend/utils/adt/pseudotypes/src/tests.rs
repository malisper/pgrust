use mcx::MemoryContext;
use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;

use crate::*;

#[test]
fn dummy_stubs_carry_0a000_and_exact_messages() {
    let err = any_in().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot accept a value of type any");

    let err = trigger_out().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot display a value of type trigger");

    let err = anyarray_in().unwrap_err();
    assert_eq!(err.message(), "cannot accept a value of type anyarray");
    let err = anyarray_recv().unwrap_err();
    assert_eq!(err.message(), "cannot accept a value of type anyarray");

    let err = pg_ddl_command_send().unwrap_err();
    assert_eq!(err.message(), "cannot display a value of type pg_ddl_command");

    let err = shell_in().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot accept a value of a shell type");
    let err = shell_out().unwrap_err();
    assert_eq!(err.message(), "cannot display a value of a shell type");
}

#[test]
fn cstring_io_round_trip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(&cstring_in(mcx, b"blah").unwrap()[..], b"blah\0");
    assert_eq!(&cstring_out(mcx, b"blah\0trail").unwrap()[..], b"blah\0");
    assert_eq!(&cstring_in(mcx, b"").unwrap()[..], b"\0");
}

#[test]
fn void_io() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(void_in().as_usize(), 0);
    assert_eq!(void_recv().as_usize(), 0);
    assert_eq!(&void_out(mcx).unwrap()[..], b"\0");
    assert_eq!(void_send(mcx).unwrap().data(), b"");
}

#[test]
fn pg_node_tree_out_passes_text_through() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(&pg_node_tree_out(mcx, b"{QUERY :junk 1}").unwrap()[..], b"{QUERY :junk 1}\0");
}

#[test]
#[should_panic(expected = "anyarray_out: delegates to array_out (arrayfuncs not ported)")]
fn unported_delegates_are_loud() {
    anyarray_out();
}

#[test]
fn builtins_table_is_oid_ascending() {
    let t = builtins::PSEUDOTYPES_BUILTINS;
    assert_eq!(t.len(), 48);
    for w in t.windows(2) {
        assert!(w[0].foid < w[1].foid, "{} !< {}", w[0].foid, w[1].foid);
    }
}
