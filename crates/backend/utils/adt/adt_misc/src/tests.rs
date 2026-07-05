use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::TEXTOID;
use types_error::PgResult;
use types_fmgr::LocalFcinfo;

use crate::builtins::fc_parse_ident;

fn run(mcx: Mcx<'_>, input: &str, strict: bool) -> PgResult<Vec<String>> {
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    // SAFETY: mcx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    let text = varlena::cstring_to_text(mcx, input.as_bytes()).unwrap();
    fcinfo.set_arg(0, Datum::from_usize(text.as_bytes().as_ptr() as usize));
    fcinfo.set_arg(1, Datum::from_bool(strict));
    let d = fc_parse_ident(None, &mut fcinfo)?;
    let p = d.as_usize() as *const u8;
    let img = unsafe { core::slice::from_raw_parts(p, arrayfuncs::foundation::varsize_any(p)) };
    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, img, TEXTOID, true).unwrap();
    Ok(elems
        .iter()
        .zip(nulls.iter())
        .map(|(&e, &isnull)| {
            assert!(!isnull);
            let p = e.as_usize() as *const u8;
            let bytes = unsafe {
                core::slice::from_raw_parts(p.add(4), arrayfuncs::foundation::varsize_any(p) - 4)
            };
            String::from_utf8(bytes.to_vec()).unwrap()
        })
        .collect())
}

#[test]
fn parse_ident_unquoted_downcases() {
    let ctx = MemoryContext::new("t");
    let parts = run(ctx.mcx(), "Foo.Bar", true).unwrap();
    assert_eq!(parts, vec!["foo", "bar"]);
}

#[test]
fn parse_ident_quoted_preserves_case() {
    let ctx = MemoryContext::new("t");
    let parts = run(ctx.mcx(), "\"MixedCase\"", true).unwrap();
    assert_eq!(parts, vec!["MixedCase"]);
}

#[test]
fn parse_ident_strict_trailing_garbage_errors() {
    let ctx = MemoryContext::new("t");
    let err = run(ctx.mcx(), "foo.bar!", true).unwrap_err();
    assert_eq!(err.message, "string is not a valid identifier: \"foo.bar!\"");
}

#[test]
fn parse_ident_nonstrict_tolerates_trailing_garbage() {
    let ctx = MemoryContext::new("t");
    let parts = run(ctx.mcx(), "foo.bar!", false).unwrap();
    assert_eq!(parts, vec!["foo", "bar"]);
}

#[test]
fn parse_ident_invalid_after_dot_message() {
    let ctx = MemoryContext::new("t");
    let err = run(ctx.mcx(), "foo.", true).unwrap_err();
    assert_eq!(err.message, "string is not a valid identifier: \"foo.\"");
    assert_eq!(err.detail.as_deref(), Some("No valid identifier after \".\"."));
}
