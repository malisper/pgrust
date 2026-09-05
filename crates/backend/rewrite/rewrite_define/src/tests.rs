// audit-18.6 remediation b071: unit pins for the internal-reach rows.
use super::*;

// Signature-agnostic shim: `relkind_not_supported_detail` returned a bare
// `&'static str` (and panicked on an unknown relkind) before the fix and a
// `PgResult<&'static str>` after it; both shapes fold into one outcome.
trait DetailOutcome {
    fn into_outcome(self) -> PgResult<&'static str>;
}
impl DetailOutcome for &'static str {
    fn into_outcome(self) -> PgResult<&'static str> {
        Ok(self)
    }
}
impl DetailOutcome for PgResult<&'static str> {
    fn into_outcome(self) -> PgResult<&'static str> {
        self
    }
}

// pg_class.c:49 errdetail_relkind_not_supported: an unknown relkind is
// elog(ERROR, "unrecognized relkind: '%c'") — a catchable XX000, never a
// process-killing panic.
#[test]
fn unrecognized_relkind_is_elog_error_not_panic() {
    let outcome = std::panic::catch_unwind(|| relkind_not_supported_detail(b'x').into_outcome());
    let outcome = outcome.expect("relkind_not_supported_detail must not panic on an unknown relkind");
    let err = outcome.expect_err("unknown relkind must be an error");
    assert_eq!(err.message(), "unrecognized relkind: 'x'");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    // The known relkinds keep C's DETAIL texts.
    assert_eq!(
        relkind_not_supported_detail(b'm').into_outcome().unwrap(),
        "This operation is not supported for materialized views."
    );
}

// name.c namestrcpy: strlcpy into NAMEDATALEN — a name of 63+ bytes is cut
// to NAMEDATALEN-1 bytes and NUL-terminated, never refused.
#[test]
fn name_image_truncates_like_namestrcpy() {
    let root = mcx::MemoryContext::new("b071-name-image");
    let mcx = root.mcx();
    let n = NAMEDATALEN as usize;
    let short = name_image(mcx, "r").unwrap();
    assert_eq!(short.len(), n);
    assert_eq!(&short[..2], b"r\0");
    let long = "a".repeat(n + 10);
    let img = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| name_image(mcx, &long)))
        .expect("name_image must not panic on a 64+-byte name")
        .unwrap();
    assert_eq!(img.len(), n);
    assert_eq!(&img[..n - 1], "a".repeat(n - 1).as_bytes());
    assert_eq!(img[n - 1], 0);
}
