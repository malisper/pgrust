// sharedtuplestore.c sts_initialize guard parity (audit-18.6 b054): the
// name-length and meta-data-size checks are elog(ERROR)s in C — a caller
// that trips them gets a catchable backend error, never a Rust panic.
use ::types_error::ERRCODE_INTERNAL_ERROR;

use super::*;

// sharedtuplestore.c:152 — elog(ERROR, "meta-data too long") when
// meta_data_size + sizeof(uint32) >= STS_CHUNK_DATA_SIZE.
#[test]
fn oversized_meta_data_is_an_error_not_a_panic() {
    let too_long = STS_CHUNK_DATA_SIZE - core::mem::size_of::<u32>();
    let outcome = std::panic::catch_unwind(|| SharedTuplestore::new(1, too_long, "b054").map(|_| ()));
    let result = outcome.expect("sts_initialize must elog(ERROR), not panic, on oversized meta-data");
    let err = result.expect_err("oversized meta-data must be refused");
    assert_eq!(err.message(), "meta-data too long");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    // One byte under the limit is accepted (C: `>=` on meta + sizeof(uint32)).
    assert!(SharedTuplestore::new(1, too_long - 1, "b054").is_ok());
}

// sharedtuplestore.c:142 — elog(ERROR, "SharedTuplestore name too long")
// when strlen(name) > NAMEDATALEN - 1 (63).
#[test]
fn overlong_name_is_refused() {
    let ok_name = "n".repeat(63);
    assert!(SharedTuplestore::new(1, core::mem::size_of::<u32>(), &ok_name).is_ok());
    let long_name = "n".repeat(64);
    let err = SharedTuplestore::new(1, core::mem::size_of::<u32>(), &long_name)
        .err()
        .expect("a 64-byte name must be refused");
    assert_eq!(err.message(), "SharedTuplestore name too long");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}
