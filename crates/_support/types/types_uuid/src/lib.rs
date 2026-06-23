//! Vocabulary types for `utils/adt/uuid.c` (the built-in `uuid` datatype).
//!
//! `uuid` is a fixed-length, pass-by-reference type (`pg_type.dat`:
//! `typlen = 16, typbyval = f`): its in-memory / on-disk image is exactly the
//! 16 [`pg_uuid_t::data`] bytes, with NO varlena length header.
//!
//! The sortsupport scratch ([`uuid_sortsupport_state`]) lives here because it
//! carries a [`HyperLogLog`] cardinality estimator; the abbreviation kernels in
//! `backend-utils-adt-uuid` build and mutate it directly.

#![allow(non_camel_case_types)]
#![forbid(unsafe_code)]

use ::nodes::nodeagg::HyperLogLog;

/// `UUID_LEN` (utils/uuid.h): the number of bytes in a UUID.
pub const UUID_LEN: usize = 16;

/// `struct pg_uuid_t` (utils/uuid.h): storage for the `uuid` type.
///
/// ```text
/// typedef struct pg_uuid_t { unsigned char data[UUID_LEN]; } pg_uuid_t;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct pg_uuid_t {
    pub data: [u8; UUID_LEN],
}

const _: () = assert!(core::mem::size_of::<pg_uuid_t>() == 16);
const _: () = assert!(core::mem::align_of::<pg_uuid_t>() == 1);

/// `uuid_sortsupport_state` (uuid.c:60-66): the `ssup_extra` scratch the
/// abbreviated-key optimization keeps for the `uuid` SortSupport.
#[derive(Debug)]
pub struct uuid_sortsupport_state<'mcx> {
    /// `int64 input_count` — number of non-null values seen.
    pub input_count: i64,
    /// `bool estimating` — true if still estimating cardinality.
    pub estimating: bool,
    /// `hyperLogLogState abbr_card` — cardinality estimator.
    pub abbr_card: HyperLogLog<'mcx>,
}
