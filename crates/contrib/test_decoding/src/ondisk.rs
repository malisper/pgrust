//! On-disk tuple-image helpers for the change callback.
//!
//! The decoded change carries each tuple as a `DecodedTuple { t_len, t_self,
//! t_table_oid, data }` where `data` is the full contiguous on-disk image
//! (`HeapTupleHeaderData` + null bitmap + user data) — exactly the bytes C's
//! `change->data.tp.newtuple->tuple` points its `t_data` at. We rebuild a
//! deformable [`FormedTuple`] from it so `heap_getattr` can read each column.

use ::types_error::PgResult;
use types_tuple::heaptuple::FormedTuple;

use ::reorderbuffer_seams::DecodedTuple;

/// Rebuild a deformable [`FormedTuple`] from the decoded on-disk tuple image.
/// `read_on_page_full` decodes the fixed header, captures the null bitmap, and
/// carries the user-data area alongside — the owned rendering of C's
/// `loctup.t_data = (HeapTupleHeader) <bytes>; loctup.t_len = t_len`.
pub fn formed_tuple_from_decoded<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    tuple: &DecodedTuple,
) -> PgResult<FormedTuple<'mcx>> {
    let block = types_core::primitive::InvalidBlockNumber;
    let offset = 0u16; // InvalidOffsetNumber; the plugin never reads t_self.
    FormedTuple::read_on_page_full(mcx, &tuple.data, block, offset, tuple.t_table_oid)
}

/// `VARATT_IS_EXTERNAL_ONDISK(ptr)` (postgres.h) — true when the by-reference
/// value is a 1-byte-header external TOAST pointer whose tag is
/// `VARTAG_ONDISK`. The first byte is the varatt tag byte (`0x01` ==
/// `VARATT_IS_EXTERNAL` short header marker); the second byte is the
/// `va_tag` for an external datum.
pub fn varatt_is_external_ondisk(bytes: &[u8]) -> bool {
    // VARATT_IS_EXTERNAL(ptr): VARATT_IS_1B_E(ptr) -> first byte == 0x01.
    // VARTAG_EXTERNAL(ptr) == VARTAG_ONDISK (18) for an on-disk external datum.
    const VARTAG_ONDISK: u8 = 18;
    if bytes.len() < 2 {
        return false;
    }
    bytes[0] == 0x01 && bytes[1] == VARTAG_ONDISK
}
