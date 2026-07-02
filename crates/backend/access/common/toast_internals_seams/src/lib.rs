use mcx::{Mcx, PgVec};
use types_error::PgResult;

// Owner: the future toast unit (toast_internals.c + tableam fetch_toast_slice).
// Input is the on-disk external TOAST pointer image (va_header 0x01, tag 18);
// output is the reassembled varlena image (4B header, still compressed when
// the pointer says so), charged to `mcx` — C's static toast_fetch_datum /
// toast_fetch_datum_slice in detoast.c, seamed here because the chunk fetch
// needs table_open + systable machinery. Uninstalled call = loud panic.

seam_core::seam!(
    pub fn toast_fetch_datum<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    pub fn toast_fetch_datum_slice<'mcx>(
        mcx: Mcx<'mcx>,
        attr: &[u8],
        sliceoffset: i32,
        slicelength: i32,
    ) -> PgResult<PgVec<'mcx, u8>>
);
