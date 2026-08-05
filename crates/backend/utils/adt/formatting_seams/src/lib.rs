//! Seam declarations for `utils/adt/formatting.c`: locale-aware case folding
//! consumed by the (unported) tsearch dictionaries.

use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `str_tolower(buff, nbytes, collid)` (formatting.c): lowercase `buff` under
    /// collation `collid` in the database encoding; folded bytes (no trailing
    /// NUL) allocated in `mcx`.
    pub fn str_tolower<'mcx>(
        mcx: Mcx<'mcx>,
        buff: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);
