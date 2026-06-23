//! Seam declarations for the `backend-utils-adt-formatting` unit
//! (`utils/adt/formatting.c`): locale-aware case folding.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `str_tolower(buff, nbytes, collid)` (formatting.c): lowercase the first
    /// `buff.len()` bytes under collation `collid`, in the database encoding.
    /// C palloc's a fresh NUL-terminated result; the seam allocates the folded
    /// bytes (no trailing NUL) in `mcx`. Collation/locale lookup errors and OOM
    /// surface on `Err`.
    pub fn str_tolower<'mcx>(
        mcx: Mcx<'mcx>,
        buff: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);
