//! Seam declarations for `src/common/string.c` (a `common-batch*` catalog
//! unit). The owning unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.

use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;

seam_core::seam!(
    /// `pg_clean_ascii(str, alloc_flags)` (`common/string.c`) — return a copy
    /// of `str` with every non-printable / non-ASCII byte replaced by a
    /// `"\xXX"` escape, allocated in `mcx` (backend builds use `palloc`;
    /// `alloc_flags` selects the allocator behavior, e.g. `MCXT_ALLOC_NO_OOM`).
    /// `Err` carries the allocation OOM (`ereport(ERROR)` for the palloc).
    pub fn pg_clean_ascii<'mcx>(
        mcx: Mcx<'mcx>,
        s: &str,
        alloc_flags: i32,
    ) -> PgResult<PgString<'mcx>>
);
