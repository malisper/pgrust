use mcx::{Mcx, PgVec};
use types_error::PgResult;

// C signals "no conversion required" by returning the caller's pointer;
// identity does not cross a seam: Ok(None) = caller's bytes stand,
// Ok(Some(v)) = converted bytes (len = C's strlen(p), no trailing NUL) in mcx.

seam_core::seam!(
    pub fn pg_server_to_client<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    pub fn pg_client_to_server<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);
