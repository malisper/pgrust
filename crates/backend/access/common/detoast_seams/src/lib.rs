use mcx::{Mcx, PgVec};
use types_error::PgResult;

// Owner: the future backend-access-common-detoast unit (detoast.c). Input is
// the full varlena image bytes (external TOAST pointer or 4B-compressed);
// output is the detoasted plain 4B-header image, charged to `mcx`. Uninstalled
// call = loud panic — never a silent slow path (AGENTS.md rule 5).

seam_core::seam!(
    pub fn detoast_attr<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<PgVec<'mcx, u8>>
);
