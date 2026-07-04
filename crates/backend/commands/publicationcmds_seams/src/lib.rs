use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn remove_publication_by_id<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<()>
);

seam_core::seam!(
    pub fn remove_publication_rel_by_id<'mcx>(mcx: Mcx<'mcx>, proid: Oid) -> PgResult<()>
);

seam_core::seam!(
    pub fn remove_publication_schema_by_id<'mcx>(mcx: Mcx<'mcx>, psoid: Oid) -> PgResult<()>
);
