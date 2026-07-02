use types_core::Buffer;
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    pub fn heap_page_prune_opt<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        buffer: Buffer,
    ) -> PgResult<()>
);
