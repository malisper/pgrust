use types_core::{Buffer, OffsetNumber};
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    pub fn heap_page_prune_opt<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        buffer: Buffer,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn heap_page_prune_execute<'a>(
        buffer: Buffer,
        lp_truncate_only: bool,
        redirected: &'a [OffsetNumber],
        nowdead: &'a [OffsetNumber],
        nowunused: &'a [OffsetNumber],
    )
);
