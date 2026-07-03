use types_error::PgResult;
use types_rel::Relation;

seam_core::seam!(
    // cluster_rel (cluster.c) for VACUUM FULL; consumes old_heap, keeps lock.
    pub fn cluster_rel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        old_heap: Relation<'mcx>,
        index_oid: types_core::Oid,
        options: u32,
    ) -> PgResult<()>
);
