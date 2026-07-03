use mcx::Mcx;
use tableam_vocab::VacuumParams;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::buf::BufferAccessStrategy;

seam_core::seam!(
    pub fn table_relation_vacuum<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        rel: &'a RelationData<'mcx>,
        params: &'a VacuumParams,
        bstrategy: BufferAccessStrategy,
    ) -> PgResult<()>
);
