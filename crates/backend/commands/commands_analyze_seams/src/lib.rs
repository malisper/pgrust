use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::NodeList;

seam_core::seam!(
    pub fn analyze_rel<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        relname: Option<&'a str>,
        va_cols: &'a NodeList<'mcx>,
        options: u32,
        // VacuumParams.log_min_duration (autovacuum's instrument threshold;
        // -1 for a manual ANALYZE).
        log_min_duration: i32,
        in_outer_xact: bool,
    ) -> PgResult<()>
);
