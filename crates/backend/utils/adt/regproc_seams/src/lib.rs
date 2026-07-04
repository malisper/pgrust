seam_core::seam!(
    pub fn parse_type_string<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typename: &str,
    ) -> types_error::PgResult<(types_core::Oid, i32)>
);
