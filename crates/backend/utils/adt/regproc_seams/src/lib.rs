seam_core::seam!(
    // parseTypeString (parse_type.c): None = soft error captured into esc.
    pub fn parse_type_string<'a, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typename: &'a str,
        esc: Option<&'a mut types_error::SoftErrorContext>,
    ) -> types_error::PgResult<Option<(types_core::Oid, i32)>>
);
