#![allow(non_snake_case)]

seam_core::seam!(
    // LookupTypeNameOid (parse_type.c) over a grammar TypeName.
    pub fn LookupTypeNameOid<'a, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tn: &'a types_nodes::rawnodes::TypeName<'a>,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    // parseTypeString (parse_type.c), NULL escontext: (type Oid, typmod).
    pub fn parseTypeString<'a, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        s: &'a str,
    ) -> types_error::PgResult<(types_core::Oid, i32)>
);
