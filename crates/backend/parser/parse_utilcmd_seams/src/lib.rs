#![allow(non_snake_case)]

use mcx::Mcx;
use parser_small1::ParseState;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::rawnodes::TypeName;

seam_core::seam!(
    // LookupTypeNameOid (parse_type.c) over a grammar TypeName.
    pub fn LookupTypeNameOid<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        tn: &'a TypeName<'a>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // parseTypeString (parse_type.c), NULL escontext: (type Oid, typmod).
    pub fn parseTypeString<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        s: &'a str,
    ) -> PgResult<(Oid, i32)>
);

seam_core::seam!(
    // typenameTypeIdAndMod (parse_type.c home; body hosted by parse_utilcmd).
    pub fn typename_type_id_and_mod<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: Option<&'a ParseState<'p, 'mcx>>,
        tn: &'a TypeName<'a>,
    ) -> PgResult<(Oid, i32)>
);

seam_core::seam!(
    // typenameTypeIdAndMod without the host's column-lane typtype gate (C has
    // none); RTE-function coldeflists take record/record[] and defer legality
    // to CheckAttributeNamesTypes(CHKATYPE_ANYRECORD).
    pub fn typename_type_id_and_mod_any<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: Option<&'a ParseState<'p, 'mcx>>,
        tn: &'a TypeName<'a>,
    ) -> PgResult<(Oid, i32)>
);
