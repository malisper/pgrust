use mcx::Mcx;
use parser_small1::ParseState;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::rawnodes::TypeName;

seam_core::seam!(
    // typenameTypeIdAndMod (parse_type.c home; body hosted by parse_utilcmd).
    pub fn typename_type_id_and_mod<'a, 'p, 'mcx>(
        mcx: Mcx<'mcx>,
        pstate: Option<&'a ParseState<'p, 'mcx>>,
        tn: &'a TypeName<'a>,
    ) -> PgResult<(Oid, i32)>
);
