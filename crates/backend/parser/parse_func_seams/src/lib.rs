#![allow(non_snake_case)]

seam_core::seam!(
    // LookupFuncWithArgs (parse_func.c); objtype is the parsenodes.h
    // ObjectType discriminant (OBJECT_FUNCTION/PROCEDURE/ROUTINE).
    pub fn LookupFuncWithArgs<'a, 'mcx>(
        objtype: i32,
        func: &'a types_nodes::parsenodes::ObjectWithArgs<'mcx>,
        missing_ok: bool,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    // LookupFuncName (parse_func.c): exact-signature lookup over dotted name
    // parts; nargs == -1 means any-arity (unique-or-error).
    pub fn LookupFuncName<'a>(
        parts: &'a [&'a str],
        nargs: i16,
        argtypes: &'a [types_core::Oid],
        missing_ok: bool,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    // expandRecordVariable (parse_target); a direct parse_func -> parse_target
    // dep would cycle through parse_expr. Installed by parse_target.
    pub fn expandRecordVariable<'a, 'p, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstate: &'a parser_small1::ParseState<'p, 'mcx>,
        var_node: types_nodes::Node<'mcx>,
        levelsup: i32,
    ) -> types_error::PgResult<types_tuple::tupdesc::TupleDescData<'mcx>>
);

seam_core::seam!(
    // transformExpressionList (parse_target.c); ROW() and VALUES item lists
    // expand `x.*` stars, which needs parse_target's expansion machinery
    // above parse_expr. Installed by parse_target.
    pub fn transformExpressionList<'a, 'p, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstate: &'a mut parser_small1::ParseState<'p, 'mcx>,
        exprlist: &'a types_nodes::NodeList<'mcx>,
        exprKind: parser_small1::ParseExprKind,
        allowDefault: bool,
    ) -> types_error::PgResult<types_nodes::NodeList<'mcx>>
);
