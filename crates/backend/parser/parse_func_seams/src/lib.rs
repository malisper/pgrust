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
