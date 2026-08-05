use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

// C format_type (not strict): NULL type_oid => NULL; NULL typemod => the
// format_type_be path; always FORMAT_TYPE_ALLOW_INVALID.
pub fn fc_format_type(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let type_oid = fcinfo.arg_oid(0);
    let (typemod, flags) = if fcinfo.argisnull(1) {
        (-1, crate::FORMAT_TYPE_ALLOW_INVALID)
    } else {
        (
            fcinfo.arg_i32(1),
            crate::FORMAT_TYPE_ALLOW_INVALID | crate::FORMAT_TYPE_TYPEMOD_GIVEN,
        )
    };
    let s = crate::format_type_extended(type_oid, typemod, flags)?
        .expect("no FORMAT_TYPE_INVALID_AS_NULL");

    let mcx = fcinfo.result_mcx();
    let mut image = mcx::vec_with_capacity_in(mcx, datum::varlena::VARHDRSZ + s.len())?;
    image.resize(datum::varlena::VARHDRSZ, 0);
    mcx::vec_append_bytes(&mut image, s.as_bytes())?;
    Ok(varlena_result(datum::Varlena::from_image(image)))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict,
        retset: false,
    func,
    }
}

// pg_proc.dat row: format_type is not strict.
pub const FORMAT_TYPE_BUILTINS: &[FmgrBuiltin] = &[b(1081, "format_type", 2, false, fc_format_type)];
