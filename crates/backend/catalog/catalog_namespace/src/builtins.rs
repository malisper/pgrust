use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::visibility::{
    FunctionIsVisibleExt, OpclassIsVisibleExt, OperatorIsVisibleExt, OpfamilyIsVisibleExt,
    RelationIsVisibleExt, StatisticsObjIsVisibleExt, TypeIsVisibleExt,
};

macro_rules! fc_is_visible {
    ($($fname:ident: $ext:ident;)*) => {$(
        // C: is_missing => PG_RETURN_NULL().
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            match $ext(fcinfo.arg_oid(0))? {
                Some(v) => Ok(Datum::from_bool(v)),
                None => Ok(fcinfo.return_null()),
            }
        }
    )*};
}

fc_is_visible! {
    fc_pg_table_is_visible: RelationIsVisibleExt;
    fc_pg_type_is_visible: TypeIsVisibleExt;
    fc_pg_function_is_visible: FunctionIsVisibleExt;
    fc_pg_operator_is_visible: OperatorIsVisibleExt;
    fc_pg_opclass_is_visible: OpclassIsVisibleExt;
    fc_pg_opfamily_is_visible: OpfamilyIsVisibleExt;
    fc_pg_statistics_obj_is_visible: StatisticsObjIsVisibleExt;
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const NAMESPACE_BUILTINS: &[FmgrBuiltin] = &[
    b(2079, "pg_table_is_visible", 1, fc_pg_table_is_visible),
    b(2080, "pg_type_is_visible", 1, fc_pg_type_is_visible),
    b(2081, "pg_function_is_visible", 1, fc_pg_function_is_visible),
    b(2082, "pg_operator_is_visible", 1, fc_pg_operator_is_visible),
    b(2083, "pg_opclass_is_visible", 1, fc_pg_opclass_is_visible),
    b(3403, "pg_statistics_obj_is_visible", 1, fc_pg_statistics_obj_is_visible),
    b(3829, "pg_opfamily_is_visible", 1, fc_pg_opfamily_is_visible),
];
