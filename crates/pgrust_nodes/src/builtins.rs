use crate::parsenodes::{SqlType, SqlTypeKind};

pub use pgrust_catalog_ids::{
    builtin_aggregate_function_for_proc_oid, builtin_hypothetical_aggregate_function_for_proc_oid,
    builtin_ordered_set_aggregate_function_for_proc_oid, builtin_scalar_function_for_proc_oid,
    builtin_window_function_for_proc_oid, proc_oid_for_builtin_aggregate_function,
    proc_oid_for_builtin_hypothetical_aggregate_function,
    proc_oid_for_builtin_ordered_set_aggregate_function, proc_oid_for_builtin_scalar_function,
    proc_oid_for_builtin_window_function,
};

pub fn builtin_sql_type_for_oid(oid: u32) -> Option<SqlType> {
    match oid {
        pgrust_core::XID8_TYPE_OID => {
            Some(SqlType::new(SqlTypeKind::Int8).with_identity(pgrust_core::XID8_TYPE_OID, 0))
        }
        pgrust_core::RECORD_TYPE_OID => Some(SqlType::record(pgrust_core::RECORD_TYPE_OID)),
        _ => None,
    }
}
