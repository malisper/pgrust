//! The `has_*_privilege` SQL families, `pg_has_role`, and their helpers
//! (`utils/adt/acl.c`).
//!
//! Each object class (table, sequence, column, database, foreign-data-wrapper,
//! function, language, schema, server, tablespace, type, parameter, large
//! object, role) has a fan of `*_name_name` / `*_name` / `*_name_id` / `*_id`
//! / `*_id_name` / `*_id_id` SQL entrypoints plus a `convert_<obj>_name` and a
//! `convert_<obj>_priv_string` helper. The shared internal checks
//! (`column_privilege_check`, `has_param_priv_byname`, `has_lo_priv_byid`,
//! `pg_role_aclcheck`) live here too.
//!
//! Scaffold note: the many `PG_FUNCTION_ARGS` entrypoints are represented by
//! one stub each (named exactly as the C function); when this family lands the
//! shared `*_id`-form check body is factored as in `acl.c`.

use types_acl::{AclMode, AclResult};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;

macro_rules! sql_stub {
    ($($name:ident),+ $(,)?) => {
        $(
            #[doc = concat!("`", stringify!($name), "` (acl.c) — `PG_FUNCTION_ARGS`.")]
            pub fn $name() -> PgResult<()> {
                todo!(concat!(
                    "scaffold: backend-utils-adt-acl::has_privilege::",
                    stringify!($name)
                ))
            }
        )+
    };
}

// --- table ---------------------------------------------------------------
sql_stub!(
    has_table_privilege_name_name,
    has_table_privilege_name,
    has_table_privilege_name_id,
    has_table_privilege_id,
    has_table_privilege_id_name,
    has_table_privilege_id_id,
);

/// `convert_table_name` (acl.c) — resolve a table name text to its OID.
pub fn convert_table_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_table_name")
}
/// `convert_table_priv_string` (acl.c).
pub fn convert_table_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_table_priv_string")
}

// --- sequence ------------------------------------------------------------
sql_stub!(
    has_sequence_privilege_name_name,
    has_sequence_privilege_name,
    has_sequence_privilege_name_id,
    has_sequence_privilege_id,
    has_sequence_privilege_id_name,
    has_sequence_privilege_id_id,
);
/// `convert_sequence_priv_string` (acl.c).
pub fn convert_sequence_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_sequence_priv_string")
}

// --- any column / column -------------------------------------------------
sql_stub!(
    has_any_column_privilege_name_name,
    has_any_column_privilege_name,
    has_any_column_privilege_name_id,
    has_any_column_privilege_id,
    has_any_column_privilege_id_name,
    has_any_column_privilege_id_id,
    has_column_privilege_name_name_name,
    has_column_privilege_name_name_attnum,
    has_column_privilege_name_id_name,
    has_column_privilege_name_id_attnum,
    has_column_privilege_id_name_name,
    has_column_privilege_id_name_attnum,
    has_column_privilege_id_id_name,
    has_column_privilege_id_id_attnum,
    has_column_privilege_name_name,
    has_column_privilege_name_attnum,
    has_column_privilege_id_name,
    has_column_privilege_id_attnum,
);

/// `column_privilege_check` (acl.c) — the shared column-priv check returning
/// the tri-state (`-1` for missing object) wrapped in `AclResult`/optional.
pub fn column_privilege_check(
    _table_oid: Oid,
    _attnum: AttrNumber,
    _roleid: Oid,
    _mask: AclMode,
) -> PgResult<i32> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::column_privilege_check")
}
/// `convert_column_name` (acl.c) — resolve a column name to its `AttrNumber`.
pub fn convert_column_name() -> PgResult<AttrNumber> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_column_name")
}
/// `convert_column_priv_string` (acl.c).
pub fn convert_column_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_column_priv_string")
}

// --- database ------------------------------------------------------------
sql_stub!(
    has_database_privilege_name_name,
    has_database_privilege_name,
    has_database_privilege_name_id,
    has_database_privilege_id,
    has_database_privilege_id_name,
    has_database_privilege_id_id,
);
/// `convert_database_name` (acl.c).
pub fn convert_database_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_database_name")
}
/// `convert_database_priv_string` (acl.c).
pub fn convert_database_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_database_priv_string")
}

// --- foreign data wrapper ------------------------------------------------
sql_stub!(
    has_foreign_data_wrapper_privilege_name_name,
    has_foreign_data_wrapper_privilege_name,
    has_foreign_data_wrapper_privilege_name_id,
    has_foreign_data_wrapper_privilege_id,
    has_foreign_data_wrapper_privilege_id_name,
    has_foreign_data_wrapper_privilege_id_id,
);
/// `convert_foreign_data_wrapper_name` (acl.c).
pub fn convert_foreign_data_wrapper_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_foreign_data_wrapper_name")
}
/// `convert_foreign_data_wrapper_priv_string` (acl.c).
pub fn convert_foreign_data_wrapper_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_foreign_data_wrapper_priv_string")
}

// --- function ------------------------------------------------------------
sql_stub!(
    has_function_privilege_name_name,
    has_function_privilege_name,
    has_function_privilege_name_id,
    has_function_privilege_id,
    has_function_privilege_id_name,
    has_function_privilege_id_id,
);
/// `convert_function_name` (acl.c).
pub fn convert_function_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_function_name")
}
/// `convert_function_priv_string` (acl.c).
pub fn convert_function_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_function_priv_string")
}

// --- language ------------------------------------------------------------
sql_stub!(
    has_language_privilege_name_name,
    has_language_privilege_name,
    has_language_privilege_name_id,
    has_language_privilege_id,
    has_language_privilege_id_name,
    has_language_privilege_id_id,
);
/// `convert_language_name` (acl.c).
pub fn convert_language_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_language_name")
}
/// `convert_language_priv_string` (acl.c).
pub fn convert_language_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_language_priv_string")
}

// --- schema --------------------------------------------------------------
sql_stub!(
    has_schema_privilege_name_name,
    has_schema_privilege_name,
    has_schema_privilege_name_id,
    has_schema_privilege_id,
    has_schema_privilege_id_name,
    has_schema_privilege_id_id,
);
/// `convert_schema_name` (acl.c).
pub fn convert_schema_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_schema_name")
}
/// `convert_schema_priv_string` (acl.c).
pub fn convert_schema_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_schema_priv_string")
}

// --- server --------------------------------------------------------------
sql_stub!(
    has_server_privilege_name_name,
    has_server_privilege_name,
    has_server_privilege_name_id,
    has_server_privilege_id,
    has_server_privilege_id_name,
    has_server_privilege_id_id,
);
/// `convert_server_name` (acl.c).
pub fn convert_server_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_server_name")
}
/// `convert_server_priv_string` (acl.c).
pub fn convert_server_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_server_priv_string")
}

// --- tablespace ----------------------------------------------------------
sql_stub!(
    has_tablespace_privilege_name_name,
    has_tablespace_privilege_name,
    has_tablespace_privilege_name_id,
    has_tablespace_privilege_id,
    has_tablespace_privilege_id_name,
    has_tablespace_privilege_id_id,
);
/// `convert_tablespace_name` (acl.c).
pub fn convert_tablespace_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_tablespace_name")
}
/// `convert_tablespace_priv_string` (acl.c).
pub fn convert_tablespace_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_tablespace_priv_string")
}

// --- type ----------------------------------------------------------------
sql_stub!(
    has_type_privilege_name_name,
    has_type_privilege_name,
    has_type_privilege_name_id,
    has_type_privilege_id,
    has_type_privilege_id_name,
    has_type_privilege_id_id,
);
/// `convert_type_name` (acl.c).
pub fn convert_type_name() -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_type_name")
}
/// `convert_type_priv_string` (acl.c).
pub fn convert_type_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_type_priv_string")
}

// --- parameter -----------------------------------------------------------
/// `has_param_priv_byname` (acl.c) — shared parameter-privilege check by name.
pub fn has_param_priv_byname(_roleid: Oid, _priv: AclMode) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::has_param_priv_byname")
}
sql_stub!(
    has_parameter_privilege_name_name,
    has_parameter_privilege_name,
    has_parameter_privilege_id_name,
);
/// `convert_parameter_priv_string` (acl.c).
pub fn convert_parameter_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_parameter_priv_string")
}

// --- large object --------------------------------------------------------
/// `has_lo_priv_byid` (acl.c) — shared large-object check by OID; `is_missing`
/// reports a vanished object (`-1` tri-state in C).
pub fn has_lo_priv_byid(
    _roleid: Oid,
    _lobj_id: Oid,
    _priv: AclMode,
    _is_missing: &mut bool,
) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::has_lo_priv_byid")
}
sql_stub!(
    has_largeobject_privilege_name_id,
    has_largeobject_privilege_id,
    has_largeobject_privilege_id_id,
);
/// `convert_largeobject_priv_string` (acl.c).
pub fn convert_largeobject_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_largeobject_priv_string")
}

// --- role ----------------------------------------------------------------
sql_stub!(
    pg_has_role_name_name,
    pg_has_role_name,
    pg_has_role_name_id,
    pg_has_role_id,
    pg_has_role_id_name,
    pg_has_role_id_id,
);
/// `convert_role_priv_string` (acl.c).
pub fn convert_role_priv_string() -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::convert_role_priv_string")
}
/// `pg_role_aclcheck` (acl.c) — does `roleid` hold `mode` over role `role_oid`?
pub fn pg_role_aclcheck(_role_oid: Oid, _roleid: Oid, _mode: AclMode) -> PgResult<AclResult> {
    todo!("scaffold: backend-utils-adt-acl::has_privilege::pg_role_aclcheck")
}
