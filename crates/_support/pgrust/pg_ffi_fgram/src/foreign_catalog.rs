//! ABI types for the foreign-data catalogs (`pg_foreign_data_wrapper`,
//! `pg_foreign_server`, `pg_user_mapping`, `pg_foreign_table`).
//!
//! Mirrors, with identical `repr(C)` layout and field order, the C definitions
//! in `catalog/pg_foreign_data_wrapper.h` / `catalog/pg_foreign_server.h` /
//! `catalog/pg_user_mapping.h` / `catalog/pg_foreign_table.h`.  Relation/index
//! OIDs and `Anum_*` / `Natts_*` column numbers come from the generated
//! `pg_foreign_*_d.h` / `pg_user_mapping_d.h` headers (PostgreSQL 18.3).
//!
//! Referenced by path (`pg_ffi_fgram::foreign_catalog::*`); deliberately NOT in
//! the crate-root glob (it carries `NameData`-bearing `FormData_*` structs and
//! catalog OIDs that overlap other modules).

use crate::types::Oid;
use crate::NameData;

/* ---------------------------------------------------------------------------
 * Relation OIDs.
 * ------------------------------------------------------------------------- */

/// `ForeignDataWrapperRelationId` — `pg_foreign_data_wrapper`.
pub const ForeignDataWrapperRelationId: Oid = 2328;
/// `ForeignServerRelationId` — `pg_foreign_server`.
pub const ForeignServerRelationId: Oid = 1417;
/// `UserMappingRelationId` — `pg_user_mapping`.
pub const UserMappingRelationId: Oid = 1418;
/// `ForeignTableRelationId` — `pg_foreign_table`.
pub const ForeignTableRelationId: Oid = 3118;

/* ---------------------------------------------------------------------------
 * Index OIDs.
 * ------------------------------------------------------------------------- */

/// `ForeignDataWrapperOidIndexId` — `pg_foreign_data_wrapper_oid_index`.
pub const ForeignDataWrapperOidIndexId: Oid = 112;
/// `ForeignDataWrapperNameIndexId` — `pg_foreign_data_wrapper_name_index`.
pub const ForeignDataWrapperNameIndexId: Oid = 548;
/// `ForeignServerOidIndexId` — `pg_foreign_server_oid_index`.
pub const ForeignServerOidIndexId: Oid = 113;
/// `ForeignServerNameIndexId` — `pg_foreign_server_name_index`.
pub const ForeignServerNameIndexId: Oid = 549;
/// `UserMappingOidIndexId` — `pg_user_mapping_oid_index`.
pub const UserMappingOidIndexId: Oid = 174;
/// `UserMappingUserServerIndexId` — `pg_user_mapping_user_server_index`.
pub const UserMappingUserServerIndexId: Oid = 175;

/* ---------------------------------------------------------------------------
 * pg_foreign_data_wrapper column numbers.
 * ------------------------------------------------------------------------- */

pub const Anum_pg_foreign_data_wrapper_oid: i16 = 1;
pub const Anum_pg_foreign_data_wrapper_fdwname: i16 = 2;
pub const Anum_pg_foreign_data_wrapper_fdwowner: i16 = 3;
pub const Anum_pg_foreign_data_wrapper_fdwhandler: i16 = 4;
pub const Anum_pg_foreign_data_wrapper_fdwvalidator: i16 = 5;
pub const Anum_pg_foreign_data_wrapper_fdwacl: i16 = 6;
pub const Anum_pg_foreign_data_wrapper_fdwoptions: i16 = 7;
pub const Natts_pg_foreign_data_wrapper: usize = 7;

/* pg_foreign_server column numbers. */
pub const Anum_pg_foreign_server_oid: i16 = 1;
pub const Anum_pg_foreign_server_srvname: i16 = 2;
pub const Anum_pg_foreign_server_srvowner: i16 = 3;
pub const Anum_pg_foreign_server_srvfdw: i16 = 4;
pub const Anum_pg_foreign_server_srvtype: i16 = 5;
pub const Anum_pg_foreign_server_srvversion: i16 = 6;
pub const Anum_pg_foreign_server_srvacl: i16 = 7;
pub const Anum_pg_foreign_server_srvoptions: i16 = 8;
pub const Natts_pg_foreign_server: usize = 8;

/* pg_user_mapping column numbers. */
pub const Anum_pg_user_mapping_oid: i16 = 1;
pub const Anum_pg_user_mapping_umuser: i16 = 2;
pub const Anum_pg_user_mapping_umserver: i16 = 3;
pub const Anum_pg_user_mapping_umoptions: i16 = 4;
pub const Natts_pg_user_mapping: usize = 4;

/* pg_foreign_table column numbers. */
pub const Anum_pg_foreign_table_ftrelid: i16 = 1;
pub const Anum_pg_foreign_table_ftserver: i16 = 2;
pub const Anum_pg_foreign_table_ftoptions: i16 = 3;
pub const Natts_pg_foreign_table: usize = 3;

/* ---------------------------------------------------------------------------
 * FormData structs — fixed-length prefix only (CATALOG_VARLEN columns
 * fdwacl/fdwoptions/srvacl/srvoptions/umoptions/ftoptions live past the struct
 * and are read via heap_getattr / SysCacheGetAttr, never as struct fields).
 * ------------------------------------------------------------------------- */

/// `FormData_pg_foreign_data_wrapper` — fixed-length prefix of a
/// `pg_foreign_data_wrapper` row.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_foreign_data_wrapper {
    pub oid: Oid,
    pub fdwname: NameData,
    pub fdwowner: Oid,
    pub fdwhandler: Oid,
    pub fdwvalidator: Oid,
}

pub type Form_pg_foreign_data_wrapper = *mut FormData_pg_foreign_data_wrapper;

/// `FormData_pg_foreign_server` — fixed-length prefix of a `pg_foreign_server`
/// row.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_foreign_server {
    pub oid: Oid,
    pub srvname: NameData,
    pub srvowner: Oid,
    pub srvfdw: Oid,
}

pub type Form_pg_foreign_server = *mut FormData_pg_foreign_server;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oids() {
        assert_eq!(ForeignDataWrapperRelationId, 2328);
        assert_eq!(ForeignServerRelationId, 1417);
        assert_eq!(UserMappingRelationId, 1418);
        assert_eq!(ForeignTableRelationId, 3118);
        assert_eq!(ForeignDataWrapperOidIndexId, 112);
        assert_eq!(ForeignServerOidIndexId, 113);
        assert_eq!(UserMappingOidIndexId, 174);
        assert_eq!(UserMappingUserServerIndexId, 175);
    }

    #[test]
    fn natts() {
        assert_eq!(Natts_pg_foreign_data_wrapper, 7);
        assert_eq!(Natts_pg_foreign_server, 8);
        assert_eq!(Natts_pg_user_mapping, 4);
        assert_eq!(Natts_pg_foreign_table, 3);
    }
}
