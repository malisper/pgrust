//! ABI vocabulary for `backend/commands/dbcommands.c` (CREATE / ALTER / DROP /
//! RENAME DATABASE).
//!
//! These `#[repr(C)]` structs / enums / constants cross the boundary between the
//! rewritten `backend-commands-dbcommands` crate and the rest of the backend.
//! They mirror the C definitions in:
//!   * `nodes/parsenodes.h`              — `CreatedbStmt`, `AlterDatabaseStmt`,
//!                                          `AlterDatabaseRefreshCollStmt`,
//!                                          `AlterDatabaseSetStmt`, `DropdbStmt`
//!   * `catalog/pg_database_d.h`         — the `pg_database` relation/index OIDs,
//!                                          `Anum_pg_database_*`, `Natts_pg_database`
//!   * `catalog/pg_database.h`           — the `DATCONNLIMIT_*` sentinels
//!   * `commands/dbcommands_xlog.h`      — `XLOG_DBASE_*`, the `xl_dbase_*`
//!                                          WAL records
//!   * `dbcommands.c` (file-local)       — the `CreateDBStrategy` enum
//!
//! This module is referenced as `pgrust_pg_ffi::dbcommands_abi::*` (mirroring the
//! `tcop.rs` convention) so the dbcommands crate names the whole ABI from one
//! place without the ambiguous-glob trap.  It deliberately re-uses the base
//! scalar aliases (`Oid`, `NodeTag`, …) and does not re-define the broadly-shared
//! statement structs that already have a home elsewhere.

use core::ffi::{c_char, c_int};

use crate::{List, Node, NodeTag, Oid};

// ---------------------------------------------------------------------------
// NodeTag discriminants (nodes/nodetags.h, PostgreSQL 18.3 — verified)
// ---------------------------------------------------------------------------

/// `T_CreatedbStmt` = 232.
pub const T_CreatedbStmt: NodeTag = 232;
/// `T_AlterDatabaseStmt` = 233.
pub const T_AlterDatabaseStmt: NodeTag = 233;
/// `T_AlterDatabaseRefreshCollStmt` = 234.
pub const T_AlterDatabaseRefreshCollStmt: NodeTag = 234;
/// `T_AlterDatabaseSetStmt` = 235.
pub const T_AlterDatabaseSetStmt: NodeTag = 235;
/// `T_DropdbStmt` = 236.
pub const T_DropdbStmt: NodeTag = 236;

// ---------------------------------------------------------------------------
// Statement parse nodes (nodes/parsenodes.h)
//
// Each struct mirrors the C layout field-for-field (`#[repr(C)]`, first field
// `type_: NodeTag`).  The `setstmt` of `AlterDatabaseSetStmt` is carried as
// `*mut Node` (opaque): dbcommands.c only forwards it to `AlterSetting`, never
// inspecting its contents, exactly as the C treats a `VariableSetStmt *`.
// ---------------------------------------------------------------------------

/// `typedef struct CreatedbStmt` — `CREATE DATABASE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreatedbStmt {
    pub type_: NodeTag,
    /// name of database to create
    pub dbname: *mut c_char,
    /// List of `DefElem` nodes
    pub options: *mut List,
}

/// `typedef struct AlterDatabaseStmt` — `ALTER DATABASE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDatabaseStmt {
    pub type_: NodeTag,
    /// name of database to alter
    pub dbname: *mut c_char,
    /// List of `DefElem` nodes
    pub options: *mut List,
}

/// `typedef struct AlterDatabaseRefreshCollStmt` —
/// `ALTER DATABASE ... REFRESH COLLATION VERSION`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDatabaseRefreshCollStmt {
    pub type_: NodeTag,
    pub dbname: *mut c_char,
}

/// `typedef struct AlterDatabaseSetStmt` — `ALTER DATABASE ... SET`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDatabaseSetStmt {
    pub type_: NodeTag,
    /// database name
    pub dbname: *mut c_char,
    /// SET or RESET subcommand (`VariableSetStmt *`, opaque here)
    pub setstmt: *mut Node,
}

/// `typedef struct DropdbStmt` — `DROP DATABASE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropdbStmt {
    pub type_: NodeTag,
    /// database to drop
    pub dbname: *mut c_char,
    /// skip error if db is missing?
    pub missing_ok: bool,
    /// currently only FORCE is supported
    pub options: *mut List,
}

// ---------------------------------------------------------------------------
// pg_database relation / index OIDs and column numbers (catalog/pg_database_d.h)
// ---------------------------------------------------------------------------

/// `DatabaseRelationId` — pg_database's relation OID.
pub const DatabaseRelationId: Oid = 1262;
/// `DatabaseNameIndexId` — pg_database_datname_index OID.
pub const DatabaseNameIndexId: Oid = 2671;
/// `DatabaseOidIndexId` — pg_database_oid_index OID.
pub const DatabaseOidIndexId: Oid = 2672;
/// `Template1DbOid`.
pub const Template1DbOid: Oid = 1;
/// `Template0DbOid`.
pub const Template0DbOid: Oid = 4;
/// `PostgresDbOid`.
pub const PostgresDbOid: Oid = 5;

/// `Anum_pg_database_oid` = 1.
pub const Anum_pg_database_oid: c_int = 1;
/// `Anum_pg_database_datname` = 2.
pub const Anum_pg_database_datname: c_int = 2;
/// `Anum_pg_database_datdba` = 3.
pub const Anum_pg_database_datdba: c_int = 3;
/// `Anum_pg_database_encoding` = 4.
pub const Anum_pg_database_encoding: c_int = 4;
/// `Anum_pg_database_datlocprovider` = 5.
pub const Anum_pg_database_datlocprovider: c_int = 5;
/// `Anum_pg_database_datistemplate` = 6.
pub const Anum_pg_database_datistemplate: c_int = 6;
/// `Anum_pg_database_datallowconn` = 7.
pub const Anum_pg_database_datallowconn: c_int = 7;
/// `Anum_pg_database_dathasloginevt` = 8.
pub const Anum_pg_database_dathasloginevt: c_int = 8;
/// `Anum_pg_database_datconnlimit` = 9.
pub const Anum_pg_database_datconnlimit: c_int = 9;
/// `Anum_pg_database_datfrozenxid` = 10.
pub const Anum_pg_database_datfrozenxid: c_int = 10;
/// `Anum_pg_database_datminmxid` = 11.
pub const Anum_pg_database_datminmxid: c_int = 11;
/// `Anum_pg_database_dattablespace` = 12.
pub const Anum_pg_database_dattablespace: c_int = 12;
/// `Anum_pg_database_datcollate` = 13.
pub const Anum_pg_database_datcollate: c_int = 13;
/// `Anum_pg_database_datctype` = 14.
pub const Anum_pg_database_datctype: c_int = 14;
/// `Anum_pg_database_datlocale` = 15.
pub const Anum_pg_database_datlocale: c_int = 15;
/// `Anum_pg_database_daticurules` = 16.
pub const Anum_pg_database_daticurules: c_int = 16;
/// `Anum_pg_database_datcollversion` = 17.
pub const Anum_pg_database_datcollversion: c_int = 17;
/// `Anum_pg_database_datacl` = 18.
pub const Anum_pg_database_datacl: c_int = 18;

/// `Natts_pg_database` = 18.
pub const Natts_pg_database: usize = 18;

// ---------------------------------------------------------------------------
// datconnlimit sentinels (catalog/pg_database.h)
// ---------------------------------------------------------------------------

/// `DATCONNLIMIT_UNLIMITED` (-1) — no limit on connections.
pub const DATCONNLIMIT_UNLIMITED: c_int = -1;
/// `DATCONNLIMIT_INVALID_DB` (-2) — database is being dropped / is invalid.
pub const DATCONNLIMIT_INVALID_DB: c_int = -2;

// ---------------------------------------------------------------------------
// CreateDBStrategy (file-local enum in dbcommands.c)
// ---------------------------------------------------------------------------

/// `typedef enum CreateDBStrategy`.
///
/// `CREATEDB_WAL_LOG` copies the database at the block level, WAL-logging each
/// copied block; `CREATEDB_FILE_COPY` performs a filesystem-level copy and logs
/// a single record per tablespace (bracketed by checkpoints).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum CreateDBStrategy {
    CREATEDB_WAL_LOG,
    CREATEDB_FILE_COPY,
}

// ---------------------------------------------------------------------------
// XLOG record types and structs (commands/dbcommands_xlog.h)
// ---------------------------------------------------------------------------

/// `XLOG_DBASE_CREATE_FILE_COPY` = 0x00.
pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
/// `XLOG_DBASE_CREATE_WAL_LOG` = 0x10.
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
/// `XLOG_DBASE_DROP` = 0x20.
pub const XLOG_DBASE_DROP: u8 = 0x20;

/// Single WAL record for an entire CREATE DATABASE operation (FILE_COPY).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_dbase_create_file_copy_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

/// WAL record for the start of a CREATE DATABASE operation (WAL_LOG).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_dbase_create_wal_log_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
}

/// WAL record for a DROP DATABASE operation (or tablespace move-out).
///
/// In C this carries a `tablespace_ids[FLEXIBLE_ARRAY_MEMBER]` tail; the fixed
/// head modeled here matches `MinSizeOfDbaseDropRec` (the flexible array is
/// appended separately when the record is assembled).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_dbase_drop_rec {
    pub db_id: Oid,
    /// number of tablespace IDs
    pub ntablespaces: c_int,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn createdbstmt_layout_matches_postgres() {
        assert_eq!(size_of::<CreatedbStmt>(), 24);
        assert_eq!(offset_of!(CreatedbStmt, type_), 0);
        assert_eq!(offset_of!(CreatedbStmt, dbname), 8);
        assert_eq!(offset_of!(CreatedbStmt, options), 16);
    }

    #[test]
    fn dropdbstmt_layout_matches_postgres() {
        assert_eq!(offset_of!(DropdbStmt, type_), 0);
        assert_eq!(offset_of!(DropdbStmt, dbname), 8);
        assert_eq!(offset_of!(DropdbStmt, missing_ok), 16);
        assert_eq!(offset_of!(DropdbStmt, options), 24);
    }

    #[test]
    fn xl_dbase_create_file_copy_layout() {
        assert_eq!(size_of::<xl_dbase_create_file_copy_rec>(), 16);
        assert_eq!(align_of::<xl_dbase_create_file_copy_rec>(), 4);
    }

    #[test]
    fn xl_dbase_create_wal_log_layout() {
        assert_eq!(size_of::<xl_dbase_create_wal_log_rec>(), 8);
    }

    #[test]
    fn natts_and_sentinels() {
        assert_eq!(Natts_pg_database, 18);
        assert_eq!(DATCONNLIMIT_UNLIMITED, -1);
        assert_eq!(DATCONNLIMIT_INVALID_DB, -2);
    }
}
