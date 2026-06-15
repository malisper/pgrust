//! Vocabulary for the `commands/foreigncmds.c` port: the FOREIGN DATA
//! WRAPPER / SERVER / USER MAPPING / FOREIGN TABLE parse-tree statement nodes
//! (`nodes/parsenodes.h`), the `DefElem` / `RoleSpec` option/role nodes, the
//! foreign-object descriptor carriers (`foreign/foreign.h`), and the syscache
//! row carriers the command drivers read.
//!
//! These are the parser-produced nodes this command unit consumes; they are
//! allocated in the parse context (`Mcx<'mcx>`). The descriptor and row
//! carriers are returned by the catalog/accessor seams allocated in the
//! caller's context.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_plancache::UtilityStmtHandle;

/* ---------------------------------------------------------------------------
 * Catalog relation OIDs (`catalog/pg_*_d.h`).
 * ------------------------------------------------------------------------- */

/// `ForeignDataWrapperRelationId` — `pg_foreign_data_wrapper`
/// (`CATALOG(pg_foreign_data_wrapper,2328,...)`).
pub const ForeignDataWrapperRelationId: Oid = 2328;
/// `ForeignServerRelationId` — `pg_foreign_server`
/// (`CATALOG(pg_foreign_server,1417,...)`).
pub const ForeignServerRelationId: Oid = 1417;
/// `ForeignTableRelationId` — `pg_foreign_table`
/// (`CATALOG(pg_foreign_table,3118,...)`).
pub const ForeignTableRelationId: Oid = 3118;
/// `UserMappingRelationId` — `pg_user_mapping`
/// (`CATALOG(pg_user_mapping,1418,...)`).
pub const UserMappingRelationId: Oid = 1418;
/// `ProcedureRelationId` — `pg_proc` (`CATALOG(pg_proc,1255,...)`).
pub const ProcedureRelationId: Oid = 1255;
/// `RelationRelationId` — `pg_class` (`CATALOG(pg_class,1259,...)`).
pub const RelationRelationId: Oid = 1259;

/* ---------------------------------------------------------------------------
 * Built-in type OIDs (`catalog/pg_type.dat`) read by the func-option lookups.
 * ------------------------------------------------------------------------- */

/// `FDW_HANDLEROID` — `fdw_handler` pseudo-type (pg_type.dat oid 3115).
pub const FDW_HANDLEROID: Oid = 3115;
/// `TEXTARRAYOID` — `text[]` (pg_type.dat oid 1009).
pub const TEXTARRAYOID: Oid = 1009;
/// `OIDOID` — `oid` (pg_type.dat oid 26).
pub const OIDOID: Oid = 26;

/// `ACL_ID_PUBLIC` — placeholder role OID for a PUBLIC acl item (`utils/acl.h`).
pub const ACL_ID_PUBLIC: Oid = 0;

/* ---------------------------------------------------------------------------
 * DefElem (`nodes/parsenodes.h`).
 * ------------------------------------------------------------------------- */

/// `DefElemAction` (`nodes/parsenodes.h`) — the SET/ADD/DROP action of a
/// `DefElem` in an option list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum DefElemAction {
    /// `DEFELEM_UNSPEC` — no action given (treated as ADD).
    Unspec = 0,
    /// `DEFELEM_SET`.
    Set = 1,
    /// `DEFELEM_ADD`.
    Add = 2,
    /// `DEFELEM_DROP`.
    Drop = 3,
}

pub use DefElemAction::{
    Add as DEFELEM_ADD, Drop as DEFELEM_DROP, Set as DEFELEM_SET, Unspec as DEFELEM_UNSPEC,
};

/// Projection of a `DefElem`'s `arg` value node (`nodes/value.h` /
/// `nodes/parsenodes.h`) — the variants foreigncmds consumes. The generic
/// option values are validated by the FDW validator (a catalog/fmgr seam) and
/// re-encoded by the catalog store seam, so foreigncmds itself reads only the
/// `NameList` form (the HANDLER/VALIDATOR function name) directly. The scalar
/// value variants are carried so the store/validator seams can render them.
#[derive(Debug)]
pub enum DefElemArg<'mcx> {
    /// `T_Integer` (`intVal`).
    Integer(i64),
    /// `T_Float` (`Float->fval`, kept as its source text).
    Float(PgString<'mcx>),
    /// `T_Boolean` (`boolVal`).
    Boolean(bool),
    /// `T_String` (`strVal`).
    String(PgString<'mcx>),
    /// `T_List` — a qualified name (`List *` of `String` nodes), as carried by
    /// HANDLER/VALIDATOR options and passed to `LookupFuncName`.
    NameList(PgVec<'mcx, PgString<'mcx>>),
}

/// `DefElem` (`nodes/parsenodes.h`) — one `name [= value]` option, with an
/// optional SET/ADD/DROP action. `defnamespace` is omitted: foreigncmds never
/// reads it.
#[derive(Debug)]
pub struct DefElem<'mcx> {
    /// `defname` — the option name.
    pub defname: PgString<'mcx>,
    /// `arg` — the value node, or `None`.
    pub arg: Option<PgBox<'mcx, DefElemArg<'mcx>>>,
    /// `defaction` — SET/ADD/DROP or unspecified.
    pub defaction: DefElemAction,
}

impl<'mcx> DefElem<'mcx> {
    /// Deep-copy this `DefElem` into `mcx` (the C `copyObject(DefElem)` shape;
    /// `transformGenericOptions`'s `lappend(resultOptions, od)` /
    /// `lfirst(cell) = od` retain the node, which the owned tree models by
    /// cloning into the result context). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DefElem<'b>> {
        let defname = self.defname.clone_in(mcx)?;
        let arg = match &self.arg {
            None => None,
            Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        };
        Ok(DefElem {
            defname,
            arg,
            defaction: self.defaction,
        })
    }
}

impl<'mcx> DefElemArg<'mcx> {
    fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DefElemArg<'b>> {
        Ok(match self {
            DefElemArg::Integer(v) => DefElemArg::Integer(*v),
            DefElemArg::Float(s) => DefElemArg::Float(s.clone_in(mcx)?),
            DefElemArg::Boolean(b) => DefElemArg::Boolean(*b),
            DefElemArg::String(s) => DefElemArg::String(s.clone_in(mcx)?),
            DefElemArg::NameList(names) => {
                let mut out = mcx::vec_with_capacity_in(mcx, names.len())?;
                for n in names.iter() {
                    out.push(n.clone_in(mcx)?);
                }
                DefElemArg::NameList(out)
            }
        })
    }
}

// `RoleSpec` / `RoleSpecType` (`nodes/parsenodes.h`) are general parse-node
// vocabulary and live in `types_nodes::parsenodes`; re-exported here for the
// USER MAPPING statements that carry one.
pub use types_nodes::parsenodes::{RoleSpec, RoleSpecType, ROLESPEC_PUBLIC};

/* ---------------------------------------------------------------------------
 * Statement nodes (`nodes/parsenodes.h`), trimmed to the fields foreigncmds
 * reads.
 * ------------------------------------------------------------------------- */

/// `CreateFdwStmt` — `CREATE FOREIGN DATA WRAPPER`.
#[derive(Debug)]
pub struct CreateFdwStmt<'mcx> {
    /// `fdwname`.
    pub fdwname: PgString<'mcx>,
    /// `func_options` — HANDLER/VALIDATOR options.
    pub func_options: PgVec<'mcx, DefElem<'mcx>>,
    /// `options` — generic options to the FDW.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/// `AlterFdwStmt` — `ALTER FOREIGN DATA WRAPPER`.
#[derive(Debug)]
pub struct AlterFdwStmt<'mcx> {
    /// `fdwname`.
    pub fdwname: PgString<'mcx>,
    /// `func_options` — HANDLER/VALIDATOR options.
    pub func_options: PgVec<'mcx, DefElem<'mcx>>,
    /// `options` — generic options to the FDW.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/// `CreateForeignServerStmt` — `CREATE SERVER`.
#[derive(Debug)]
pub struct CreateForeignServerStmt<'mcx> {
    /// `servername`.
    pub servername: PgString<'mcx>,
    /// `servertype` — optional server type.
    pub servertype: Option<PgString<'mcx>>,
    /// `version` — optional server version.
    pub version: Option<PgString<'mcx>>,
    /// `fdwname` — FDW name.
    pub fdwname: PgString<'mcx>,
    /// `if_not_exists`.
    pub if_not_exists: bool,
    /// `options` — generic options to the server.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/// `AlterForeignServerStmt` — `ALTER SERVER`.
#[derive(Debug)]
pub struct AlterForeignServerStmt<'mcx> {
    /// `servername`.
    pub servername: PgString<'mcx>,
    /// `version` — optional server version.
    pub version: Option<PgString<'mcx>>,
    /// `options` — generic options to the server.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
    /// `has_version` — version was specified.
    pub has_version: bool,
}

/// `CreateForeignTableStmt` — `CREATE FOREIGN TABLE`. The `base` `CreateStmt`
/// is created by `DefineRelation` before this runs; foreigncmds reads only the
/// server name and options here.
#[derive(Debug)]
pub struct CreateForeignTableStmt<'mcx> {
    /// `servername`.
    pub servername: PgString<'mcx>,
    /// `options` — generic options to the FDW.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/// `CreateUserMappingStmt` — `CREATE USER MAPPING`.
#[derive(Debug)]
pub struct CreateUserMappingStmt<'mcx> {
    /// `user` — the user role.
    pub user: PgBox<'mcx, RoleSpec<'mcx>>,
    /// `servername`.
    pub servername: PgString<'mcx>,
    /// `if_not_exists`.
    pub if_not_exists: bool,
    /// `options` — generic options to the server.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/// `AlterUserMappingStmt` — `ALTER USER MAPPING`.
#[derive(Debug)]
pub struct AlterUserMappingStmt<'mcx> {
    /// `user` — the user role.
    pub user: PgBox<'mcx, RoleSpec<'mcx>>,
    /// `servername`.
    pub servername: PgString<'mcx>,
    /// `options` — generic options to the server.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/// `DropUserMappingStmt` — `DROP USER MAPPING`.
#[derive(Debug)]
pub struct DropUserMappingStmt<'mcx> {
    /// `user` — the user role.
    pub user: PgBox<'mcx, RoleSpec<'mcx>>,
    /// `servername`.
    pub servername: PgString<'mcx>,
    /// `missing_ok` — ignore missing mappings.
    pub missing_ok: bool,
}

/// `ImportForeignSchemaType` (`nodes/parsenodes.h`) — which tables an IMPORT
/// FOREIGN SCHEMA wants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ImportForeignSchemaType {
    /// `FDW_IMPORT_SCHEMA_ALL` — all relations wanted.
    All = 0,
    /// `FDW_IMPORT_SCHEMA_LIMIT_TO` — include only listed tables.
    LimitTo = 1,
    /// `FDW_IMPORT_SCHEMA_EXCEPT` — exclude listed tables.
    Except = 2,
}

pub use ImportForeignSchemaType::{
    All as FDW_IMPORT_SCHEMA_ALL, Except as FDW_IMPORT_SCHEMA_EXCEPT,
    LimitTo as FDW_IMPORT_SCHEMA_LIMIT_TO,
};

/// `ImportForeignSchemaStmt` — `IMPORT FOREIGN SCHEMA`. `table_list` is the C
/// `List *` of `RangeVar`; only the table names (the field
/// `IsImportableForeignTable` compares) are retained.
#[derive(Debug)]
pub struct ImportForeignSchemaStmt<'mcx> {
    /// `server_name` — FDW server name.
    pub server_name: PgString<'mcx>,
    /// `remote_schema` — remote schema to query.
    pub remote_schema: PgString<'mcx>,
    /// `local_schema` — local schema to create objects in.
    pub local_schema: PgString<'mcx>,
    /// `list_type` — type of the table list.
    pub list_type: ImportForeignSchemaType,
    /// `table_list` — the relation names of the `List *` of `RangeVar`.
    pub table_list: PgVec<'mcx, PgString<'mcx>>,
    /// `options` — options to pass to the FDW.
    pub options: PgVec<'mcx, DefElem<'mcx>>,
}

/* ---------------------------------------------------------------------------
 * Foreign-object descriptor carriers (`foreign/foreign.h`), trimmed to the
 * fields foreigncmds reads.
 * ------------------------------------------------------------------------- */

/// `ForeignDataWrapper` (foreign/foreign.h) — the subset of fields
/// foreigncmds.c reads.
#[derive(Debug)]
pub struct ForeignDataWrapper<'mcx> {
    /// `fdwid` — FDW OID.
    pub fdwid: Oid,
    /// `fdwname` — name of the FDW.
    pub fdwname: PgString<'mcx>,
    /// `fdwhandler` — OID of the handler function, or `InvalidOid`.
    pub fdwhandler: Oid,
    /// `fdwvalidator` — OID of the validator function, or `InvalidOid`.
    pub fdwvalidator: Oid,
}

/// `ForeignServer` (foreign/foreign.h) — the subset of fields foreigncmds.c
/// reads.
#[derive(Debug)]
pub struct ForeignServer<'mcx> {
    /// `serverid` — server OID.
    pub serverid: Oid,
    /// `fdwid` — the server's foreign-data wrapper OID.
    pub fdwid: Oid,
    /// `servername` — name of the server.
    pub servername: PgString<'mcx>,
}

/// `ForeignTable` (foreign/foreign.h) — the per-relation foreign-table
/// descriptor `GetForeignTable` returns. `options` is the `ftoptions` column
/// decoded by `untransformRelOptions` into `(name, value)` pairs (the C
/// `List *` of `DefElem`).
#[derive(Debug)]
pub struct ForeignTable {
    /// `relid` — relation OID.
    pub relid: Oid,
    /// `serverid` — server OID.
    pub serverid: Oid,
    /// `options` — `ftoptions` as `(name, value)` pairs.
    pub options: Vec<(String, Option<String>)>,
}

/// `UserMapping` (foreign/foreign.h) — the descriptor `GetUserMapping` returns.
/// `options` is the `umoptions` column decoded by `untransformRelOptions`.
#[derive(Debug)]
pub struct UserMapping {
    /// `umid` — OID of the user mapping.
    pub umid: Oid,
    /// `userid` — local user OID (the requested user, not the matched row's).
    pub userid: Oid,
    /// `serverid` — server OID.
    pub serverid: Oid,
    /// `options` — `umoptions` as `(name, value)` pairs.
    pub options: Vec<(String, Option<String>)>,
}

/* ---------------------------------------------------------------------------
 * Syscache row carriers — the `(...)` columns read out of the catalog tuple
 * the owner-change / alter paths fetch via `SearchSysCacheCopy1`.
 * ------------------------------------------------------------------------- */

/// `(fdwid, fdwname, fdwowner)` — read by the FDW owner-change path.
#[derive(Debug)]
pub struct FdwOwnerRow<'mcx> {
    pub fdwid: Oid,
    pub fdwname: PgString<'mcx>,
    pub fdwowner: Oid,
}

/// `(fdwid, fdwvalidator)` — read by `AlterForeignDataWrapper`.
#[derive(Clone, Copy, Debug)]
pub struct FdwUpdateRow {
    pub fdwid: Oid,
    pub fdwvalidator: Oid,
}

/// `(serverid, srvname, srvowner, srvfdw)` — read by the server owner-change
/// path.
#[derive(Debug)]
pub struct ServerOwnerRow<'mcx> {
    pub serverid: Oid,
    pub srvname: PgString<'mcx>,
    pub srvowner: Oid,
    pub srvfdw: Oid,
}

/// `(serverid, srvfdw)` — read by `AlterForeignServer`.
#[derive(Clone, Copy, Debug)]
pub struct ServerUpdateRow {
    pub serverid: Oid,
    pub srvfdw: Oid,
}

/* ---------------------------------------------------------------------------
 * pg_foreign_* catalog-write row carriers + catalog Anum/Natts/index-OID
 * constants (`catalog/pg_foreign_*.h`, `catalog/pg_*_d.h`).
 *
 * These are the deformed-row carriers the catalog-DML seams build and hand to
 * the `catalog/indexing.c`-owned `catalog_tuple_insert_pg_*` /
 * `catalog_tuple_update_pg_*` seams (which `heap_form_tuple` +
 * `CatalogTupleInsert`/`Update` against the catalog descriptor). The option
 * list is rendered to `(name, value)` pairs (the C `optionListToArray` packs
 * `"name=value"` text varlenas); both halves are non-null because the C
 * `defGetString` always yields a string. An empty `options` carrier matches
 * the C `PointerGetDatum(NULL)` "store SQL NULL" case (the seam writes a NULL
 * `*options` column).
 * ------------------------------------------------------------------------- */

/// `Natts_pg_foreign_data_wrapper` — `pg_foreign_data_wrapper` has 7 columns
/// (`oid, fdwname, fdwowner, fdwhandler, fdwvalidator, fdwacl, fdwoptions`).
pub const Natts_pg_foreign_data_wrapper: usize = 7;
/// `Anum_pg_foreign_data_wrapper_oid` (column 1).
pub const Anum_pg_foreign_data_wrapper_oid: i16 = 1;
/// `Anum_pg_foreign_data_wrapper_fdwname` (column 2).
pub const Anum_pg_foreign_data_wrapper_fdwname: i16 = 2;
/// `Anum_pg_foreign_data_wrapper_fdwowner` (column 3).
pub const Anum_pg_foreign_data_wrapper_fdwowner: i16 = 3;
/// `Anum_pg_foreign_data_wrapper_fdwhandler` (column 4).
pub const Anum_pg_foreign_data_wrapper_fdwhandler: i16 = 4;
/// `Anum_pg_foreign_data_wrapper_fdwvalidator` (column 5).
pub const Anum_pg_foreign_data_wrapper_fdwvalidator: i16 = 5;
/// `Anum_pg_foreign_data_wrapper_fdwacl` (column 6).
pub const Anum_pg_foreign_data_wrapper_fdwacl: i16 = 6;
/// `Anum_pg_foreign_data_wrapper_fdwoptions` (column 7).
pub const Anum_pg_foreign_data_wrapper_fdwoptions: i16 = 7;
/// `ForeignDataWrapperOidIndexId` — `pg_foreign_data_wrapper_oid_index`
/// (`catalog/pg_foreign_data_wrapper.h`, OID 112).
pub const ForeignDataWrapperOidIndexId: Oid = 112;

/// `Natts_pg_foreign_server` — `pg_foreign_server` has 8 columns
/// (`oid, srvname, srvowner, srvfdw, srvtype, srvversion, srvacl, srvoptions`).
pub const Natts_pg_foreign_server: usize = 8;
/// `Anum_pg_foreign_server_oid` (column 1).
pub const Anum_pg_foreign_server_oid: i16 = 1;
/// `Anum_pg_foreign_server_srvname` (column 2).
pub const Anum_pg_foreign_server_srvname: i16 = 2;
/// `Anum_pg_foreign_server_srvowner` (column 3).
pub const Anum_pg_foreign_server_srvowner: i16 = 3;
/// `Anum_pg_foreign_server_srvfdw` (column 4).
pub const Anum_pg_foreign_server_srvfdw: i16 = 4;
/// `Anum_pg_foreign_server_srvtype` (column 5).
pub const Anum_pg_foreign_server_srvtype: i16 = 5;
/// `Anum_pg_foreign_server_srvversion` (column 6).
pub const Anum_pg_foreign_server_srvversion: i16 = 6;
/// `Anum_pg_foreign_server_srvacl` (column 7).
pub const Anum_pg_foreign_server_srvacl: i16 = 7;
/// `Anum_pg_foreign_server_srvoptions` (column 8).
pub const Anum_pg_foreign_server_srvoptions: i16 = 8;
/// `ForeignServerOidIndexId` — `pg_foreign_server_oid_index`
/// (`catalog/pg_foreign_server.h`, OID 113).
pub const ForeignServerOidIndexId: Oid = 113;

/// `Natts_pg_user_mapping` — `pg_user_mapping` has 4 columns
/// (`oid, umuser, umserver, umoptions`).
pub const Natts_pg_user_mapping: usize = 4;
/// `Anum_pg_user_mapping_oid` (column 1).
pub const Anum_pg_user_mapping_oid: i16 = 1;
/// `Anum_pg_user_mapping_umuser` (column 2).
pub const Anum_pg_user_mapping_umuser: i16 = 2;
/// `Anum_pg_user_mapping_umserver` (column 3).
pub const Anum_pg_user_mapping_umserver: i16 = 3;
/// `Anum_pg_user_mapping_umoptions` (column 4).
pub const Anum_pg_user_mapping_umoptions: i16 = 4;
/// `UserMappingOidIndexId` — `pg_user_mapping_oid_index`
/// (`catalog/pg_user_mapping.h`, OID 174).
pub const UserMappingOidIndexId: Oid = 174;

/// `Natts_pg_foreign_table` — `pg_foreign_table` has 3 columns
/// (`ftrelid, ftserver, ftoptions`); it has no OID column.
pub const Natts_pg_foreign_table: usize = 3;
/// `Anum_pg_foreign_table_ftrelid` (column 1).
pub const Anum_pg_foreign_table_ftrelid: i16 = 1;
/// `Anum_pg_foreign_table_ftserver` (column 2).
pub const Anum_pg_foreign_table_ftserver: i16 = 2;
/// `Anum_pg_foreign_table_ftoptions` (column 3).
pub const Anum_pg_foreign_table_ftoptions: i16 = 3;

/// Insert carrier for one `pg_foreign_data_wrapper` row
/// (`CreateForeignDataWrapper`). `oid` is the caller-assigned
/// `GetNewOidWithIndex` value; `fdwacl` is always SQL NULL on create.
#[derive(Clone, Debug)]
pub struct PgForeignDataWrapperInsertRow {
    /// `oid`.
    pub oid: Oid,
    /// `fdwname` (`namein(stmt->fdwname)`).
    pub fdwname: String,
    /// `fdwowner`.
    pub fdwowner: Oid,
    /// `fdwhandler` (`InvalidOid` if none).
    pub fdwhandler: Oid,
    /// `fdwvalidator` (`InvalidOid` if none).
    pub fdwvalidator: Oid,
    /// `fdwoptions` as `(name, value)` pairs; `None` ⇒ store SQL NULL.
    pub options: Option<Vec<(String, String)>>,
}

/// Update carrier for `AlterForeignDataWrapper`'s tuple update: only the
/// `Some` columns are replaced (the C `repl_repl[..] = true` columns).
/// `options = Some(None)` stores SQL NULL.
#[derive(Clone, Debug)]
pub struct PgForeignDataWrapperUpdateRow {
    /// `fdwhandler`.
    pub handler: Option<Oid>,
    /// `fdwvalidator`.
    pub validator: Option<Oid>,
    /// `fdwoptions`.
    pub options: Option<Option<Vec<(String, String)>>>,
}

/// Insert carrier for one `pg_foreign_server` row (`CreateForeignServer`).
#[derive(Clone, Debug)]
pub struct PgForeignServerInsertRow {
    /// `oid`.
    pub oid: Oid,
    /// `srvname` (`namein(stmt->servername)`).
    pub srvname: String,
    /// `srvowner`.
    pub srvowner: Oid,
    /// `srvfdw`.
    pub srvfdw: Oid,
    /// `srvtype` (`CStringGetTextDatum`); `None` ⇒ SQL NULL.
    pub srvtype: Option<String>,
    /// `srvversion` (`CStringGetTextDatum`); `None` ⇒ SQL NULL.
    pub srvversion: Option<String>,
    /// `srvoptions` as `(name, value)` pairs; `None` ⇒ store SQL NULL.
    /// `srvacl` is always SQL NULL on create.
    pub options: Option<Vec<(String, String)>>,
}

/// Update carrier for `AlterForeignServer`'s tuple update.
#[derive(Clone, Debug)]
pub struct PgForeignServerUpdateRow {
    /// `srvversion`; `Some(None)` stores SQL NULL.
    pub version: Option<Option<String>>,
    /// `srvoptions`.
    pub options: Option<Option<Vec<(String, String)>>>,
}

/// Insert carrier for one `pg_user_mapping` row (`CreateUserMapping`).
#[derive(Clone, Debug)]
pub struct PgUserMappingInsertRow {
    /// `oid`.
    pub oid: Oid,
    /// `umuser` (`InvalidOid` for PUBLIC).
    pub umuser: Oid,
    /// `umserver`.
    pub umserver: Oid,
    /// `umoptions` as `(name, value)` pairs; `None` ⇒ store SQL NULL.
    pub options: Option<Vec<(String, String)>>,
}

/// Update carrier for `AlterUserMapping`'s tuple update.
#[derive(Clone, Debug)]
pub struct PgUserMappingUpdateRow {
    /// `umoptions`.
    pub options: Option<Option<Vec<(String, String)>>>,
}

/// Insert carrier for one `pg_foreign_table` row (`CreateForeignTable`).
/// `pg_foreign_table` has no OID column; `ftrelid` is the row key.
#[derive(Clone, Debug)]
pub struct PgForeignTableInsertRow {
    /// `ftrelid`.
    pub ftrelid: Oid,
    /// `ftserver`.
    pub ftserver: Oid,
    /// `ftoptions` as `(name, value)` pairs; `None` ⇒ store SQL NULL.
    pub options: Option<Vec<(String, String)>>,
}

/* ---------------------------------------------------------------------------
 * IMPORT FOREIGN SCHEMA loop carriers.
 *
 * `ImportForeignSchema` parses each FDW-returned command into a list of
 * `RawStmt *` (`pg_parse_query`, tcop-owned) and processes each. The raw parse
 * trees and the embedded `CreateForeignTableStmt`/`CreateStmt`/`RangeVar` are
 * unported parser nodes, so they ride as the established opaque handles; the
 * fields the loop branches on (the node tag, the table name, the
 * `stmt_location`/`stmt_len`, the embedded statement node) are projected by the
 * parse-node seam, and the in-crate loop owns the type-check `elog`, the
 * `IsImportableForeignTable` filter, the schema-name rewrite, the `PlannedStmt`
 * construction, and the inter-subcommand command-counter advance.
 * ------------------------------------------------------------------------- */

/// `nodeTag(rs->stmt)` classification of one raw parse tree produced by an
/// IMPORT FOREIGN SCHEMA command: either the expected `CreateForeignTableStmt`
/// (with the fields the loop reads/writes), or some other node (carrying its
/// `NodeTag` value for the C `elog` message). The wrapped statement node rides
/// as a [`UtilityStmtHandle`] (the C `rs->stmt`, set as `pstmt->utilityStmt`).
#[derive(Clone, Debug)]
pub enum ImportRawStmt {
    /// `IsA(cstmt, CreateForeignTableStmt)` — the importable case.
    CreateForeignTable {
        /// `cstmt->base.relation->relname` — the IMPORT-filter / error-context
        /// table name.
        relname: String,
        /// `rs->stmt_location`.
        stmt_location: i32,
        /// `rs->stmt_len`.
        stmt_len: i32,
        /// `rs->stmt` (`= (Node *) cstmt`) — the node `pstmt->utilityStmt`
        /// points at and `ProcessUtility` executes.
        utility_stmt: UtilityStmtHandle,
    },
    /// Any other node type the FDW (incorrectly) returned; carries
    /// `(int) nodeTag(cstmt)` for the `elog(ERROR, "...incorrect statement
    /// type %d")` message.
    Other {
        /// `(int) nodeTag(cstmt)`.
        node_tag: i32,
    },
}

/// The wrapper `PlannedStmt` `ImportForeignSchema` builds for each importable
/// command (`makeNode(PlannedStmt)` with `commandType = CMD_UTILITY`,
/// `canSetTag = false`, `utilityStmt = (Node *) cstmt`, and the raw stmt's
/// location/length). Constructed in-crate and handed to `ProcessUtility`; the
/// remaining `PlannedStmt` fields are zero/NULL as `makeNode` leaves them.
#[derive(Clone, Copy, Debug)]
pub struct ImportPlannedStmt {
    /// `pstmt->commandType` — always `CMD_UTILITY` here.
    pub command_type: CmdType,
    /// `pstmt->canSetTag` — always `false` here.
    pub can_set_tag: bool,
    /// `pstmt->utilityStmt` — the `CreateForeignTableStmt` node.
    pub utility_stmt: UtilityStmtHandle,
    /// `pstmt->stmt_location`.
    pub stmt_location: i32,
    /// `pstmt->stmt_len`.
    pub stmt_len: i32,
}

/// `CmdType` (`nodes/nodes.h`) — the subset `ImportForeignSchema` uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum CmdType {
    /// `CMD_UTILITY`.
    Utility = 6,
}

pub use CmdType::Utility as CMD_UTILITY;

/// Re-export of the raw-parse-tree handle the IMPORT loop threads
/// (`pg_parse_query` → `RawStmt *`).
pub use types_plancache::RawStmtHandle;
