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
