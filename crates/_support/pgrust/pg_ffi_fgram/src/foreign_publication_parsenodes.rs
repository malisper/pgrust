//! Parse-node command structs from `nodes/parsenodes.h` consumed by the
//! foreign-data-wrapper / foreign-server / user-mapping / foreign-table DDL
//! commands (`backend/commands/foreigncmds.c`) and the publication DDL commands
//! (`backend/commands/publicationcmds.c`).
//!
//! Each `struct` mirrors the C layout field-for-field (`#[repr(C)]`, same field
//! order, first field `type_: NodeTag`).  Pointers to node types not modeled in
//! detail are carried as `*mut Node`/`*mut List`/`*mut RangeVar`/`*mut RoleSpec`,
//! matching the C `Node *`/`List *`/`RangeVar *`/`RoleSpec *` shapes.
//!
//! `T_*` NodeTag discriminants are verified against
//! `build-rust/src/include/nodes/nodetags.h` (PostgreSQL 18.3).
//!
//! Referenced by path (`pg_ffi_fgram::foreign_publication_parsenodes::*`) and
//! deliberately NOT in the crate-root glob, to avoid ambiguous-glob collisions
//! with the widely-named `List`/`Node`/`RangeVar`/`RoleSpec` types it uses, and
//! with the `CreateStmt` re-exported from `commands_ddl_parsenodes`.

use core::ffi::c_char;

use crate::commands_parsenodes::RoleSpec;
use crate::{List, Node, NodeTag, ParseLoc, RangeVar};

// ---------------------------------------------------------------------------
// NodeTag discriminants (nodes/nodetags.h, PostgreSQL 18.3)
// ---------------------------------------------------------------------------

pub const T_CreateFdwStmt: NodeTag = 169;
pub const T_AlterFdwStmt: NodeTag = 170;
pub const T_CreateForeignServerStmt: NodeTag = 171;
pub const T_AlterForeignServerStmt: NodeTag = 172;
pub const T_CreateForeignTableStmt: NodeTag = 173;
pub const T_CreateUserMappingStmt: NodeTag = 174;
pub const T_AlterUserMappingStmt: NodeTag = 175;
pub const T_DropUserMappingStmt: NodeTag = 176;
pub const T_PublicationTable: NodeTag = 259;
pub const T_PublicationObjSpec: NodeTag = 260;
pub const T_CreatePublicationStmt: NodeTag = 261;
pub const T_AlterPublicationStmt: NodeTag = 262;

// ---------------------------------------------------------------------------
// CreateStmt prefix used by CreateForeignTableStmt.
//
// `CreateForeignTableStmt` embeds a full `CreateStmt` as its first field
// (`CreateStmt base;`), so the foreign-table struct repeats the entire
// `CreateStmt` layout inline.  We mirror the same field set as the canonical
// `commands_ddl_parsenodes::CreateStmt` so `CreateForeignTableStmt` has an
// identical `repr(C)` layout to the C struct.
// ---------------------------------------------------------------------------

/// `typedef struct CreateStmt` (parsenodes.h) — embedded as `base` in
/// [`CreateForeignTableStmt`].  Identical layout to
/// `crate::commands_ddl_parsenodes::CreateStmt`; redeclared here to keep
/// [`CreateForeignTableStmt`] self-contained for layout assertions.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub tableElts: *mut List,
    pub inhRelations: *mut List,
    pub partbound: *mut Node,
    pub partspec: *mut Node,
    pub ofTypename: *mut crate::TypeName,
    pub constraints: *mut List,
    /// `nnconstraints` (NOT NULL constraints) — PG18 parsenodes.h:2752 places this
    /// between `constraints` and `options`; omitting it shifted `options`+ every
    /// later field 8 bytes earlier (104B vs canonical 112B), corrupting the
    /// embedded CreateForeignTableStmt's servername/options offsets.
    pub nnconstraints: *mut List,
    pub options: *mut List,
    pub oncommit: core::ffi::c_int,
    pub tablespacename: *mut c_char,
    pub accessMethod: *mut c_char,
    pub if_not_exists: bool,
}

// ---------------------------------------------------------------------------
// Foreign-data-wrapper / server / user-mapping / table parse nodes.
// ---------------------------------------------------------------------------

/// `typedef struct CreateFdwStmt` (parsenodes.h) — CREATE FOREIGN DATA WRAPPER.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateFdwStmt {
    pub type_: NodeTag,
    /// foreign-data wrapper name
    pub fdwname: *mut c_char,
    /// HANDLER/VALIDATOR options
    pub func_options: *mut List,
    /// generic options to FDW
    pub options: *mut List,
}

/// `typedef struct AlterFdwStmt` (parsenodes.h) — ALTER FOREIGN DATA WRAPPER.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterFdwStmt {
    pub type_: NodeTag,
    /// foreign-data wrapper name
    pub fdwname: *mut c_char,
    /// HANDLER/VALIDATOR options
    pub func_options: *mut List,
    /// generic options to FDW
    pub options: *mut List,
}

/// `typedef struct CreateForeignServerStmt` (parsenodes.h) — CREATE SERVER.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateForeignServerStmt {
    pub type_: NodeTag,
    /// server name
    pub servername: *mut c_char,
    /// optional server type
    pub servertype: *mut c_char,
    /// optional server version
    pub version: *mut c_char,
    /// FDW name
    pub fdwname: *mut c_char,
    /// just do nothing if it already exists?
    pub if_not_exists: bool,
    /// generic options to server
    pub options: *mut List,
}

/// `typedef struct AlterForeignServerStmt` (parsenodes.h) — ALTER SERVER.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterForeignServerStmt {
    pub type_: NodeTag,
    /// server name
    pub servername: *mut c_char,
    /// optional server version
    pub version: *mut c_char,
    /// generic options to server
    pub options: *mut List,
    /// version specified
    pub has_version: bool,
}

/// `typedef struct CreateForeignTableStmt` (parsenodes.h) — CREATE FOREIGN
/// TABLE.  Embeds a full [`CreateStmt`] as `base`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateForeignTableStmt {
    pub base: CreateStmt,
    pub servername: *mut c_char,
    pub options: *mut List,
}

/// `typedef struct CreateUserMappingStmt` (parsenodes.h) — CREATE USER MAPPING.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateUserMappingStmt {
    pub type_: NodeTag,
    /// user role
    pub user: *mut RoleSpec,
    /// server name
    pub servername: *mut c_char,
    /// just do nothing if it already exists?
    pub if_not_exists: bool,
    /// generic options to server
    pub options: *mut List,
}

/// `typedef struct AlterUserMappingStmt` (parsenodes.h) — ALTER USER MAPPING.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterUserMappingStmt {
    pub type_: NodeTag,
    /// user role
    pub user: *mut RoleSpec,
    /// server name
    pub servername: *mut c_char,
    /// generic options to server
    pub options: *mut List,
}

/// `typedef struct DropUserMappingStmt` (parsenodes.h) — DROP USER MAPPING.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DropUserMappingStmt {
    pub type_: NodeTag,
    /// user role
    pub user: *mut RoleSpec,
    /// server name
    pub servername: *mut c_char,
    /// ignore missing mappings
    pub missing_ok: bool,
}

// ---------------------------------------------------------------------------
// Publication parse nodes.
// ---------------------------------------------------------------------------

/// `typedef struct PublicationTable` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PublicationTable {
    pub type_: NodeTag,
    /// relation to be published
    pub relation: *mut RangeVar,
    /// qualifications
    pub whereClause: *mut Node,
    /// List of columns in a publication table
    pub columns: *mut List,
}

/// `typedef enum PublicationObjSpecType` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum PublicationObjSpecType {
    /// A table
    PUBLICATIONOBJ_TABLE,
    /// All tables in schema
    PUBLICATIONOBJ_TABLES_IN_SCHEMA,
    /// All tables in first element of search_path
    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA,
    /// Continuation of previous type
    PUBLICATIONOBJ_CONTINUATION,
}
pub use PublicationObjSpecType::*;

/// `typedef struct PublicationObjSpec` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PublicationObjSpec {
    pub type_: NodeTag,
    /// type of this publication object
    pub pubobjtype: PublicationObjSpecType,
    pub name: *mut c_char,
    pub pubtable: *mut PublicationTable,
    /// token location, or -1 if unknown
    pub location: ParseLoc,
}

/// `typedef struct CreatePublicationStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreatePublicationStmt {
    pub type_: NodeTag,
    /// Name of the publication
    pub pubname: *mut c_char,
    /// List of DefElem nodes
    pub options: *mut List,
    /// Optional list of publication objects
    pub pubobjects: *mut List,
    /// Special publication for all tables in db
    pub for_all_tables: bool,
}

/// `typedef enum AlterPublicationAction` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum AlterPublicationAction {
    /// add objects to publication
    AP_AddObjects,
    /// remove objects from publication
    AP_DropObjects,
    /// set list of objects
    AP_SetObjects,
}
pub use AlterPublicationAction::*;

/// `typedef struct AlterPublicationStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterPublicationStmt {
    pub type_: NodeTag,
    /// Name of the publication
    pub pubname: *mut c_char,
    /// List of DefElem nodes (ALTER PUBLICATION ... WITH)
    pub options: *mut List,
    /// Optional list of publication objects
    pub pubobjects: *mut List,
    /// Special publication for all tables in db
    pub for_all_tables: bool,
    /// What action to perform with the given objects
    pub action: AlterPublicationAction,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{offset_of, size_of};

    #[test]
    fn create_fdw_stmt_layout() {
        assert_eq!(offset_of!(CreateFdwStmt, type_), 0);
        assert_eq!(offset_of!(CreateFdwStmt, fdwname), 8);
        assert_eq!(offset_of!(CreateFdwStmt, func_options), 16);
        assert_eq!(offset_of!(CreateFdwStmt, options), 24);
    }

    #[test]
    fn create_foreign_server_stmt_layout() {
        assert_eq!(offset_of!(CreateForeignServerStmt, servername), 8);
        assert_eq!(offset_of!(CreateForeignServerStmt, servertype), 16);
        assert_eq!(offset_of!(CreateForeignServerStmt, version), 24);
        assert_eq!(offset_of!(CreateForeignServerStmt, fdwname), 32);
        assert_eq!(offset_of!(CreateForeignServerStmt, if_not_exists), 40);
        assert_eq!(offset_of!(CreateForeignServerStmt, options), 48);
    }

    #[test]
    fn alter_foreign_server_stmt_layout() {
        assert_eq!(offset_of!(AlterForeignServerStmt, servername), 8);
        assert_eq!(offset_of!(AlterForeignServerStmt, version), 16);
        assert_eq!(offset_of!(AlterForeignServerStmt, options), 24);
        assert_eq!(offset_of!(AlterForeignServerStmt, has_version), 32);
    }

    #[test]
    fn create_user_mapping_stmt_layout() {
        assert_eq!(offset_of!(CreateUserMappingStmt, user), 8);
        assert_eq!(offset_of!(CreateUserMappingStmt, servername), 16);
        assert_eq!(offset_of!(CreateUserMappingStmt, if_not_exists), 24);
        assert_eq!(offset_of!(CreateUserMappingStmt, options), 32);
    }

    #[test]
    fn drop_user_mapping_stmt_layout() {
        assert_eq!(offset_of!(DropUserMappingStmt, user), 8);
        assert_eq!(offset_of!(DropUserMappingStmt, servername), 16);
        assert_eq!(offset_of!(DropUserMappingStmt, missing_ok), 24);
    }

    #[test]
    fn create_foreign_table_stmt_layout() {
        // base is a full CreateStmt; servername/options follow it.
        let base_size = size_of::<CreateStmt>();
        assert_eq!(offset_of!(CreateForeignTableStmt, base), 0);
        assert_eq!(offset_of!(CreateForeignTableStmt, servername), base_size);
        assert_eq!(
            offset_of!(CreateForeignTableStmt, options),
            base_size + size_of::<*mut c_char>()
        );
    }

    #[test]
    fn publication_table_layout() {
        assert_eq!(offset_of!(PublicationTable, relation), 8);
        assert_eq!(offset_of!(PublicationTable, whereClause), 16);
        assert_eq!(offset_of!(PublicationTable, columns), 24);
    }

    #[test]
    fn publication_obj_spec_layout() {
        // type_ (NodeTag, 4) + pubobjtype (i32, 4) pack into the first 8 bytes,
        // then the `name` pointer is 8-aligned at offset 8.
        assert_eq!(offset_of!(PublicationObjSpec, pubobjtype), 4);
        assert_eq!(offset_of!(PublicationObjSpec, name), 8);
        assert_eq!(offset_of!(PublicationObjSpec, pubtable), 16);
        assert_eq!(offset_of!(PublicationObjSpec, location), 24);
    }

    #[test]
    fn create_publication_stmt_layout() {
        assert_eq!(offset_of!(CreatePublicationStmt, pubname), 8);
        assert_eq!(offset_of!(CreatePublicationStmt, options), 16);
        assert_eq!(offset_of!(CreatePublicationStmt, pubobjects), 24);
        assert_eq!(offset_of!(CreatePublicationStmt, for_all_tables), 32);
    }

    #[test]
    fn alter_publication_stmt_layout() {
        assert_eq!(offset_of!(AlterPublicationStmt, pubname), 8);
        assert_eq!(offset_of!(AlterPublicationStmt, options), 16);
        assert_eq!(offset_of!(AlterPublicationStmt, pubobjects), 24);
        assert_eq!(offset_of!(AlterPublicationStmt, for_all_tables), 32);
        assert_eq!(offset_of!(AlterPublicationStmt, action), 36);
    }

    #[test]
    fn nodetag_values() {
        assert_eq!(T_CreateFdwStmt, 169);
        assert_eq!(T_DropUserMappingStmt, 176);
        assert_eq!(T_PublicationTable, 259);
        assert_eq!(T_AlterPublicationStmt, 262);
    }
}
