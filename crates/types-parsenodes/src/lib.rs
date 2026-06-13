//! Raw-statement parse-node vocabulary (`nodes/parsenodes.h`, `nodes/value.h`,
//! `nodes/primnodes.h`), trimmed to what the command executors consume.
//!
//! C's parser builds a tree of `Node *`; against the owned model a parse-tree
//! node is the [`Node`] enum here. Variants are added as command units that
//! consume them are ported. This is distinct from `types_nodes::Node`, which
//! is the *plan*-tree dispatch enum.
//!
//! `RoleStmtType`/`RoleSpecType`/`DefElemAction` values are verified against
//! PostgreSQL 18.3 headers.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use types_core::primitive::ParseLoc;
use types_nodes::parsenodes::DropBehavior;

/// `typedef struct String` (`nodes/value.h`) — `T_String` value node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct String_ {
    pub sval: Option<String>,
}

/// `typedef struct Integer` (`nodes/value.h`) — `T_Integer` value node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Integer {
    pub ival: i32,
}

/// `typedef struct Boolean` (`nodes/value.h`) — `T_Boolean` value node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Boolean {
    pub boolval: bool,
}

/// `typedef struct Float` (`nodes/value.h`) — `T_Float` value node (the
/// numeric value is kept as its text form, as the C does).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Float {
    pub fval: Option<String>,
}

/// `typedef enum RoleSpecType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum RoleSpecType {
    /// role name is stored as a C string
    ROLESPEC_CSTRING = 0,
    /// role spec is CURRENT_ROLE
    ROLESPEC_CURRENT_ROLE = 1,
    /// role spec is CURRENT_USER
    ROLESPEC_CURRENT_USER = 2,
    /// role spec is SESSION_USER
    ROLESPEC_SESSION_USER = 3,
    /// role name is "public"
    ROLESPEC_PUBLIC = 4,
}
pub use RoleSpecType::{
    ROLESPEC_CSTRING, ROLESPEC_CURRENT_ROLE, ROLESPEC_CURRENT_USER, ROLESPEC_PUBLIC,
    ROLESPEC_SESSION_USER,
};

/// `typedef struct RoleSpec` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleSpec {
    pub roletype: RoleSpecType,
    /// filled only for `ROLESPEC_CSTRING`
    pub rolename: Option<String>,
    pub location: ParseLoc,
}

/// `typedef enum DefElemAction` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum DefElemAction {
    /// no action given
    DEFELEM_UNSPEC = 0,
    DEFELEM_SET = 1,
    DEFELEM_ADD = 2,
    DEFELEM_DROP = 3,
}
pub use DefElemAction::{DEFELEM_ADD, DEFELEM_DROP, DEFELEM_SET, DEFELEM_UNSPEC};

/// `typedef struct DefElem` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct DefElem {
    /// NULL if unqualified name
    pub defnamespace: Option<String>,
    pub defname: Option<String>,
    /// typically Integer, Float, String, or TypeName
    pub arg: Option<Box<Node>>,
    pub defaction: DefElemAction,
    pub location: ParseLoc,
}

/// `typedef struct AccessPriv` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct AccessPriv {
    /// string name of privilege
    pub priv_name: Option<String>,
    /// list of String
    pub cols: Vec<Node>,
}

/// `typedef enum RoleStmtType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum RoleStmtType {
    ROLESTMT_ROLE = 0,
    ROLESTMT_USER = 1,
    ROLESTMT_GROUP = 2,
}
pub use RoleStmtType::{ROLESTMT_GROUP, ROLESTMT_ROLE, ROLESTMT_USER};

/// `typedef struct CreateRoleStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct CreateRoleStmt {
    pub stmt_type: RoleStmtType,
    /// role name
    pub role: Option<String>,
    /// List of DefElem nodes
    pub options: Vec<Node>,
}

/// `typedef struct AlterRoleStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct AlterRoleStmt {
    /// role
    pub role: Option<Box<Node>>,
    /// List of DefElem nodes
    pub options: Vec<Node>,
    /// +1 = add members, -1 = drop members
    pub action: i32,
}

/// `typedef struct AlterRoleSetStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct AlterRoleSetStmt {
    /// role
    pub role: Option<Box<Node>>,
    /// database name, or None
    pub database: Option<String>,
    /// SET or RESET subcommand (a `VariableSetStmt`, carried opaquely)
    pub setstmt: Option<Box<Node>>,
}

/// `typedef struct DropRoleStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct DropRoleStmt {
    /// List of roles to remove
    pub roles: Vec<Node>,
    /// skip error if a role is missing?
    pub missing_ok: bool,
}

/// `typedef struct GrantRoleStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct GrantRoleStmt {
    /// list of roles to be granted/revoked (list of `AccessPriv`)
    pub granted_roles: Vec<Node>,
    /// list of member roles to add/delete
    pub grantee_roles: Vec<Node>,
    /// true = GRANT, false = REVOKE
    pub is_grant: bool,
    /// options e.g. WITH GRANT OPTION (list of `DefElem`)
    pub opt: Vec<Node>,
    /// set grantor to other than current role
    pub grantor: Option<Box<Node>>,
    /// drop behavior (for REVOKE)
    pub behavior: DropBehavior,
}

/// `typedef struct DropOwnedStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct DropOwnedStmt {
    pub roles: Vec<Node>,
    pub behavior: DropBehavior,
}

/// `typedef struct ReassignOwnedStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct ReassignOwnedStmt {
    pub roles: Vec<Node>,
    pub newrole: Option<Box<Node>>,
}

/// `typedef struct ParseState` (`parser/parse_node.h`), trimmed. user.c only
/// passes the `ParseState *` through to `errorConflictingDefElem` /
/// `parser_errposition` for error positioning; the parser (its owner) fills
/// the source text. Kept opaque-but-real with the consumed field only.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParseState {
    /// `p_sourcetext` — the source text, used for error positioning.
    pub p_sourcetext: Option<String>,
}

/// A raw-statement parse-tree node (`Node *`). The discriminant carries the C
/// node tag; variants are added as consuming command units are ported.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Node {
    /// `T_String`
    String(String_),
    /// `T_Integer`
    Integer(Integer),
    /// `T_Boolean`
    Boolean(Boolean),
    /// `T_Float`
    Float(Float),
    /// `T_RoleSpec`
    RoleSpec(RoleSpec),
    /// `T_DefElem`
    DefElem(DefElem),
    /// `T_AccessPriv`
    AccessPriv(AccessPriv),
}

impl Node {
    /// `IsA(node, String)` → `&String` node.
    pub fn as_string(&self) -> Option<&String_> {
        match self {
            Node::String(s) => Some(s),
            _ => None,
        }
    }
    /// `IsA(node, Integer)`.
    pub fn as_integer(&self) -> Option<&Integer> {
        match self {
            Node::Integer(i) => Some(i),
            _ => None,
        }
    }
    /// `IsA(node, Boolean)`.
    pub fn as_boolean(&self) -> Option<&Boolean> {
        match self {
            Node::Boolean(b) => Some(b),
            _ => None,
        }
    }
    /// `IsA(node, RoleSpec)`.
    pub fn as_rolespec(&self) -> Option<&RoleSpec> {
        match self {
            Node::RoleSpec(rs) => Some(rs),
            _ => None,
        }
    }
    /// `IsA(node, DefElem)`.
    pub fn as_defelem(&self) -> Option<&DefElem> {
        match self {
            Node::DefElem(d) => Some(d),
            _ => None,
        }
    }
    /// `IsA(node, AccessPriv)`.
    pub fn as_accesspriv(&self) -> Option<&AccessPriv> {
        match self {
            Node::AccessPriv(a) => Some(a),
            _ => None,
        }
    }
}
