//! Parse-tree node vocabulary (`nodes/parsenodes.h`, `nodes/value.h`,
//! `nodes/primnodes.h`), trimmed to what the command drivers consume.
//!
//! This is the *raw-parser* node model — owned, heap-allocated, distinct from
//! the executor/plan-node dispatch enum in `types_nodes::nodes` (`Plan *`). C's
//! `Node *` over Value/DefElem/TypeName/statement nodes becomes the [`Node`]
//! enum here; copies are plain Rust `.clone()` (raw parse trees are not
//! context-allocated through `mcx` the way plan trees are).
//!
//! Variants and structs are added as command ports consume them; only the
//! fields a port reads are carried (docs/types.md rule 3).
//!
//! `RoleStmtType`/`RoleSpecType`/`DefElemAction` values are verified against
//! PostgreSQL 18.3 headers.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use types_core::Oid;
use types_nodes::parsenodes::{DropBehavior, ObjectType};

/// `int ParseLoc` (`nodes/parsenodes.h`) — token location, `-1` if unknown.
pub type ParseLoc = i32;

// ---------------------------------------------------------------------------
// Value nodes (nodes/value.h)
// ---------------------------------------------------------------------------

/// `typedef struct Integer` (`nodes/value.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Integer {
    pub ival: i32,
}

/// `typedef struct Float` (`nodes/value.h`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Float {
    pub fval: Option<String>,
}

/// `typedef struct Boolean` (`nodes/value.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Boolean {
    pub boolval: bool,
}

/// `typedef struct String` (`nodes/value.h`) — named `StringNode` so it does
/// not collide with Rust's `String`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StringNode {
    pub sval: Option<String>,
}

/// `typedef struct BitString` (`nodes/value.h`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BitString {
    pub bsval: Option<String>,
}

// ---------------------------------------------------------------------------
// TypeName (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct TypeName` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TypeName {
    /// qualified name (list of String nodes).
    pub names: Vec<Node>,
    /// type identified by OID.
    pub typeOid: Oid,
    /// is a set?
    pub setof: bool,
    /// %TYPE specified?
    pub pct_type: bool,
    /// type modifier expression(s).
    pub typmods: Vec<Node>,
    /// prespecified type modifier.
    pub typemod: i32,
    /// array bounds.
    pub arrayBounds: Vec<Node>,
    /// token location, or -1 if unknown.
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// DefElem (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef enum DefElemAction` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum DefElemAction {
    /// no action given.
    #[default]
    DEFELEM_UNSPEC = 0,
    DEFELEM_SET = 1,
    DEFELEM_ADD = 2,
    DEFELEM_DROP = 3,
}
pub use DefElemAction::{DEFELEM_ADD, DEFELEM_DROP, DEFELEM_SET, DEFELEM_UNSPEC};

/// `typedef struct DefElem` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DefElem {
    pub defnamespace: Option<String>,
    pub defname: Option<String>,
    pub arg: Option<Box<Node>>,
    pub defaction: DefElemAction,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// ObjectWithArgs (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct ObjectWithArgs` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectWithArgs {
    /// qualified name of function/operator (list of String).
    pub objname: Vec<String>,
    /// list of TypeName nodes (input args only).
    pub objargs: Vec<Node>,
    /// list of FunctionParameter nodes.
    pub objfuncargs: Vec<Node>,
    pub args_unspecified: bool,
}

// ---------------------------------------------------------------------------
// FunctionParameter (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef enum FunctionParameterMode` values (`nodes/parsenodes.h`) — C
/// stores the mode as a `char`, so these are the `char` codes.
pub const FUNC_PARAM_IN: i8 = b'i' as i8;
pub const FUNC_PARAM_OUT: i8 = b'o' as i8;
pub const FUNC_PARAM_INOUT: i8 = b'b' as i8;
pub const FUNC_PARAM_VARIADIC: i8 = b'v' as i8;
pub const FUNC_PARAM_TABLE: i8 = b't' as i8;
/// default; effectively same as IN.
pub const FUNC_PARAM_DEFAULT: i8 = b'd' as i8;

/// `typedef struct FunctionParameter` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionParameter {
    pub name: Option<String>,
    pub argType: Option<Box<Node>>,
    pub mode: i8,
    pub defexpr: Option<Box<Node>>,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// CoercionContext (nodes/primnodes.h)
// ---------------------------------------------------------------------------

/// `typedef enum CoercionContext` (`nodes/primnodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum CoercionContext {
    /// coercion in context of expression.
    #[default]
    COERCION_IMPLICIT = 0,
    /// coercion in context of assignment.
    COERCION_ASSIGNMENT = 1,
    /// if no assignment cast, use CoerceViaIO.
    COERCION_PLPGSQL = 2,
    /// explicit cast operation.
    COERCION_EXPLICIT = 3,
}

// ---------------------------------------------------------------------------
// AccessPriv (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct AccessPriv` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessPriv {
    /// string name of privilege
    pub priv_name: Option<String>,
    /// list of String
    pub cols: Vec<Node>,
}

// ---------------------------------------------------------------------------
// RoleSpec (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Statement nodes (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct CreateFunctionStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug)]
pub struct CreateFunctionStmt {
    pub is_procedure: bool,
    pub replace: bool,
    /// qualified name of function to create (list of String).
    pub funcname: Vec<StringNode>,
    /// a list of FunctionParameter.
    pub parameters: Vec<Node>,
    pub returnType: Option<Box<Node>>,
    /// a list of DefElem.
    pub options: Vec<Node>,
    pub sql_body: Option<Box<Node>>,
}

/// `typedef struct AlterFunctionStmt` (`nodes/parsenodes.h`). `objtype` is the
/// `ObjectType` enum value (carried as `i32`).
#[derive(Clone, Debug)]
pub struct AlterFunctionStmt {
    pub objtype: i32,
    /// `ObjectWithArgs *func`.
    pub func: Option<Box<Node>>,
    /// list of DefElem.
    pub actions: Vec<Node>,
}

/// `typedef struct CommentStmt` (`nodes/parsenodes.h`) — `COMMENT ON`.
///
/// `objtype` is the `ObjectType` of the commented object; `object` is the raw
/// parser node naming it (a `String` value node for DATABASE/SCHEMA/ROLE/…, a
/// `List *` for a qualified or per-column name, etc.) — opaque to this crate,
/// handed to `get_object_address`/`check_object_ownership`. `comment` is the
/// `NULL`-or-string comment text (`COMMENT ... IS NULL` drops the comment).
#[derive(Clone, Debug)]
pub struct CommentStmt {
    pub objtype: types_nodes::parsenodes::ObjectType,
    /// `Node *object` — the parser representation of the object to comment on.
    pub object: Option<Box<Node>>,
    /// `char *comment` — the comment text, or `None` for `IS NULL`.
    pub comment: Option<String>,
}

/// `typedef struct DoStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug)]
pub struct DoStmt {
    /// list of DefElem nodes.
    pub args: Vec<Node>,
}

/// `typedef struct InlineCodeBlock` (`nodes/parsenodes.h`) — execution-time
/// API for DO.
#[derive(Clone, Debug)]
pub struct InlineCodeBlock {
    pub source_text: Option<String>,
    pub langOid: Oid,
    pub langIsTrusted: bool,
    pub atomic: bool,
}

/// `typedef struct CallStmt` (`nodes/parsenodes.h`). `funccall`/`funcexpr` are
/// carried opaquely.
#[derive(Clone, Debug)]
pub struct CallStmt {
    pub funccall: Option<Box<Node>>,
    pub funcexpr: Option<Box<Node>>,
    /// transformed output-argument expressions.
    pub outargs: Vec<Node>,
}

/// `typedef struct CreateTransformStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug)]
pub struct CreateTransformStmt {
    pub replace: bool,
    pub type_name: Option<Box<Node>>,
    pub lang: Option<String>,
    pub fromsql: Option<Box<Node>>,
    pub tosql: Option<Box<Node>>,
}

/// `typedef struct CreateCastStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug)]
pub struct CreateCastStmt {
    pub sourcetype: Option<Box<Node>>,
    pub targettype: Option<Box<Node>>,
    pub func: Option<Box<Node>>,
    pub context: CoercionContext,
    pub inout: bool,
}

// ---------------------------------------------------------------------------
// Role statement nodes (nodes/parsenodes.h) consumed by user.c
// ---------------------------------------------------------------------------

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

/// `typedef enum VariableSetKind` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VariableSetKind {
    /// `SET var = value`
    SetValue,
    /// `SET var TO DEFAULT`
    SetDefault,
    /// `SET var FROM CURRENT`
    SetCurrent,
    /// special case for `SET TRANSACTION ...`
    SetMulti,
    /// `RESET var`
    Reset,
    /// `RESET ALL`
    ResetAll,
}

/// `typedef struct VariableSetStmt` (`nodes/parsenodes.h`), trimmed to the
/// fields `update_proconfig_value` consumes. The value extraction over the
/// `args` list (`A_Const` nodes) lives behind the GUC owner's seam
/// (`ExtractSetVariableArgs`), so the args are carried opaquely for that owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VariableSetStmt {
    pub kind: VariableSetKind,
    /// variable to be set (None for `RESET ALL`)
    pub name: Option<String>,
    /// List of `A_Const` nodes (the SET arguments), carried for the GUC owner.
    pub args: Vec<Node>,
    /// `SET LOCAL`?
    pub is_local: bool,
    /// token location, or -1 if unknown
    pub location: ParseLoc,
}

/// `typedef struct DropStmt` (`nodes/parsenodes.h`) — DROP of one or more
/// generic objects (TYPE / DOMAIN / FUNCTION / AGGREGATE / OPERATOR / SCHEMA /
/// CAST / TRANSFORM / TRIGGER / RULE / POLICY / TEXT SEARCH … — every object
/// type handled by `RemoveObjects`).
#[derive(Clone, Debug, PartialEq)]
pub struct DropStmt {
    /// `objects` — list of names. Each element is a polymorphic value node
    /// (`String`, `TypeName`, `ObjectWithArgs`, or `List`) per `removeType`.
    pub objects: Vec<Node>,
    /// `removeType` — object type.
    pub removeType: ObjectType,
    /// `behavior` — RESTRICT or CASCADE.
    pub behavior: DropBehavior,
    /// `missing_ok` — skip error if object is missing?
    pub missing_ok: bool,
    /// `concurrent` — drop index concurrently?
    pub concurrent: bool,
}

/// `typedef enum DiscardMode` (`nodes/parsenodes.h`) — verified against
/// PostgreSQL 18.3 (`DISCARD { ALL | PLANS | SEQUENCES | TEMP }`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscardMode {
    DISCARD_ALL,
    DISCARD_PLANS,
    DISCARD_SEQUENCES,
    DISCARD_TEMP,
}

/// `typedef struct DiscardStmt` (`nodes/parsenodes.h`). The C `NodeTag type`
/// header field carries no information for the dispatcher (it switches solely
/// on `target`) and there is no `NodeTag` enum in this tree, so it is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscardStmt {
    pub target: DiscardMode,
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

/// `typedef struct SecLabelStmt` (`nodes/parsenodes.h`) — the SECURITY LABEL
/// command parse node. `object` is the opaque parser representation of the
/// target object (a qualified-name `List *` / typename / etc.), passed through
/// to `get_object_address`/`check_object_ownership`. `provider` and `label` are
/// `NULL` in C when omitted (`SECURITY LABEL ... IS NULL` removes the label).
#[derive(Clone, Debug, PartialEq)]
pub struct SecLabelStmt {
    /// Object's type.
    pub objtype: types_nodes::parsenodes::ObjectType,
    /// Qualified name of the object.
    pub object: Option<Box<Node>>,
    /// Label provider (or `None`).
    pub provider: Option<String>,
    /// New security label to be assigned (or `None` to remove).
    pub label: Option<String>,
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

// ---------------------------------------------------------------------------
// The parse-tree Node enum (nodes/nodes.h `Node *` over the structs above)
// ---------------------------------------------------------------------------

/// A raw-parser `Node *`. Variants are added as command ports consume them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Node {
    Integer(Integer),
    Float(Float),
    Boolean(Boolean),
    String(StringNode),
    BitString(BitString),
    TypeName(TypeName),
    DefElem(DefElem),
    ObjectWithArgs(ObjectWithArgs),
    FunctionParameter(FunctionParameter),
    /// `T_RoleSpec`
    RoleSpec(RoleSpec),
    /// `T_AccessPriv`
    AccessPriv(AccessPriv),
    /// `T_VariableSetStmt` — a SET/RESET subcommand.
    VariableSetStmt(VariableSetStmt),
    /// `T_List` — a generic `List *` of nodes (e.g. a `DefElem` value that is a
    /// possibly-qualified name list, or `defGetStringList`'s list of `String`s).
    List(Vec<Node>),
    /// `T_A_Star` — the `*` wildcard value node (`nodes/parsenodes.h`).
    A_Star,
}

impl Node {
    /// `IsA(node, String)` accessor — `strVal` reads off this.
    pub fn as_string(&self) -> Option<&StringNode> {
        match self {
            Node::String(s) => Some(s),
            _ => None,
        }
    }

    /// `IsA(node, Integer)` accessor.
    pub fn as_integer(&self) -> Option<&Integer> {
        match self {
            Node::Integer(i) => Some(i),
            _ => None,
        }
    }

    /// `IsA(node, Boolean)` accessor — `boolVal` reads off this.
    pub fn as_boolean(&self) -> Option<&Boolean> {
        match self {
            Node::Boolean(b) => Some(b),
            _ => None,
        }
    }

    /// `IsA(node, TypeName)` accessor.
    pub fn as_typename(&self) -> Option<&TypeName> {
        match self {
            Node::TypeName(t) => Some(t),
            _ => None,
        }
    }

    /// `IsA(node, DefElem)` accessor.
    pub fn as_defelem(&self) -> Option<&DefElem> {
        match self {
            Node::DefElem(d) => Some(d),
            _ => None,
        }
    }

    /// `IsA(node, FunctionParameter)` accessor.
    pub fn as_functionparameter(&self) -> Option<&FunctionParameter> {
        match self {
            Node::FunctionParameter(f) => Some(f),
            _ => None,
        }
    }

    /// `IsA(node, ObjectWithArgs)` accessor.
    pub fn as_objectwithargs(&self) -> Option<&ObjectWithArgs> {
        match self {
            Node::ObjectWithArgs(o) => Some(o),
            _ => None,
        }
    }

    /// `IsA(node, RoleSpec)` accessor.
    pub fn as_rolespec(&self) -> Option<&RoleSpec> {
        match self {
            Node::RoleSpec(rs) => Some(rs),
            _ => None,
        }
    }

    /// `IsA(node, AccessPriv)` accessor.
    pub fn as_accesspriv(&self) -> Option<&AccessPriv> {
        match self {
            Node::AccessPriv(a) => Some(a),
            _ => None,
        }
    }

    /// `IsA(node, List)` accessor — the cells of a generic `List *`.
    pub fn as_list(&self) -> Option<&[Node]> {
        match self {
            Node::List(l) => Some(l),
            _ => None,
        }
    }

    /// `castNode(Float, node)->fval` accessor.
    pub fn as_float(&self) -> Option<&Float> {
        match self {
            Node::Float(f) => Some(f),
            _ => None,
        }
    }

    /// The node's tag name, used in the `unrecognized node type` / `unexpected
    /// node type in name list` error messages (C prints the numeric `NodeTag`;
    /// the trimmed enum has no numeric tag, so we print the C type name).
    pub fn node_tag_name(&self) -> &'static str {
        match self {
            Node::Integer(_) => "Integer",
            Node::Float(_) => "Float",
            Node::Boolean(_) => "Boolean",
            Node::String(_) => "String",
            Node::BitString(_) => "BitString",
            Node::TypeName(_) => "TypeName",
            Node::DefElem(_) => "DefElem",
            Node::ObjectWithArgs(_) => "ObjectWithArgs",
            Node::FunctionParameter(_) => "FunctionParameter",
            Node::RoleSpec(_) => "RoleSpec",
            Node::AccessPriv(_) => "AccessPriv",
            Node::VariableSetStmt(_) => "VariableSetStmt",
            Node::List(_) => "List",
            Node::A_Star => "A_Star",
        }
    }
}

// ---------------------------------------------------------------------------
// pg_proc / pg_cast / pg_type catalog vocabulary (catalog/pg_proc.h,
// catalog/pg_cast.h) consumed by functioncmds.c
// ---------------------------------------------------------------------------

/// `ProcedureRelationId` — pg_proc relation OID (`catalog/pg_proc_d.h`, 1255).
pub const ProcedureRelationId: Oid = 1255;

// pg_proc.prokind values (`catalog/pg_proc.h`).
pub const PROKIND_FUNCTION: i8 = b'f' as i8;
pub const PROKIND_AGGREGATE: i8 = b'a' as i8;
pub const PROKIND_WINDOW: i8 = b'w' as i8;
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

// pg_proc.provolatile values (`catalog/pg_proc.h`).
pub const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
pub const PROVOLATILE_STABLE: i8 = b's' as i8;
pub const PROVOLATILE_VOLATILE: i8 = b'v' as i8;

// pg_proc.proparallel values (`catalog/pg_proc.h`).
pub const PROPARALLEL_SAFE: i8 = b's' as i8;
pub const PROPARALLEL_RESTRICTED: i8 = b'r' as i8;
pub const PROPARALLEL_UNSAFE: i8 = b'u' as i8;

// pg_aggregate.aggkind values (`catalog/pg_aggregate.h`).
pub const AGGKIND_NORMAL: i8 = b'n' as i8;
pub const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
pub const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

// pg_aggregate.aggfinalmodify / aggmfinalmodify values (`catalog/pg_aggregate.h`).
pub const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;
pub const AGGMODIFY_SHAREABLE: i8 = b's' as i8;
pub const AGGMODIFY_READ_WRITE: i8 = b'w' as i8;

// pg_cast.castmethod values (`catalog/pg_cast.h`).
pub const COERCION_METHOD_FUNCTION: i8 = b'f' as i8;
pub const COERCION_METHOD_BINARY: i8 = b'b' as i8;
pub const COERCION_METHOD_INOUT: i8 = b'i' as i8;

// pg_cast.castcontext values (`catalog/pg_cast.h`).
pub const COERCION_CODE_IMPLICIT: i8 = b'i' as i8;
pub const COERCION_CODE_ASSIGNMENT: i8 = b'a' as i8;
pub const COERCION_CODE_EXPLICIT: i8 = b'e' as i8;

// pg_type.typtype values (`catalog/pg_type.h`).
pub const TYPTYPE_BASE: i8 = b'b' as i8;
pub const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;
pub const TYPTYPE_ENUM: i8 = b'e' as i8;
pub const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
pub const TYPTYPE_PSEUDO: i8 = b'p' as i8;
pub const TYPTYPE_RANGE: i8 = b'r' as i8;
