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

use types_core::Oid;

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
}

impl Node {
    /// `IsA(node, String)` accessor — `strVal` reads off this.
    pub fn as_string(&self) -> Option<&StringNode> {
        match self {
            Node::String(s) => Some(s),
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
