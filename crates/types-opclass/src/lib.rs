//! Parse-node and transient vocabulary for CREATE/ALTER OPERATOR CLASS &
//! OPERATOR FAMILY (`commands/opclasscmds.c`, `nodes/parsenodes.h`,
//! `access/amapi.h`).
//!
//! Per `docs/types.md` rule 6, the C `Node *` fields these statements carry
//! are resolved to the concrete node types the grammar always produces for
//! them (`TypeName`, `ObjectWithArgs`, `CreateOpClassItem`): `datatype` /
//! `storedtype` are always `TypeName`, `class_args` is a list of `TypeName`,
//! `name` is always `ObjectWithArgs`, and `items` is a list of
//! `CreateOpClassItem`. Opacity is not introduced where the grammar fixes the
//! type.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::primitive::Oid;

/// `T_CreateOpClassStmt` (nodes/nodetags.h) — values verified against
/// PostgreSQL 18.3.
pub const T_CreateOpClassStmt: u32 = 193;
pub const T_CreateOpClassItem: u32 = 194;
pub const T_CreateOpFamilyStmt: u32 = 195;
pub const T_AlterOpFamilyStmt: u32 = 196;

/// `#define OPCLASS_ITEM_OPERATOR 1` (parsenodes.h).
pub const OPCLASS_ITEM_OPERATOR: i32 = 1;
/// `#define OPCLASS_ITEM_FUNCTION 2` (parsenodes.h).
pub const OPCLASS_ITEM_FUNCTION: i32 = 2;
/// `#define OPCLASS_ITEM_STORAGETYPE 3` (parsenodes.h).
pub const OPCLASS_ITEM_STORAGETYPE: i32 = 3;

/// `#define AMOP_SEARCH 's'` (catalog/pg_amop.h) — operator is for search.
pub const AMOP_SEARCH: i8 = b's' as i8;
/// `#define AMOP_ORDER 'o'` (catalog/pg_amop.h) — operator is for ordering.
pub const AMOP_ORDER: i8 = b'o' as i8;

/// One component of a qualified name — a `String` value node
/// (`nodes/value.h`). The grammar never produces a NULL `sval` in these name
/// lists, but the field is `Option` to mirror C's nullable `String.sval`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StringNode {
    pub sval: Option<String>,
}

/// `typedef struct TypeName` (parsenodes.h), trimmed to the fields a type
/// resolver (`parse_type.c`) reads. opclasscmds.c never inspects this; it
/// passes it through to `typenameTypeId` / `TypeNameToString`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeName {
    /// qualified name (list of String nodes).
    pub names: Vec<String>,
    /// type identified by OID (when the grammar built it from an OID).
    pub typeOid: Oid,
    /// is a set?
    pub setof: bool,
    /// %TYPE specified?
    pub pct_type: bool,
    /// prespecified type modifier.
    pub typemod: i32,
    /// token location, or -1 if unknown.
    pub location: i32,
}

/// `typedef struct ObjectWithArgs` (parsenodes.h), trimmed to the fields
/// opclasscmds.c and the operator/function lookups consume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectWithArgs {
    /// qualified name of function/operator (list of String).
    pub objname: Vec<String>,
    /// list of TypeName nodes (input args only).
    pub objargs: Vec<TypeName>,
    pub args_unspecified: bool,
}

/// `typedef struct CreateOpClassItem` (parsenodes.h).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateOpClassItem {
    /// see `OPCLASS_ITEM_*`.
    pub itemtype: i32,
    /// operator or function name and args (`OPCLASS_ITEM_OPERATOR`/`_FUNCTION`).
    pub name: Option<ObjectWithArgs>,
    /// strategy num or support proc num.
    pub number: i32,
    /// only used for ordering operators (qualified opfamily name).
    pub order_family: Vec<StringNode>,
    /// amproclefttype/amprocrighttype or amoplefttype/amoprighttype.
    pub class_args: Vec<TypeName>,
    /// storage datatype (`OPCLASS_ITEM_STORAGETYPE`).
    pub storedtype: Option<TypeName>,
}

/// `typedef struct CreateOpClassStmt` (parsenodes.h).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateOpClassStmt {
    /// qualified name (list of String).
    pub opclassname: Vec<StringNode>,
    /// qualified name (ditto); empty if omitted (list of String).
    pub opfamilyname: Vec<StringNode>,
    pub amname: Option<String>,
    pub datatype: Option<TypeName>,
    /// list of CreateOpClassItem nodes.
    pub items: Vec<CreateOpClassItem>,
    pub isDefault: bool,
}

/// `typedef struct CreateOpFamilyStmt` (parsenodes.h).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateOpFamilyStmt {
    /// qualified name (list of String).
    pub opfamilyname: Vec<StringNode>,
    pub amname: Option<String>,
}

/// `typedef struct AlterOpFamilyStmt` (parsenodes.h).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterOpFamilyStmt {
    /// qualified name (list of String).
    pub opfamilyname: Vec<StringNode>,
    pub amname: Option<String>,
    pub isDrop: bool,
    /// list of CreateOpClassItem nodes.
    pub items: Vec<CreateOpClassItem>,
}

/// The scalar `IndexAmRoutine` fields opclasscmds.c reads
/// (`GetIndexAmRoutineByAmId(amoid, false)`), a caller-shaped projection (the
/// installer owns the routine's allocation and frees it). `has_adjustmembers`
/// is `amroutine->amadjustmembers != NULL`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexAmInfo {
    pub amstrategies: i32,
    pub amsupport: i32,
    pub amoptsprocnum: i32,
    pub amstorage: bool,
    pub amcanorder: bool,
    pub amcanhash: bool,
    pub amcanorderbyop: bool,
    pub has_adjustmembers: bool,
}

/// `typedef struct OpFamilyMember` (access/amapi.h). The transient in-memory
/// record describing one operator or support function while CREATE/ALTER
/// OPERATOR CLASS/FAMILY builds up its `pg_amop`/`pg_amproc` entries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpFamilyMember {
    /// is this an operator, or support func?
    pub is_func: bool,
    /// operator or support func's OID.
    pub object: Oid,
    /// strategy or support func number.
    pub number: i32,
    /// lefttype.
    pub lefttype: Oid,
    /// righttype.
    pub righttype: Oid,
    /// ordering operator's sort opfamily, or 0.
    pub sortfamily: Oid,
    /// hard or soft dependency?
    pub ref_is_hard: bool,
    /// is dependency on opclass or opfamily?
    pub ref_is_family: bool,
    /// OID of opclass or opfamily.
    pub refobjid: Oid,
}
