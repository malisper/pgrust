//! ABI for `backend/utils/adt/ruleutils.c` — the SQL deparser.
//!
//! Carries the file-local `repr(C)` structs that ruleutils.c defines at the top
//! of the file (the "Local data types" block): [`deparse_context`],
//! [`deparse_namespace`], [`deparse_columns`], [`NameHashEntry`], and the
//! [`rsv_callback`] function-pointer type used by `resolve_special_varno()`.
//!
//! These are private to ruleutils.c in PostgreSQL, but the Rust port splits the
//! file into several module files that all need to see the structs, so they live
//! here in `pgrust-pg-ffi`.  Reach them by path
//! (`pg_ffi_fgram::ruleutils::deparse_context`, etc.); this module is
//! deliberately **not** in the crate-root glob re-export, to avoid
//! ambiguous-glob collisions (it intentionally re-uses widely-named types such
//! as `List`/`Plan`/`Query`).
//!
//! Layouts mirror PostgreSQL 18.3 `src/backend/utils/adt/ruleutils.c` exactly
//! (field order, types, and the `NAMEDATALEN` hash-key length).

use core::ffi::{c_char, c_int};

use crate::bitmapset::Bitmapset;
use crate::fmgr::Node;
use crate::heaptuple::TupleDesc;
use crate::list::List;
use crate::nodeindexscan::Plan;
use crate::pathnodes::AppendRelInfo;
use crate::stringinfo::StringInfo;

/// `NAMEDATALEN` (PostgreSQL fixed name length, including NUL terminator).
pub const NAMEDATALEN: usize = 64;

/* ----------
 * Pretty formatting constants (ruleutils.c lines ~80-100)
 * ---------- */

/// `PRETTYINDENT_STD`.
pub const PRETTYINDENT_STD: c_int = 8;
/// `PRETTYINDENT_JOIN`.
pub const PRETTYINDENT_JOIN: c_int = 4;
/// `PRETTYINDENT_VAR`.
pub const PRETTYINDENT_VAR: c_int = 4;
/// `PRETTYINDENT_LIMIT` — wrap limit.
pub const PRETTYINDENT_LIMIT: c_int = 40;

/// `PRETTYFLAG_PAREN`.
pub const PRETTYFLAG_PAREN: c_int = 0x0001;
/// `PRETTYFLAG_INDENT`.
pub const PRETTYFLAG_INDENT: c_int = 0x0002;
/// `PRETTYFLAG_SCHEMA`.
pub const PRETTYFLAG_SCHEMA: c_int = 0x0004;

/// `WRAP_COLUMN_DEFAULT` — 0 means wrap always.
pub const WRAP_COLUMN_DEFAULT: c_int = 0;

/// Flags for `pg_get_indexdef_columns_extended()` (ruleutils.h).
pub const RULE_INDEXDEF_PRETTY: u16 = 0x01;
/// Flags for `pg_get_indexdef_columns_extended()` (ruleutils.h).
pub const RULE_INDEXDEF_KEYS_ONLY: u16 = 0x02;

/// Context info needed for invoking a recursive querytree display routine.
///
/// `typedef struct { ... } deparse_context;` (ruleutils.c).
#[repr(C)]
pub struct deparse_context {
    /// output buffer to append to
    pub buf: StringInfo,
    /// List of `deparse_namespace` nodes
    pub namespaces: *mut List,
    /// if top level of a view, the view's tupdesc
    pub resultDesc: TupleDesc,
    /// Current query level's SELECT targetlist
    pub targetList: *mut List,
    /// Current query level's WINDOW clause
    pub windowClause: *mut List,
    /// enabling of pretty-print functions
    pub prettyFlags: c_int,
    /// max line length, or -1 for no limit
    pub wrapColumn: c_int,
    /// current indent level for pretty-print
    pub indentLevel: c_int,
    /// true to print prefixes on Vars
    pub varprefix: bool,
    /// do we care about output column names?
    pub colNamesVisible: bool,
    /// deparsing GROUP BY clause?
    pub inGroupBy: bool,
    /// deparsing simple Var in ORDER BY?
    pub varInOrderBy: bool,
    /// if not null, map child Vars of these relids back to the parent rel
    pub appendparents: *mut Bitmapset,
}

/// Per-query-level Var namespace.
///
/// `typedef struct { ... } deparse_namespace;` (ruleutils.c).
#[repr(C)]
pub struct deparse_namespace {
    /// List of RangeTblEntry nodes
    pub rtable: *mut List,
    /// Parallel list of names for RTEs
    pub rtable_names: *mut List,
    /// Parallel list of `deparse_columns` structs
    pub rtable_columns: *mut List,
    /// List of Plan trees for SubPlans
    pub subplans: *mut List,
    /// List of CommonTableExpr nodes
    pub ctes: *mut List,
    /// Array of AppendRelInfo nodes, or NULL
    pub appendrels: *mut *mut AppendRelInfo,
    /// alias for OLD in RETURNING list
    pub ret_old_alias: *mut c_char,
    /// alias for NEW in RETURNING list
    pub ret_new_alias: *mut c_char,
    /// Are we making USING names globally unique
    pub unique_using: bool,
    /// List of assigned names for USING columns
    pub using_names: *mut List,
    /// immediate parent of current expression
    pub plan: *mut Plan,
    /// ancestors of plan
    pub ancestors: *mut List,
    /// outer subnode, or NULL if none
    pub outer_plan: *mut Plan,
    /// inner subnode, or NULL if none
    pub inner_plan: *mut Plan,
    /// referent for OUTER_VAR Vars
    pub outer_tlist: *mut List,
    /// referent for INNER_VAR Vars
    pub inner_tlist: *mut List,
    /// referent for INDEX_VAR Vars
    pub index_tlist: *mut List,
    /// special namespace: function name
    pub funcname: *mut c_char,
    /// special namespace: number of args
    pub numargs: c_int,
    /// special namespace: arg names
    pub argnames: *mut *mut c_char,
}

/// Per-relation data about column alias names.
///
/// `typedef struct { ... } deparse_columns;` (ruleutils.c).
#[repr(C)]
pub struct deparse_columns {
    /// length of `colnames[]` array
    pub num_cols: c_int,
    /// array of C strings and NULLs
    pub colnames: *mut *mut c_char,
    /// length of `new_colnames[]` array
    pub num_new_cols: c_int,
    /// array of C strings
    pub new_colnames: *mut *mut c_char,
    /// array of bool flags
    pub is_new_col: *mut bool,
    /// whether we should actually print a column alias list
    pub printaliases: bool,
    /// names assigned to parent merged columns
    pub parentUsing: *mut List,
    /// rangetable index of left child
    pub leftrti: c_int,
    /// rangetable index of right child
    pub rightrti: c_int,
    /// left-child varattnos of join cols, or 0
    pub leftattnos: *mut c_int,
    /// right-child varattnos of join cols, or 0
    pub rightattnos: *mut c_int,
    /// names assigned to merged columns
    pub usingNames: *mut List,
    /// hash table of strings (entries just strings), or NULL
    pub names_hash: *mut crate::dynahash::HTAB,
}

/// Entry in `set_rtable_names`' hash table.
///
/// `typedef struct { char name[NAMEDATALEN]; int counter; } NameHashEntry;`
#[repr(C)]
pub struct NameHashEntry {
    /// Hash key --- must be first
    pub name: [c_char; NAMEDATALEN],
    /// Largest addition used so far for name
    pub counter: c_int,
}

/// Callback signature for `resolve_special_varno()`.
///
/// `typedef void (*rsv_callback) (Node *node, deparse_context *context, void *callback_arg);`
pub type rsv_callback =
    fn(node: *mut Node, context: *mut deparse_context, callback_arg: *mut core::ffi::c_void);
