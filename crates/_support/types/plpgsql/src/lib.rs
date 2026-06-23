//! `types-plpgsql` — the PL/pgSQL type carriers.
//!
//! A faithful, owned-tree port of every `PLpgSQL_*` struct and enum defined in
//! `src/pl/plpgsql/src/plpgsql.h` (PostgreSQL 18.3). This is the foundation
//! crate of the PL/pgSQL subsystem port: pure type declarations, no behavior.
//! Every plpgsql owner crate (`pl_comp.c`, `pl_exec.c`, `pl_funcs.c`,
//! `pl_gram.y`, `pl_handler.c`, `pl_scanner.c`) consumes these definitions.
//!
//! ## Modeling conventions
//!
//! PL/pgSQL manages all of this in its own MemoryContexts, so we model it with
//! idiomatic owned values:
//!   * node links (`struct X *`) -> `Option<Box<X>>`
//!   * C lists (`List *`)         -> `Vec<T>` (element type noted per field)
//!   * C strings (`char *`)       -> `String` / `Option<String>`
//!   * tagged unions (`cmd_type` / `dtype` discriminator + a struct family) ->
//!     Rust enums whose variants carry the per-kind payload structs. Each
//!     payload struct retains its own `cmd_type`/`dtype` discriminator field
//!     field-for-field with C, both because the C code reads it directly off
//!     the struct and because the variant tag and the field must agree.
//!
//! ## Opacity (types.md rule 6 — inherited, never introduced)
//!
//! The real scalar/value types PL/pgSQL spells out cross as their real repo
//! types: `Oid` and `Datum` (from `types-core`/`types-datum`) and the
//! `RawParseMode` selector (`types-parsenodes`).
//!
//! The remaining cross-references are *foreign subsystem pointers* that
//! PL/pgSQL only ever holds and forwards across a seam — it never reaches into
//! their payload. Several of their owners are not yet ported (`funccache.c`'s
//! `CachedFunction`, `expandedrecord.c`'s `ExpandedRecordHeader`, the SPI
//! result tables, the executor `EState`/`ExprState`/`ExprContext`, the
//! plancache `CachedPlan`/`CachedPlanSource`, the typcache `TypeCacheEntry`,
//! the trigger `TriggerData`/`EventTriggerData`, `ErrorData`); others
//! (`Bitmapset`, `Expr`, `Param`, `TypeName`, `TupleDesc`) live in the
//! lifetime-parameterized `types-nodes` arena knot. To keep this foundation
//! crate buildable standalone and free of the `'mcx` node-arena dependency,
//! those pointers are modeled here as inherited-opacity handle newtypes — the
//! sanctioned `types-execparallel` / `types-ri-triggers` precedent for
//! foreign-owned objects a unit only marshals across seams. Each collapses
//! onto its real owner type when that owner lands.

#![no_std]
// `PLpgSQL_*` type and field names are kept verbatim from `plpgsql.h` (the C
// code reads these identifiers directly); allow the C-style casing.
#![allow(non_camel_case_types, non_snake_case)]
// The `PLpgSQL_plugin` vtable carries the C-ABI function-pointer slots
// field-for-field; `Datum`/`Oid` are not `repr(C)` here, so the improper-ctypes
// lint fires on the (correct) extern-fn slots — the plugin owner (`pl_exec.c`)
// is the only caller across this boundary.
#![allow(improper_ctypes_definitions)]
extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

pub use ::types_core::Oid;
pub use ::datum::Datum;
pub use ::parsenodes::RawParseMode;

// ---------------------------------------------------------------------------
// Fixed-width integer aliases (match plpgsql.h's use of c.h typedefs).
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
pub type int16 = i16;
#[allow(non_camel_case_types)]
pub type int32 = i32;
#[allow(non_camel_case_types)]
pub type uint64 = u64;
#[allow(non_camel_case_types)]
pub type Size = usize;

/// `LocalTransactionId` — a uint32 transaction-local id (`storage/lock.h`).
pub type LocalTransactionId = u32;

// ---------------------------------------------------------------------------
// Inherited-opacity handles for foreign subsystem pointers PL/pgSQL forwards
// across seams (see crate docs). These are NOT reimplementations; each is a
// handle whose payload lives in another (often not-yet-ported) crate and
// collapses onto that real type when the owner lands.
// ---------------------------------------------------------------------------

/// `SPIPlanPtr` (`struct _SPI_plan *`) — a prepared SPI plan handle. Opaque in
/// C as well (`executor/spi.h` keeps `_SPI_plan` private).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SPIPlanPtr(pub u64);

/// `Bitmapset *` — set of datum numbers referenced by a query
/// (`nodes/bitmapset.h`; real owner `types-nodes::bitmapset`, `'mcx`-arena).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bitmapset(pub u64);

/// Planner `Expr *` node — a simple-expr tree extracted from a plan
/// (`nodes/primnodes.h`; real owner `types-nodes::primnodes::Expr`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Expr(pub u64);

/// Executor `ExprState *` — a compiled evaluation tree for a simple expr
/// (`nodes/execnodes.h`; real owner `types-nodes::execexpr::ExprState<'mcx>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExprState(pub u64);

/// Planner `Param *` node (`nodes/primnodes.h`; real owner
/// `types-nodes::primnodes::Param`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Param(pub u64);

/// `CachedPlanSource *` (`utils/plancache.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CachedPlanSource(pub u64);

/// `CachedPlan *` (`utils/plancache.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CachedPlan(pub u64);

/// Parser `TypeName *` — a type name as written by the user
/// (`nodes/parsenodes.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeName(pub u64);

/// `TypeCacheEntry *` — typcache entry for a composite type
/// (`utils/typcache.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeCacheEntry(pub u64);

/// `TupleDesc` — pointer to a tuple descriptor (`access/tupdesc.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TupleDesc(pub u64);

/// `ExpandedRecordHeader *` (`utils/expandedrecord.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExpandedRecordHeader(pub u64);

/// `MemoryContext` handle — PL/pgSQL's per-function / per-stmt contexts
/// (`nodes/memnodes.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MemoryContext(pub u64);

/// `ParamListInfo` (`ParamListInfoData *`) (`nodes/params.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParamListInfo(pub u64);

/// `EState *` — executor state for simple-expression evaluation
/// (`nodes/execnodes.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EState(pub u64);

/// `ResourceOwner` (`utils/resowner.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResourceOwner(pub u64);

/// `HTAB *` — dynahash table (used for the cast hash) (`utils/hsearch.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HTAB(pub u64);

/// `ExprContext *` (`nodes/execnodes.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExprContext(pub u64);

/// `SPITupleTable *` (`executor/spi.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SPITupleTable(pub u64);

/// `Tuplestorestate *` (`utils/tuplestore.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tuplestorestate(pub u64);

/// `ReturnSetInfo *` (`nodes/execnodes.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReturnSetInfo(pub u64);

/// `ErrorData *` — a captured error (`utils/elog.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorData(pub u64);

/// `TriggerData *` — DML trigger firing info (`commands/trigger.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TriggerData(pub u64);

/// `EventTriggerData *` (`commands/event_trigger.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventTriggerData(pub u64);

/// `CachedFunction` — the `funccache.c`-managed header embedded by value at the
/// head of every `PLpgSQL_function` (`utils/funccache.h`). funccache.c is not
/// yet ported; held as an opaque handle until it lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CachedFunction(pub u64);

/// `ExpandedRecordFieldInfo` — a record field's attnum + cached type info
/// (`utils/expandedrecord.h`). A small by-value struct embedded in
/// `PLpgSQL_recfield`; its contents are filled by `expandedrecord.c` across the
/// seam, but it is spelled out by value in C, so it is carried by value here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExpandedRecordFieldInfo {
    pub fnumber: int32,
    pub ftypeid: Oid,
    pub ftypmod: int32,
    pub fcollation: Oid,
}

/// `FetchDirection` — fetch/move direction (`nodes/parsenodes.h`). Carried by
/// value in `PLpgSQL_stmt_fetch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FetchDirection {
    FETCH_FORWARD = 0,
    FETCH_BACKWARD = 1,
    FETCH_ABSOLUTE = 2,
    FETCH_RELATIVE = 3,
}

// ===========================================================================
// Enums (plpgsql.h "Definitions" section)
// ===========================================================================

/// Compiler's namespace item types (`PLpgSQL_nsitem_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_nsitem_type {
    PLPGSQL_NSTYPE_LABEL, // block label
    PLPGSQL_NSTYPE_VAR,   // scalar variable
    PLPGSQL_NSTYPE_REC,   // composite variable
}

/// A `PLPGSQL_NSTYPE_LABEL` stack entry's label kind (`PLpgSQL_label_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_label_type {
    PLPGSQL_LABEL_BLOCK, // DECLARE/BEGIN block
    PLPGSQL_LABEL_LOOP,  // looping construct
    PLPGSQL_LABEL_OTHER, // anything else
}

/// Datum array node types (`PLpgSQL_datum_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_datum_type {
    PLPGSQL_DTYPE_VAR,
    PLPGSQL_DTYPE_ROW,
    PLPGSQL_DTYPE_REC,
    PLPGSQL_DTYPE_RECFIELD,
    PLPGSQL_DTYPE_PROMISE,
}

/// `DTYPE_PROMISE` datums' ways of computing the promise (`PLpgSQL_promise_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_promise_type {
    PLPGSQL_PROMISE_NONE = 0, // not a promise, or promise satisfied
    PLPGSQL_PROMISE_TG_NAME,
    PLPGSQL_PROMISE_TG_WHEN,
    PLPGSQL_PROMISE_TG_LEVEL,
    PLPGSQL_PROMISE_TG_OP,
    PLPGSQL_PROMISE_TG_RELID,
    PLPGSQL_PROMISE_TG_TABLE_NAME,
    PLPGSQL_PROMISE_TG_TABLE_SCHEMA,
    PLPGSQL_PROMISE_TG_NARGS,
    PLPGSQL_PROMISE_TG_ARGV,
    PLPGSQL_PROMISE_TG_EVENT,
    PLPGSQL_PROMISE_TG_TAG,
}

/// Variants distinguished in `PLpgSQL_type` structs (`PLpgSQL_type_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_type_type {
    PLPGSQL_TTYPE_SCALAR, // scalar types and domains
    PLPGSQL_TTYPE_REC,    // composite types, including RECORD
    PLPGSQL_TTYPE_PSEUDO, // pseudotypes
}

/// Execution tree node types (`PLpgSQL_stmt_type`) — the `cmd_type` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_stmt_type {
    PLPGSQL_STMT_BLOCK,
    PLPGSQL_STMT_ASSIGN,
    PLPGSQL_STMT_IF,
    PLPGSQL_STMT_CASE,
    PLPGSQL_STMT_LOOP,
    PLPGSQL_STMT_WHILE,
    PLPGSQL_STMT_FORI,
    PLPGSQL_STMT_FORS,
    PLPGSQL_STMT_FORC,
    PLPGSQL_STMT_FOREACH_A,
    PLPGSQL_STMT_EXIT,
    PLPGSQL_STMT_RETURN,
    PLPGSQL_STMT_RETURN_NEXT,
    PLPGSQL_STMT_RETURN_QUERY,
    PLPGSQL_STMT_RAISE,
    PLPGSQL_STMT_ASSERT,
    PLPGSQL_STMT_EXECSQL,
    PLPGSQL_STMT_DYNEXECUTE,
    PLPGSQL_STMT_DYNFORS,
    PLPGSQL_STMT_GETDIAG,
    PLPGSQL_STMT_OPEN,
    PLPGSQL_STMT_FETCH,
    PLPGSQL_STMT_CLOSE,
    PLPGSQL_STMT_PERFORM,
    PLPGSQL_STMT_CALL,
    PLPGSQL_STMT_COMMIT,
    PLPGSQL_STMT_ROLLBACK,
}

/// Execution node return codes (anonymous enum in C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_rc {
    PLPGSQL_RC_OK,
    PLPGSQL_RC_EXIT,
    PLPGSQL_RC_RETURN,
    PLPGSQL_RC_CONTINUE,
}

/// GET DIAGNOSTICS information items (`PLpgSQL_getdiag_kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_getdiag_kind {
    PLPGSQL_GETDIAG_ROW_COUNT,
    PLPGSQL_GETDIAG_ROUTINE_OID,
    PLPGSQL_GETDIAG_CONTEXT,
    PLPGSQL_GETDIAG_ERROR_CONTEXT,
    PLPGSQL_GETDIAG_ERROR_DETAIL,
    PLPGSQL_GETDIAG_ERROR_HINT,
    PLPGSQL_GETDIAG_RETURNED_SQLSTATE,
    PLPGSQL_GETDIAG_COLUMN_NAME,
    PLPGSQL_GETDIAG_CONSTRAINT_NAME,
    PLPGSQL_GETDIAG_DATATYPE_NAME,
    PLPGSQL_GETDIAG_MESSAGE_TEXT,
    PLPGSQL_GETDIAG_TABLE_NAME,
    PLPGSQL_GETDIAG_SCHEMA_NAME,
}

/// RAISE statement options (`PLpgSQL_raise_option_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_raise_option_type {
    PLPGSQL_RAISEOPTION_ERRCODE,
    PLPGSQL_RAISEOPTION_MESSAGE,
    PLPGSQL_RAISEOPTION_DETAIL,
    PLPGSQL_RAISEOPTION_HINT,
    PLPGSQL_RAISEOPTION_COLUMN,
    PLPGSQL_RAISEOPTION_CONSTRAINT,
    PLPGSQL_RAISEOPTION_DATATYPE,
    PLPGSQL_RAISEOPTION_TABLE,
    PLPGSQL_RAISEOPTION_SCHEMA,
}

/// Behavioral modes for plpgsql variable resolution (`PLpgSQL_resolve_option`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_resolve_option {
    PLPGSQL_RESOLVE_ERROR,    // throw error if ambiguous
    PLPGSQL_RESOLVE_VARIABLE, // prefer plpgsql var to table column
    PLPGSQL_RESOLVE_COLUMN,   // prefer table column to plpgsql var
}

/// Status of optimization of assignment to a r/w expanded object (`PLpgSQL_rwopt`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_rwopt {
    PLPGSQL_RWOPT_UNKNOWN = 0, // applicability not determined yet
    PLPGSQL_RWOPT_NOPE,        // cannot do any optimization
    PLPGSQL_RWOPT_TRANSFER,    // transfer the old value into expr state
    PLPGSQL_RWOPT_INPLACE,     // pass value as R/W to top-level function
}

/// Trigger type (`PLpgSQL_trigtype`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PLpgSQL_trigtype {
    PLPGSQL_DML_TRIGGER,
    PLPGSQL_EVENT_TRIGGER,
    PLPGSQL_NOT_TRIGGER,
}

/// `IdentifierLookup` — scanner identifier-lookup mode (a per-backend global
/// enum in C, modeled as thread-local state by the scanner owner).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum IdentifierLookup {
    IDENTIFIER_LOOKUP_NORMAL,  // normal processing of var names
    IDENTIFIER_LOOKUP_DECLARE, // In DECLARE --- don't look up names
    IDENTIFIER_LOOKUP_EXPR,    // In SQL expression --- special case
}

/// This value mustn't match any possible output of `MAKE_SQLSTATE()`.
pub const PLPGSQL_OTHERS: int32 = -1;

// Extra compile-time and run-time check bitflags.
pub const PLPGSQL_XCHECK_NONE: int32 = 0;
pub const PLPGSQL_XCHECK_SHADOWVAR: int32 = 1 << 1;
pub const PLPGSQL_XCHECK_TOOMANYROWS: int32 = 1 << 2;
pub const PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT: int32 = 1 << 3;
pub const PLPGSQL_XCHECK_ALL: int32 = !0;

/// `FUNC_MAX_ARGS` — max function arguments (PostgreSQL default is 100). Sizes
/// `PLpgSQL_function.fn_argvarnos` (`pg_config_manual.h`).
pub const FUNC_MAX_ARGS: usize = 100;

// ===========================================================================
// Node and structure definitions (plpgsql.h)
// ===========================================================================

/// Postgres data type (`PLpgSQL_type`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_type {
    pub typname: String,          // (simple) name of the type
    pub typoid: Oid,              // OID of the data type
    pub ttype: PLpgSQL_type_type, // PLPGSQL_TTYPE_ code
    pub typlen: int16,            // stuff copied from its pg_type entry
    pub typbyval: bool,
    pub typtype: u8,      // C `char`
    pub collation: Oid,   // from pg_type, but can be overridden
    pub typisarray: bool, // is "true" array, or domain over one
    pub atttypmod: int32, // typmod (taken from someplace else)
    // Remaining fields used only for named composite types (not RECORD)
    pub origtypname: Option<TypeName>, // type name as written by user
    pub tcache: Option<TypeCacheEntry>, // typcache entry for composite type
    pub tupdesc_id: uint64,            // last-seen tupdesc identifier
}

/// SQL query to plan and execute (`PLpgSQL_expr`).
///
/// `func` and `ns` are back/cross links into the owning function and its
/// namespace chain; `func` is modeled as an opaque back-reference (the function
/// owns the expr tree) to avoid an ownership cycle.
#[derive(Debug, Clone)]
pub struct PLpgSQL_expr {
    pub query: String,           // query string, verbatim from function body
    pub parseMode: RawParseMode, // raw_parser() mode to use
    /// `struct PLpgSQL_function *func` — function containing this expr.
    pub func: Option<u64>,
    /// `struct PLpgSQL_nsitem *ns` — namespace chain visible to this expr.
    pub ns: Option<Box<PLpgSQL_nsitem>>,

    // Set during plpgsql parsing:
    pub target_param: int32,   // dno of assign target, or -1 if none
    pub target_is_local: bool, // is it within nearest exception block?

    // Left as zeroes/NULLs until first parse/plan of the query:
    pub plan: Option<SPIPlanPtr>, // plan, or None if not made yet
    pub paramnos: Option<Bitmapset>, // all dnos referenced by this query

    // "simple expression" fast-path execution:
    pub expr_simple_expr: Option<Expr>, // None means not a simple expr
    pub expr_simple_type: Oid,          // result type Oid, if simple
    pub expr_simple_typmod: int32,      // result typmod, if simple
    pub expr_simple_mutable: bool,      // true if simple expr is mutable

    // R/W expanded-object assignment optimization:
    pub expr_rwopt: PLpgSQL_rwopt,    // can we apply R/W optimization?
    pub expr_rw_param: Option<Param>, // read/write Param within expr, if any

    // Remembered CachedPlanSource / CachedPlan if ever determined simple:
    pub expr_simple_plansource: Option<CachedPlanSource>, // from "plan"
    pub expr_simple_plan: Option<CachedPlan>,             // from "plan"
    pub expr_simple_plan_lxid: LocalTransactionId,

    // Valid only if simple AND prepared in current transaction:
    pub expr_simple_state: Option<ExprState>, // eval tree for expr_simple_expr
    pub expr_simple_in_use: bool,             // true if eval tree is active
    pub expr_simple_lxid: LocalTransactionId,
}

/// Generic datum array item (`PLpgSQL_datum`) — the tagged-union supertype of
/// `PLpgSQL_var`, `PLpgSQL_row`, `PLpgSQL_rec`, and `PLpgSQL_recfield`.
///
/// Note: `DTYPE_VAR` and `DTYPE_PROMISE` both use [`PLpgSQL_var`] (the variant
/// is distinguished at runtime by the `promise` field), matching the C code.
#[derive(Debug, Clone)]
pub enum PLpgSQL_datum {
    Var(Box<PLpgSQL_var>),           // PLPGSQL_DTYPE_VAR / PLPGSQL_DTYPE_PROMISE
    Row(Box<PLpgSQL_row>),           // PLPGSQL_DTYPE_ROW
    Rec(Box<PLpgSQL_rec>),           // PLPGSQL_DTYPE_REC
    Recfield(Box<PLpgSQL_recfield>), // PLPGSQL_DTYPE_RECFIELD
}

/// Common `PLpgSQL_variable` header fields shared (by C struct-prefix layout)
/// by `PLpgSQL_var`, `PLpgSQL_row`, and `PLpgSQL_rec`.
///
/// In C, the loop/INTO/CALL code passes around a `PLpgSQL_variable *` that is
/// really one of the three; we keep this as a distinct struct (matching the
/// "end of PLpgSQL_variable fields" comments) for the fields those code paths
/// read in common, and inline the same fields into each variant struct.
#[derive(Debug, Clone)]
pub struct PLpgSQL_variable {
    pub dtype: PLpgSQL_datum_type,
    pub dno: int32,
    pub refname: String,
    pub lineno: int32,
    pub isconst: bool,
    pub notnull: bool,
    pub default_val: Option<Box<PLpgSQL_expr>>,
}

/// Scalar variable (`PLpgSQL_var`). Used by both `DTYPE_VAR` and
/// `DTYPE_PROMISE` (see `promise`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_var {
    // PLpgSQL_variable fields:
    pub dtype: PLpgSQL_datum_type,
    pub dno: int32,
    pub refname: String,
    pub lineno: int32,
    pub isconst: bool,
    pub notnull: bool,
    pub default_val: Option<Box<PLpgSQL_expr>>,
    // end of PLpgSQL_variable fields
    pub datatype: Option<Box<PLpgSQL_type>>,

    // CURSOR FOR <query> extra properties:
    pub cursor_explicit_expr: Option<Box<PLpgSQL_expr>>,
    pub cursor_explicit_argrow: int32,
    pub cursor_options: int32,

    // Fields below here can change at runtime:
    pub value: Datum,
    pub isnull: bool,
    pub freeval: bool,

    /// Out-of-band companion to `value` for a pass-by-reference scalar
    /// (`text`/`varchar`/`numeric`/…): its verbatim header-ful varlena / cstring
    /// byte image. The bare-word `Datum` (`struct Datum(usize)`) cannot carry a
    /// by-reference payload, so the image rides alongside (mirroring the
    /// execstate's `retval_byref`/`last_eval_byref` companions); it is `Some`
    /// exactly when `value` holds a by-reference datum, `None` for a by-value
    /// or NULL variable. `build_datum_snapshot` reads it into the param-bind so
    /// the image survives into expression evaluation.
    pub value_byref: Option<Vec<u8>>,

    /// Which "promised" value to assign if the promise must be honored.
    pub promise: PLpgSQL_promise_type,
}

/// Row variable (`PLpgSQL_row`) — one or more variables in an INTO clause,
/// FOR-loop targetlist, cursor argument list, etc.
#[derive(Debug, Clone)]
pub struct PLpgSQL_row {
    // PLpgSQL_variable fields:
    pub dtype: PLpgSQL_datum_type,
    pub dno: int32,
    pub refname: String,
    pub lineno: int32,
    pub isconst: bool,
    pub notnull: bool,
    pub default_val: Option<Box<PLpgSQL_expr>>,
    // end of PLpgSQL_variable fields
    /// Set up only if we might need to convert the row into a composite datum
    /// (currently only for OUT parameters); otherwise None.
    pub rowtupdesc: Option<TupleDesc>,

    pub nfields: int32,
    pub fieldnames: Vec<String>, // char **fieldnames
    pub varnos: Vec<int32>,      // int *varnos
}

/// Record variable (`PLpgSQL_rec`) — any composite type, including RECORD.
#[derive(Debug, Clone)]
pub struct PLpgSQL_rec {
    // PLpgSQL_variable fields:
    pub dtype: PLpgSQL_datum_type,
    pub dno: int32,
    pub refname: String,
    pub lineno: int32,
    pub isconst: bool,
    pub notnull: bool,
    pub default_val: Option<Box<PLpgSQL_expr>>,
    // end of PLpgSQL_variable fields
    pub datatype: Option<Box<PLpgSQL_type>>, // None if rectypeid is RECORDOID
    pub rectypeid: Oid,                      // declared type of variable
    pub firstfield: int32,                   // dno of first RECFIELD, or -1 if none

    // Fields below here can change at runtime:
    pub erh: Option<ExpandedRecordHeader>, // stored as an "expanded" record
}

/// Field in record (`PLpgSQL_recfield`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_recfield {
    // PLpgSQL_datum fields:
    pub dtype: PLpgSQL_datum_type,
    pub dno: int32,
    // end of PLpgSQL_datum fields
    pub fieldname: String,              // name of field
    pub recparentno: int32,             // dno of parent record
    pub nextfield: int32,               // dno of next child, or -1 if none
    pub rectupledescid: uint64,         // record's tupledesc ID as of last lookup
    pub finfo: ExpandedRecordFieldInfo, // field's attnum and type info
}

/// Item in the compiler's namespace tree (`PLpgSQL_nsitem`).
///
/// The C struct ends in a flexible-array `char name[]`; we store it as an owned
/// `String`. `prev` is the parent link in the namespace chain.
#[derive(Debug, Clone)]
pub struct PLpgSQL_nsitem {
    pub itemtype: PLpgSQL_nsitem_type,
    /// For labels, `itemno` is a [`PLpgSQL_label_type`] value; for other
    /// itemtypes it is the associated datum's dno.
    pub itemno: int32,
    pub prev: Option<Box<PLpgSQL_nsitem>>,
    pub name: String, // nul-terminated string (C flexible array member)
}

/// Generic execution node (`PLpgSQL_stmt`) — the tagged-union supertype.
///
/// Each variant carries the corresponding `PLpgSQL_stmt_*` payload struct; the
/// `cmd_type` discriminator in C maps onto the enum variant.
#[derive(Debug, Clone)]
pub enum PLpgSQL_stmt {
    Block(Box<PLpgSQL_stmt_block>),              // PLPGSQL_STMT_BLOCK
    Assign(Box<PLpgSQL_stmt_assign>),            // PLPGSQL_STMT_ASSIGN
    If(Box<PLpgSQL_stmt_if>),                    // PLPGSQL_STMT_IF
    Case(Box<PLpgSQL_stmt_case>),                // PLPGSQL_STMT_CASE
    Loop(Box<PLpgSQL_stmt_loop>),                // PLPGSQL_STMT_LOOP
    While(Box<PLpgSQL_stmt_while>),              // PLPGSQL_STMT_WHILE
    Fori(Box<PLpgSQL_stmt_fori>),                // PLPGSQL_STMT_FORI
    Fors(Box<PLpgSQL_stmt_fors>),                // PLPGSQL_STMT_FORS
    Forc(Box<PLpgSQL_stmt_forc>),                // PLPGSQL_STMT_FORC
    ForeachA(Box<PLpgSQL_stmt_foreach_a>),       // PLPGSQL_STMT_FOREACH_A
    Exit(Box<PLpgSQL_stmt_exit>),                // PLPGSQL_STMT_EXIT
    Return(Box<PLpgSQL_stmt_return>),            // PLPGSQL_STMT_RETURN
    ReturnNext(Box<PLpgSQL_stmt_return_next>),   // PLPGSQL_STMT_RETURN_NEXT
    ReturnQuery(Box<PLpgSQL_stmt_return_query>), // PLPGSQL_STMT_RETURN_QUERY
    Raise(Box<PLpgSQL_stmt_raise>),              // PLPGSQL_STMT_RAISE
    Assert(Box<PLpgSQL_stmt_assert>),            // PLPGSQL_STMT_ASSERT
    Execsql(Box<PLpgSQL_stmt_execsql>),          // PLPGSQL_STMT_EXECSQL
    Dynexecute(Box<PLpgSQL_stmt_dynexecute>),    // PLPGSQL_STMT_DYNEXECUTE
    Dynfors(Box<PLpgSQL_stmt_dynfors>),          // PLPGSQL_STMT_DYNFORS
    Getdiag(Box<PLpgSQL_stmt_getdiag>),          // PLPGSQL_STMT_GETDIAG
    Open(Box<PLpgSQL_stmt_open>),                // PLPGSQL_STMT_OPEN
    Fetch(Box<PLpgSQL_stmt_fetch>),              // PLPGSQL_STMT_FETCH
    Close(Box<PLpgSQL_stmt_close>),              // PLPGSQL_STMT_CLOSE
    Perform(Box<PLpgSQL_stmt_perform>),          // PLPGSQL_STMT_PERFORM
    Call(Box<PLpgSQL_stmt_call>),                // PLPGSQL_STMT_CALL
    Commit(Box<PLpgSQL_stmt_commit>),            // PLPGSQL_STMT_COMMIT
    Rollback(Box<PLpgSQL_stmt_rollback>),        // PLPGSQL_STMT_ROLLBACK
}

/// One EXCEPTION condition name (`PLpgSQL_condition`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_condition {
    pub sqlerrstate: int32, // SQLSTATE code, or PLPGSQL_OTHERS
    pub condname: String,   // condition name (for debugging)
    pub next: Option<Box<PLpgSQL_condition>>,
}

/// EXCEPTION block (`PLpgSQL_exception_block`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_exception_block {
    pub sqlstate_varno: int32,
    pub sqlerrm_varno: int32,
    pub exc_list: Vec<PLpgSQL_exception>, // List of WHEN clauses
}

/// One EXCEPTION ... WHEN clause (`PLpgSQL_exception`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_exception {
    pub lineno: int32,
    pub conditions: Option<Box<PLpgSQL_condition>>,
    pub action: Vec<PLpgSQL_stmt>, // List of statements
}

/// Block of statements (`PLpgSQL_stmt_block`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_block {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub body: Vec<PLpgSQL_stmt>, // List of statements
    pub n_initvars: int32,       // Length of initvarnos[]
    pub initvarnos: Vec<int32>,  // dnos of variables declared in this block
    pub exceptions: Option<Box<PLpgSQL_exception_block>>,
}

/// Assign statement (`PLpgSQL_stmt_assign`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_assign {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub varno: int32,
    pub expr: Option<Box<PLpgSQL_expr>>,
}

/// PERFORM statement (`PLpgSQL_stmt_perform`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_perform {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub expr: Option<Box<PLpgSQL_expr>>,
}

/// CALL statement (`PLpgSQL_stmt_call`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_call {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub expr: Option<Box<PLpgSQL_expr>>,
    pub is_call: bool,
    pub target: Option<Box<PLpgSQL_variable>>,
}

/// COMMIT statement (`PLpgSQL_stmt_commit`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_commit {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub chain: bool,
}

/// ROLLBACK statement (`PLpgSQL_stmt_rollback`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_rollback {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub chain: bool,
}

/// GET DIAGNOSTICS item (`PLpgSQL_diag_item`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_diag_item {
    pub kind: PLpgSQL_getdiag_kind, // id for diagnostic value desired
    pub target: int32,              // where to assign it
}

/// GET DIAGNOSTICS statement (`PLpgSQL_stmt_getdiag`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_getdiag {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub is_stacked: bool,                   // STACKED or CURRENT diagnostics area?
    pub diag_items: Vec<PLpgSQL_diag_item>, // List of PLpgSQL_diag_item
}

/// IF statement (`PLpgSQL_stmt_if`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_if {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub cond: Option<Box<PLpgSQL_expr>>,   // boolean expression for THEN
    pub then_body: Vec<PLpgSQL_stmt>,      // List of statements
    pub elsif_list: Vec<PLpgSQL_if_elsif>, // List of PLpgSQL_if_elsif structs
    pub else_body: Vec<PLpgSQL_stmt>,      // List of statements
}

/// One ELSIF arm of an IF statement (`PLpgSQL_if_elsif`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_if_elsif {
    pub lineno: int32,
    pub cond: Option<Box<PLpgSQL_expr>>, // boolean expression for this case
    pub stmts: Vec<PLpgSQL_stmt>,        // List of statements
}

/// CASE statement (`PLpgSQL_stmt_case`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_case {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub t_expr: Option<Box<PLpgSQL_expr>>, // test expression, or None if none
    pub t_varno: int32,                    // var to store test expr value into
    pub case_when_list: Vec<PLpgSQL_case_when>, // List of PLpgSQL_case_when structs
    pub have_else: bool,                   // needed because list could be empty
    pub else_stmts: Vec<PLpgSQL_stmt>,     // List of statements
}

/// One arm of a CASE statement (`PLpgSQL_case_when`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_case_when {
    pub lineno: int32,
    pub expr: Option<Box<PLpgSQL_expr>>, // boolean expression for this case
    pub stmts: Vec<PLpgSQL_stmt>,        // List of statements
}

/// Unconditional LOOP statement (`PLpgSQL_stmt_loop`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_loop {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub body: Vec<PLpgSQL_stmt>, // List of statements
}

/// WHILE cond LOOP statement (`PLpgSQL_stmt_while`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_while {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub cond: Option<Box<PLpgSQL_expr>>,
    pub body: Vec<PLpgSQL_stmt>, // List of statements
}

/// FOR statement with integer loopvar (`PLpgSQL_stmt_fori`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_fori {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<Box<PLpgSQL_var>>,
    pub lower: Option<Box<PLpgSQL_expr>>,
    pub upper: Option<Box<PLpgSQL_expr>>,
    pub step: Option<Box<PLpgSQL_expr>>, // None means default (ie, BY 1)
    pub reverse: int32,
    pub body: Vec<PLpgSQL_stmt>, // List of statements
}

/// FOR statement running over a SQL query (`PLpgSQL_stmt_forq`) — common
/// supertype of `fors`, `forc`, and `dynfors`. The C code passes around a
/// `PLpgSQL_stmt_forq *` to drive the shared loop machinery; we keep this as a
/// distinct header struct (matching the "must match PLpgSQL_stmt_forq"
/// comments) and inline the same prefix fields into each variant struct.
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_forq {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<Box<PLpgSQL_variable>>, // Loop variable (record or row)
    pub body: Vec<PLpgSQL_stmt>,            // List of statements
}

/// FOR statement running over SELECT (`PLpgSQL_stmt_fors`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_fors {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<Box<PLpgSQL_variable>>, // Loop variable (record or row)
    pub body: Vec<PLpgSQL_stmt>,            // List of statements
    // end of fields that must match PLpgSQL_stmt_forq
    pub query: Option<Box<PLpgSQL_expr>>,
}

/// FOR statement running over a cursor (`PLpgSQL_stmt_forc`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_forc {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<Box<PLpgSQL_variable>>, // Loop variable (record or row)
    pub body: Vec<PLpgSQL_stmt>,            // List of statements
    // end of fields that must match PLpgSQL_stmt_forq
    pub curvar: int32,
    pub argquery: Option<Box<PLpgSQL_expr>>, // cursor arguments if any
}

/// FOR statement running over EXECUTE (`PLpgSQL_stmt_dynfors`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_dynfors {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<Box<PLpgSQL_variable>>, // Loop variable (record or row)
    pub body: Vec<PLpgSQL_stmt>,            // List of statements
    // end of fields that must match PLpgSQL_stmt_forq
    pub query: Option<Box<PLpgSQL_expr>>,
    pub params: Vec<PLpgSQL_expr>, // USING expressions
}

/// FOREACH item in array loop (`PLpgSQL_stmt_foreach_a`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_foreach_a {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub varno: int32,                    // loop target variable
    pub slice: int32,                    // slice dimension, or 0
    pub expr: Option<Box<PLpgSQL_expr>>, // array expression
    pub body: Vec<PLpgSQL_stmt>,         // List of statements
}

/// OPEN a curvar (`PLpgSQL_stmt_open`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_open {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub curvar: int32,
    pub cursor_options: int32,
    pub argquery: Option<Box<PLpgSQL_expr>>,
    pub query: Option<Box<PLpgSQL_expr>>,
    pub dynquery: Option<Box<PLpgSQL_expr>>,
    pub params: Vec<PLpgSQL_expr>, // USING expressions
}

/// FETCH or MOVE statement (`PLpgSQL_stmt_fetch`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_fetch {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub target: Option<Box<PLpgSQL_variable>>, // target (record or row)
    pub curvar: int32,                         // cursor variable to fetch from
    pub direction: FetchDirection,             // fetch direction
    pub how_many: i64,                         // count, if constant (expr is None); C `long`
    pub expr: Option<Box<PLpgSQL_expr>>,       // count, if expression
    pub is_move: bool,                         // is this a fetch or move?
    pub returns_multiple_rows: bool,           // can return more than one row?
}

/// CLOSE curvar (`PLpgSQL_stmt_close`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_close {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub curvar: int32,
}

/// EXIT or CONTINUE statement (`PLpgSQL_stmt_exit`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_exit {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub is_exit: bool,         // Is this an exit or a continue?
    pub label: Option<String>, // None if it's an unlabeled EXIT/CONTINUE
    pub cond: Option<Box<PLpgSQL_expr>>,
}

/// RETURN statement (`PLpgSQL_stmt_return`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_return {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub expr: Option<Box<PLpgSQL_expr>>,
    pub retvarno: int32,
}

/// RETURN NEXT statement (`PLpgSQL_stmt_return_next`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_return_next {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub expr: Option<Box<PLpgSQL_expr>>,
    pub retvarno: int32,
}

/// RETURN QUERY statement (`PLpgSQL_stmt_return_query`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_return_query {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub query: Option<Box<PLpgSQL_expr>>,    // if static query
    pub dynquery: Option<Box<PLpgSQL_expr>>, // if dynamic query (RETURN QUERY EXECUTE)
    pub params: Vec<PLpgSQL_expr>,           // USING arguments for dynamic query
}

/// RAISE statement (`PLpgSQL_stmt_raise`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_raise {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub elog_level: int32,
    pub condname: Option<String>, // condition name, SQLSTATE, or None
    pub message: Option<String>,  // old-style message format literal, or None
    pub params: Vec<PLpgSQL_expr>, // list of exprs for old-style message
    pub options: Vec<PLpgSQL_raise_option>, // list of PLpgSQL_raise_option
}

/// RAISE statement option (`PLpgSQL_raise_option`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_raise_option {
    pub opt_type: PLpgSQL_raise_option_type,
    pub expr: Option<Box<PLpgSQL_expr>>,
}

/// ASSERT statement (`PLpgSQL_stmt_assert`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_assert {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub cond: Option<Box<PLpgSQL_expr>>,
    pub message: Option<Box<PLpgSQL_expr>>,
}

/// Generic SQL statement to execute (`PLpgSQL_stmt_execsql`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_execsql {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub sqlstmt: Option<Box<PLpgSQL_expr>>,
    pub mod_stmt: bool,     // is the stmt INSERT/UPDATE/DELETE/MERGE?
    pub mod_stmt_set: bool, // is mod_stmt valid yet?
    pub into: bool,         // INTO supplied?
    pub strict: bool,       // INTO STRICT flag
    pub target: Option<Box<PLpgSQL_variable>>, // INTO target (record or row)
}

/// Dynamic SQL string to execute (`PLpgSQL_stmt_dynexecute`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_stmt_dynexecute {
    pub cmd_type: PLpgSQL_stmt_type,
    pub lineno: int32,
    pub stmtid: u32,
    pub query: Option<Box<PLpgSQL_expr>>, // string expression
    pub into: bool,                       // INTO supplied?
    pub strict: bool,                     // INTO STRICT flag
    pub target: Option<Box<PLpgSQL_variable>>, // INTO target (record or row)
    pub params: Vec<PLpgSQL_expr>,        // USING expressions
}

/// Complete compiled function (`PLpgSQL_function`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_function {
    pub cfunc: CachedFunction, // fields managed by funccache.c

    pub fn_signature: String,
    pub fn_oid: Oid,
    pub fn_is_trigger: PLpgSQL_trigtype,
    pub fn_input_collation: Oid,
    pub fn_cxt: Option<MemoryContext>,

    pub fn_rettype: Oid,
    pub fn_rettyplen: int32,
    pub fn_retbyval: bool,
    pub fn_retistuple: bool,
    pub fn_retisdomain: bool,
    pub fn_retset: bool,
    pub fn_readonly: bool,
    pub fn_prokind: u8, // C `char`

    pub fn_nargs: int32,
    pub fn_argvarnos: [int32; FUNC_MAX_ARGS],
    pub out_param_varno: int32,
    pub found_varno: int32,
    pub new_varno: int32,
    pub old_varno: int32,

    pub resolve_option: PLpgSQL_resolve_option,

    pub print_strict_params: bool,

    // extra checks
    pub extra_warnings: int32,
    pub extra_errors: int32,

    // the datums representing the function's local variables
    pub ndatums: int32,
    pub datums: Vec<PLpgSQL_datum>, // PLpgSQL_datum **datums
    pub copiable_size: Size,        // space for locally instantiated datums

    // function body parsetree
    pub action: Option<Box<PLpgSQL_stmt_block>>,

    // data derived while parsing body
    pub nstatements: u32,                  // counter for assigning stmtids
    pub requires_procedure_resowner: bool, // contains CALL or DO?
    pub has_exception_block: bool,         // contains BEGIN...EXCEPTION?

    /// this field changes when the function is used (`cur_estate`). Held as an
    /// opaque back-reference to the active execstate.
    pub cur_estate: Option<u64>,
}

/// The error-context data the `plpgsql_exec_error_callback` reads off the
/// currently-executing statement (`estate->err_stmt`): its source line and the
/// `plpgsql_stmt_typename` of its command type.
#[derive(Debug, Clone, Copy)]
pub struct ErrStmtMark {
    pub lineno: int32,
    pub typename: &'static str,
}

/// Runtime execution data (`PLpgSQL_execstate`).
#[derive(Debug, Clone)]
pub struct PLpgSQL_execstate {
    /// function being executed (`func`) — opaque back-reference.
    pub func: Option<u64>,
    /// the executing function's printable signature (`func->fn_signature`),
    /// carried directly so the error-context line can be built without
    /// dereferencing the opaque `func` back-reference.
    pub fn_signature: String,
    /// `func->fn_oid` — the executing function's OID, carried directly (like
    /// `fn_signature`) so `GET DIAGNOSTICS ... = PG_ROUTINE_OID` can read it
    /// without dereferencing the opaque `func` back-reference. An inline
    /// (anonymous DO) block has `InvalidOid` here, matching C.
    pub fn_oid: Oid,

    pub trigdata: Option<TriggerData>,        // if regular trigger
    pub evtrigdata: Option<EventTriggerData>, // if event trigger

    pub retval: Datum,
    pub retisnull: bool,
    pub rettype: Oid, // type of current retval

    /// `Some(image)` when the current `retval` is a pass-by-reference value: the
    /// verbatim header-ful varlena / cstring byte image (`datumCopy`'d out of the
    /// SPI/eval arena), carried alongside the bare-word `retval` so a by-ref
    /// result (text/varchar/numeric/…) survives to the function's result context.
    /// The bare-word `retval` is `0` in that case. `None` for a by-value/NULL
    /// result. Mirrors C's `estate->retval` being a live by-ref pointer.
    pub retval_byref: Option<Vec<u8>>,
    /// Out-of-band by-ref image of the most recent `exec_eval_expr` result (the
    /// companion to the `(value, isnull, rettype, rettypmod)` tuple the evaluator
    /// returns): `Some(image)` for a by-ref expression result, taken by the
    /// caller (`exec_stmt_return` into `retval_byref`). `None` for by-value.
    pub last_eval_byref: Option<Vec<u8>>,

    pub fn_rettype: Oid, // info about declared function rettype
    pub retistuple: bool,
    pub retisset: bool,

    /// `func->fn_input_collation` — the function's input collation, carried
    /// directly so paths that rebuild a datatype (`exec_stmt_case`'s simple-CASE
    /// temp var) can pass it to `plpgsql_build_datatype` without dereferencing
    /// the opaque `func` back-reference.
    pub fn_input_collation: Oid,

    pub readonly_func: bool,
    pub atomic: bool,

    /// `func->extra_warnings` — the `plpgsql.extra_warnings` bitmask the function
    /// was compiled with, carried directly so the executor's extra-check sites
    /// (too-many-rows, strict-multi-assignment) can read the active level without
    /// dereferencing the opaque `func` back-reference.
    pub extra_warnings: int32,
    /// `func->extra_errors` — the `plpgsql.extra_errors` bitmask (companion to
    /// [`Self::extra_warnings`]).
    pub extra_errors: int32,
    /// `func->print_strict_params` — whether STRICT INTO failures append the
    /// `parameters: …` DETAIL.
    pub print_strict_params: bool,

    /// `func->resolve_option` — the function's `#variable_conflict` mode,
    /// carried directly so the expression parser hooks (`plpgsql_parser_setup`
    /// pre/post columnref split) can select variable-vs-column precedence
    /// without dereferencing the opaque `func` back-reference.
    pub resolve_option: PLpgSQL_resolve_option,

    /// the "target" label of the current EXIT/CONTINUE stmt, if any.
    pub exitlabel: Option<String>,
    // `estate->cur_error` — the live error being handled by the current
    // EXCEPTION handler (read by GET STACKED DIAGNOSTICS and RAISE-without-
    // parameters). C carries an `ErrorData *`; the owned model carries the
    // captured `PgError` value directly.
    pub cur_error: Option<types_error::PgError>,

    pub tuple_store: Option<Tuplestorestate>, // SRFs accumulate results here
    pub tuple_store_desc: Option<TupleDesc>,  // descriptor for tuples
    pub tuple_store_cxt: Option<MemoryContext>,
    pub tuple_store_owner: Option<ResourceOwner>,
    pub rsi: Option<ReturnSetInfo>,

    pub found_varno: int32,

    /// The datums representing the function's local variables. Some are local
    /// storage in this execstate, some point at the shared copy in the function.
    pub ndatums: int32,
    pub datums: Vec<PLpgSQL_datum>,           // PLpgSQL_datum **datums
    pub datum_context: Option<MemoryContext>, // context containing variable values

    /// `paramLI` — passes local variable values to the executor.
    pub paramLI: Option<ParamListInfo>,

    // EState and resowner for "simple" expression evaluation:
    pub simple_eval_estate: Option<EState>,
    pub simple_eval_resowner: Option<ResourceOwner>,

    /// resowner for CALL, if running nonatomic procedure or DO block.
    pub procedure_resowner: Option<ResourceOwner>,

    pub cast_hash: Option<HTAB>, // lookup table for executing type casts

    // memory context for statement-lifespan temporary values:
    pub stmt_mcontext: Option<MemoryContext>, // current stmt context, or None
    pub stmt_mcontext_parent: Option<MemoryContext>, // parent of current context

    // temporary state for results from evaluation of query or expr:
    pub eval_tuptable: Option<SPITupleTable>,
    pub eval_processed: uint64,
    pub eval_econtext: Option<ExprContext>, // for executing simple expressions

    // status information for error context reporting:
    /// current stmt (`err_stmt`). The owned model can't hold a live pointer into
    /// the parse tree, so it carries the data `plpgsql_exec_error_callback`
    /// reads off the statement: its source line number and its type name.
    pub err_stmt: Option<ErrStmtMark>,
    /// current variable's declaration line, if in a DECLARE section (`err_var`);
    /// C reports `estate->err_var->lineno`.
    pub err_var: Option<int32>,
    pub err_text: Option<String>, // additional state info

    pub plugin_info: Option<u64>, // reserved for use by optional plugin
}

/// A `PLpgSQL_plugin` structure — an instrumentation plugin (`plpgsql.h`).
///
/// A collection of function pointers PL/pgSQL calls at interesting points in
/// `pl_exec.c`. The plugin sets the first five; PL/pgSQL fills the remaining
/// with pointers to some of its own functions before `func_setup`. The exact
/// `FunctionCallInfo`/datum signatures are owned by `pl_exec.c`; here the vtable
/// is carried as the C-ABI function-pointer slots (each optional/nullable),
/// mirroring the C struct field-for-field.
#[derive(Clone, Copy)]
pub struct PLpgSQL_plugin {
    // Function pointers set up by the plugin:
    pub func_setup: Option<extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_function)>,
    pub func_beg: Option<extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_function)>,
    pub func_end: Option<extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_function)>,
    pub stmt_beg: Option<extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_stmt)>,
    pub stmt_end: Option<extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_stmt)>,

    // Function pointers set by PL/pgSQL itself:
    pub error_callback: Option<extern "C" fn(*mut core::ffi::c_void)>,
    pub assign_expr:
        Option<extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_datum, *mut PLpgSQL_expr)>,
    pub assign_value: Option<
        extern "C" fn(*mut PLpgSQL_execstate, *mut PLpgSQL_datum, Datum, bool, Oid, int32),
    >,
    pub eval_datum: Option<
        extern "C" fn(
            *mut PLpgSQL_execstate,
            *mut PLpgSQL_datum,
            *mut Oid,
            *mut int32,
            *mut Datum,
            *mut bool,
        ),
    >,
    pub cast_value: Option<
        extern "C" fn(*mut PLpgSQL_execstate, Datum, *mut bool, Oid, int32, Oid, int32) -> Datum,
    >,
}

// ===========================================================================
// Struct types used during parsing (plpgsql.h)
// ===========================================================================

/// `PLword` — a (possibly quoted) single identifier from the scanner.
#[derive(Debug, Clone)]
pub struct PLword {
    pub ident: String, // palloc'd converted identifier
    pub quoted: bool,  // Was it double-quoted?
}

/// `PLcword` — a composite (dotted) identifier from the scanner.
#[derive(Debug, Clone)]
pub struct PLcword {
    pub idents: Vec<String>, // composite identifiers (list of String)
}

/// `PLwdatum` — a scanner word that resolved to a known datum.
#[derive(Debug, Clone)]
pub struct PLwdatum {
    /// referenced variable (`datum`) — opaque back-reference by dno.
    pub datum: Option<u64>,
    pub ident: Option<String>, // valid if simple name
    pub quoted: bool,
    pub idents: Vec<String>, // valid if composite name
}
