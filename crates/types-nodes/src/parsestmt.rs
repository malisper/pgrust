//! Utility-statement parse nodes consumed by the command drivers
//! (`nodes/parsenodes.h`), trimmed to the fields the PREPARE / EXECUTE /
//! DEALLOCATE / EXPLAIN drivers read.
//!
//! prepare.c (and its peer command drivers) thread several live parse/plan
//! trees that belong to unported subsystems (the plan cache's
//! `CachedPlanSource`/`CachedPlan`, the parser's parameter-expression `Node`s,
//! the executor's `ParamListInfo`, the portal machinery's `Portal`). Where the
//! driver only passes such a value through to the owning subsystem without
//! dereferencing it, it crosses as an opaque handle newtype here, mirroring
//! the C pointer to the owner-defined struct. These resolve to the real type
//! when their owning unit lands.

use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid, TimestampTz, INVALID_OID};
use types_error::PgResult;
use types_opclass::TypeName;
use types_rel::Relation;

use crate::bitmapset::Bitmapset;
use crate::nodes::{Node, NodePtr};
use crate::parsenodes::{RTEPermissionInfo, RangeTblEntry};
use crate::primnodes::{Param, TargetEntry, VarReturningType};
use crate::queryenvironment::QueryEnvironment;
use crate::rawnodes::{Alias, ColumnRef, CommonTableExpr, JoinExpr, ParamRef, WindowDef};

/// Opaque handle to a `CachedPlanSource *` (`utils/plancache.h`). `NULL` is the
/// `None` of the carrier. Owned by the unported plancache unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CachedPlanSourceHandle(pub u64);

impl CachedPlanSourceHandle {
    /// The `NULL` plan source.
    pub const NULL: CachedPlanSourceHandle = CachedPlanSourceHandle(0);
}

/// Opaque handle to a `CachedPlan *` (`utils/plancache.h`). Owned by the
/// unported plancache unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CachedPlanHandle(pub u64);

impl CachedPlanHandle {
    /// The `NULL` cached plan.
    pub const NULL: CachedPlanHandle = CachedPlanHandle(0);
}

/// Opaque handle to a `Portal` (`utils/portal.h`, name-keyed in portalmem.c).
/// Owned by the unported portalmem unit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PortalHandle(pub alloc::string::String);

/// Opaque handle to a `MemoryContext` (`utils/mmgr`). The portal's
/// `portalContext` is owned by the unported portalmem unit, so the driver only
/// threads the handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MemoryContextHandle(pub u64);

impl MemoryContextHandle {
    /// The `NULL` memory context.
    pub const NULL: MemoryContextHandle = MemoryContextHandle(0);
}

/// `ResourceOwner` (`utils/resowner.h`) — the one canonical
/// [`types_resowner::ResourceOwner`] handle, re-exported so the PREPARE/EXECUTE
/// and decoding drivers keep naming it `ResourceOwnerHandle` (C NULL ==
/// `ResourceOwnerHandle::NULL`).
pub type ResourceOwnerHandle = types_resowner::ResourceOwner;

/// Opaque handle to a `DestReceiver *` (`tcop/dest.h`). Owned by the caller of
/// the EXECUTE driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DestReceiverHandle(pub u64);

impl DestReceiverHandle {
    /// The `NULL` dest receiver.
    pub const NULL: DestReceiverHandle = DestReceiverHandle(0);
}

/// Opaque handle to a `QueryCompletion *` (`tcop/cmdtag.h`). Owned by the
/// caller of the EXECUTE driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryCompletionHandle(pub u64);

impl QueryCompletionHandle {
    /// The `NULL` query completion.
    pub const NULL: QueryCompletionHandle = QueryCompletionHandle(0);
}

/// `CommandTag` (`tcop/cmdtag.h`) — the statement's command-tag enumerator,
/// carried as its integer value (the generated `cmdtaglist.h` order).
/// Canonically defined in `types_core` (shared with the matview/plancache
/// layers, which do not depend on `types-nodes`).
pub use types_core::cmdtag::CommandTag;

/// `ProcessUtilityContext` (`tcop/utility.h`) — identifies the nesting /
/// atomicity context a utility statement is executed in. Discriminants follow
/// PostgreSQL 18.3's enumeration order exactly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ProcessUtilityContext {
    /// toplevel interactive command
    #[default]
    PROCESS_UTILITY_TOPLEVEL = 0,
    /// a complete query, but not toplevel
    PROCESS_UTILITY_QUERY,
    /// a complete query, nonatomic execution context
    PROCESS_UTILITY_QUERY_NONATOMIC,
    /// a portion of a query
    PROCESS_UTILITY_SUBCOMMAND,
}
pub use ProcessUtilityContext::{
    PROCESS_UTILITY_QUERY, PROCESS_UTILITY_QUERY_NONATOMIC, PROCESS_UTILITY_SUBCOMMAND,
    PROCESS_UTILITY_TOPLEVEL,
};

/// `ParseExprKind` (`parser/parse_node.h`) — the kind of expression currently
/// being parsed. `EXPR_KIND_NONE` when not in an expression. Discriminants
/// follow PostgreSQL 18.3's enumeration order exactly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ParseExprKind {
    /// "not in an expression"
    #[default]
    EXPR_KIND_NONE = 0,
    /// reserved for extensions
    EXPR_KIND_OTHER = 1,
    /// JOIN ON
    EXPR_KIND_JOIN_ON = 2,
    /// JOIN USING
    EXPR_KIND_JOIN_USING = 3,
    /// sub-SELECT in FROM clause
    EXPR_KIND_FROM_SUBSELECT = 4,
    /// function in FROM clause
    EXPR_KIND_FROM_FUNCTION = 5,
    /// WHERE
    EXPR_KIND_WHERE = 6,
    /// HAVING
    EXPR_KIND_HAVING = 7,
    /// FILTER
    EXPR_KIND_FILTER = 8,
    /// window definition PARTITION BY
    EXPR_KIND_WINDOW_PARTITION = 9,
    /// window definition ORDER BY
    EXPR_KIND_WINDOW_ORDER = 10,
    /// window frame clause with RANGE
    EXPR_KIND_WINDOW_FRAME_RANGE = 11,
    /// window frame clause with ROWS
    EXPR_KIND_WINDOW_FRAME_ROWS = 12,
    /// window frame clause with GROUPS
    EXPR_KIND_WINDOW_FRAME_GROUPS = 13,
    /// SELECT target list item
    EXPR_KIND_SELECT_TARGET = 14,
    /// INSERT target list item
    EXPR_KIND_INSERT_TARGET = 15,
    /// UPDATE assignment source item
    EXPR_KIND_UPDATE_SOURCE = 16,
    /// UPDATE assignment target item
    EXPR_KIND_UPDATE_TARGET = 17,
    /// MERGE WHEN [NOT] MATCHED condition
    EXPR_KIND_MERGE_WHEN = 18,
    /// GROUP BY
    EXPR_KIND_GROUP_BY = 19,
    /// ORDER BY
    EXPR_KIND_ORDER_BY = 20,
    /// DISTINCT ON
    EXPR_KIND_DISTINCT_ON = 21,
    /// LIMIT
    EXPR_KIND_LIMIT = 22,
    /// OFFSET
    EXPR_KIND_OFFSET = 23,
    /// RETURNING in INSERT/UPDATE/DELETE
    EXPR_KIND_RETURNING = 24,
    /// RETURNING in MERGE
    EXPR_KIND_MERGE_RETURNING = 25,
    /// VALUES
    EXPR_KIND_VALUES = 26,
    /// single-row VALUES (in INSERT only)
    EXPR_KIND_VALUES_SINGLE = 27,
    /// CHECK constraint for a table
    EXPR_KIND_CHECK_CONSTRAINT = 28,
    /// CHECK constraint for a domain
    EXPR_KIND_DOMAIN_CHECK = 29,
    /// default value for a table column
    EXPR_KIND_COLUMN_DEFAULT = 30,
    /// default parameter value for function
    EXPR_KIND_FUNCTION_DEFAULT = 31,
    /// index expression
    EXPR_KIND_INDEX_EXPRESSION = 32,
    /// index predicate
    EXPR_KIND_INDEX_PREDICATE = 33,
    /// extended statistics expression
    EXPR_KIND_STATS_EXPRESSION = 34,
    /// transform expr in ALTER COLUMN TYPE
    EXPR_KIND_ALTER_COL_TRANSFORM = 35,
    /// parameter value in EXECUTE
    EXPR_KIND_EXECUTE_PARAMETER = 36,
    /// WHEN condition in CREATE TRIGGER
    EXPR_KIND_TRIGGER_WHEN = 37,
    /// USING or WITH CHECK expr in policy
    EXPR_KIND_POLICY = 38,
    /// partition bound expression
    EXPR_KIND_PARTITION_BOUND = 39,
    /// PARTITION BY expression
    EXPR_KIND_PARTITION_EXPRESSION = 40,
    /// procedure argument in CALL
    EXPR_KIND_CALL_ARGUMENT = 41,
    /// WHERE condition in COPY FROM
    EXPR_KIND_COPY_WHERE = 42,
    /// generation expression for a column
    EXPR_KIND_GENERATED_COLUMN = 43,
    /// cycle mark value
    EXPR_KIND_CYCLE_MARK = 44,
}

/// `PreParseColumnRefHook` (`parser/parse_node.h`) — optional parser hook
/// invoked before resolving a `ColumnRef`. Returns the replacement `Node`, or
/// `None` to fall through to the default resolution. The hook owner (the
/// parser's caller, e.g. SPI/PL) supplies the function; it is `None` unless set
/// by `make_parsestate`'s caller.
pub type PreParseColumnRefHook<'mcx> =
    fn(&mut ParseState<'mcx>, &ColumnRef<'mcx>) -> PgResult<Option<NodePtr<'mcx>>>;

/// `PostParseColumnRefHook` (`parser/parse_node.h`) — optional parser hook
/// invoked after default `ColumnRef` resolution (`var` is the default result,
/// possibly `None`). Returns the (possibly rewritten) `Node`.
pub type PostParseColumnRefHook<'mcx> = fn(
    &mut ParseState<'mcx>,
    &ColumnRef<'mcx>,
    Option<NodePtr<'mcx>>,
) -> PgResult<Option<NodePtr<'mcx>>>;

/// `ParseParamRefHook` (`parser/parse_node.h`) — optional parser hook for
/// resolving a `$n` `ParamRef`. Returns the replacement `Node`.
pub type ParseParamRefHook<'mcx> =
    fn(&mut ParseState<'mcx>, &ParamRef) -> PgResult<Option<NodePtr<'mcx>>>;

/// `CoerceParamHook` (`parser/parse_node.h`) — optional parser hook for coercing
/// a `Param` to a target type. Returns the coerced `Node`.
pub type CoerceParamHook<'mcx> =
    fn(&mut ParseState<'mcx>, &Param, Oid, i32, i32) -> PgResult<Option<NodePtr<'mcx>>>;

/// `VarParamState` (`parser/parse_param.c`) — the variable-parameter hook's
/// reference state. In C this is `{ Oid **paramTypes; int *numParams; }`, two
/// pointers that *alias the caller's* mutable `Oid *` type array and its element
/// count; `variable_paramref_hook` / `variable_coerce_param_hook` re-`palloc`
/// and write through them so the caller (e.g. `PrepareQuery`) reads the resolved
/// types back after parse analysis.
///
/// The owned model keeps the caller-aliasing semantics safely: the type array
/// lives in a single `Rc<RefCell<Vec<Oid>>>` that the caller constructs, hands
/// to [`setup_parse_variable_parameters`], and then inspects after analysis. The
/// `Vec`'s length is the C `*numParams` (the two-level `int *numParams` pointer
/// collapses to the shared vector's own length). Cloning the carrier clones the
/// `Rc` (the same shared array), matching C's pointer aliasing exactly.
#[derive(Clone)]
pub struct VarParamState {
    /// The shared, growable parameter-type array (`Oid **paramTypes`, with
    /// `*numParams == param_types.borrow().len()`). A zero (`InvalidOid`) entry
    /// means that parameter number hasn't been seen; `UNKNOWNOID` means it has
    /// been used but its type is not yet known.
    pub param_types: Rc<RefCell<Vec<Oid>>>,
}

impl VarParamState {
    /// Build a `VarParamState` over a freshly shared, empty type array.
    pub fn new() -> VarParamState {
        VarParamState {
            param_types: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Build a `VarParamState` sharing the caller's existing type array. The
    /// caller retains its `Rc` clone to read the resolved types back.
    pub fn from_shared(param_types: Rc<RefCell<Vec<Oid>>>) -> VarParamState {
        VarParamState { param_types }
    }
}

impl Default for VarParamState {
    fn default() -> VarParamState {
        VarParamState::new()
    }
}

/// `FixedParamState` (`parser/parse_param.c`) — the fixed-parameter hook's
/// reference state. In C this is `{ const Oid *paramTypes; int numParams; }`: a
/// pointer aliasing the caller's *immutable* `const Oid *` type array (alive for
/// the duration of analysis) and its element count.
///
/// Unlike [`VarParamState`], the fixed array is never mutated during analysis —
/// `fixed_paramref_hook` only reads it. The owned model therefore keeps the
/// types in an `Rc<Vec<Oid>>` (a cheap-to-clone shared snapshot of the caller's
/// array, with the `Vec`'s length serving as C's `numParams`). The state lives
/// in `pstate->p_ref_hook_state` for the whole walk, exactly as C's
/// `setup_parse_fixed_parameters` installs it.
#[derive(Clone)]
pub struct FixedParamState {
    /// `const Oid *paramTypes` / `int numParams` — the fixed parameter-type
    /// array (with `numParams == param_types.len()`). An `InvalidOid` entry is
    /// an unspecified parameter slot (rejected by the hook).
    pub param_types: Rc<Vec<Oid>>,
}

impl FixedParamState {
    /// Build a `FixedParamState` from the caller's fixed type array.
    pub fn new(param_types: &[Oid]) -> FixedParamState {
        FixedParamState {
            param_types: Rc::new(param_types.to_vec()),
        }
    }
}

/// `SQLFunctionParseInfo` (`executor/functions.c`) — the ref-hook state installed
/// by `sql_fn_parser_setup` when parsing a SQL-function body. It lets a body
/// bareword that names a function parameter (or `fname.param`, `fname.param.field`,
/// `param.field`) resolve to the corresponding `$n` `Param`, and lets a `$n`
/// `ParamRef` resolve against the function's declared argument types.
///
/// Built by `prepare_sql_fn_parse_info` from the function's `pg_proc` row: the
/// function name (`fname`, used only to qualify argument names), the input
/// collation, the (poly-resolved) argument types, and — when present — the
/// argument names. `argnames` is `None` when the function has no named arguments
/// (or too few name entries); an individual `None` entry is an unnamed slot.
#[derive(Clone)]
pub struct SqlFnParseInfo {
    /// `pinfo->fname` — the function's name (only used to qualify argument
    /// names in `name.param` / `name.param.field` references).
    pub fname: alloc::string::String,
    /// `pinfo->collation` — the function's input collation; when valid it
    /// overrides the type-derived collation of a parameter `Param`.
    pub collation: Oid,
    /// `pinfo->argtypes` — the (polymorphic-resolved) declared argument types,
    /// `numParams == argtypes.len()`.
    pub argtypes: Rc<Vec<Oid>>,
    /// `pinfo->argnames` — per-argument names, or `None` when the function has
    /// no usable argument names. An individual `None` entry is an unnamed slot.
    pub argnames: Option<Rc<Vec<Option<alloc::string::String>>>>,
}

impl SqlFnParseInfo {
    /// Build a `SqlFnParseInfo` from the function's name, input collation,
    /// argument types, and (optional) argument names.
    pub fn new(
        fname: alloc::string::String,
        collation: Oid,
        argtypes: Vec<Oid>,
        argnames: Option<Vec<Option<alloc::string::String>>>,
    ) -> SqlFnParseInfo {
        SqlFnParseInfo {
            fname,
            collation,
            argtypes: Rc::new(argtypes),
            argnames: argnames.map(Rc::new),
        }
    }
}

/// One PL/pgSQL variable resolvable in an expression: the data the parser hook
/// needs to materialize a `PARAM_EXTERN` `Param` for a reference to it (the C
/// `make_datum_param` result). `dno` is the variable's datum number; the C
/// param id is `dno + 1`.
#[derive(Clone)]
pub struct PlpgsqlParamInfo {
    /// `datum->dno` — the variable's datum number. The `Param.paramid` is
    /// `dno + 1`.
    pub dno: i32,
    /// The variable's type OID (`Param.paramtype`).
    pub typeid: Oid,
    /// The variable's typmod (`Param.paramtypmod`).
    pub typmod: i32,
    /// The variable's collation (`Param.paramcollid`).
    pub collation: Oid,
}

/// `PLpgSQL_expr`'s parser ref-hook state (`pl_comp.c`'s
/// `plpgsql_parser_setup` / `plpgsql_pre_column_ref` / `plpgsql_param_ref`).
///
/// In C the hook state is the live `PLpgSQL_expr *`, and the hooks walk the
/// expr's namespace chain (`plpgsql_ns_lookup`) on demand to resolve a bareword
/// (`a`) or `block.var` reference to the variable's datum, then build a
/// `PARAM_EXTERN` `Param` via `make_datum_param`. The owned ref-hook state is an
/// enum arm (not a `void *`), so the namespace is pre-resolved at expr-prepare
/// time into a name → [`PlpgsqlParamInfo`] map the hook reads; the set of
/// referenced datum numbers is recorded back through the shared `paramnos`
/// (mirroring C's `expr->paramnos` bitmap, which `setup_param_list` later reads
/// to know which estate datums to bind).
#[derive(Clone)]
pub struct PlpgsqlExprParseState {
    /// Lower-cased variable name → its [`PlpgsqlParamInfo`]. The plpgsql scanner
    /// already down-cases identifiers, so the map is keyed on the down-cased
    /// name. A `block.var` qualified reference is stored under both `var` and
    /// `block.var` keys by the builder.
    pub names: Rc<alloc::collections::BTreeMap<alloc::string::String, PlpgsqlParamInfo>>,
    /// Down-cased set of enclosing-block LABEL names visible to the expr (the
    /// `PLPGSQL_NSTYPE_LABEL` nsitems). C's `plpgsql_ns_lookup` only strips a
    /// *leading block label* from a qualified reference before matching the
    /// remaining var/record name — it never drops an arbitrary leading
    /// qualifier (e.g. a SQL table alias). The pre-columnref hook consults this
    /// set so `t.balance` (where `t` is a table alias, not a plpgsql label) does
    /// NOT spuriously resolve to a scalar var named `balance`.
    pub labels: Rc<alloc::collections::BTreeSet<alloc::string::String>>,
    /// The datum numbers actually referenced during the parse (C
    /// `expr->paramnos`). Shared, growable; read back after analysis to drive
    /// `setup_param_list`. Cloning the carrier shares the same `Vec` (the hook
    /// records into the live state the caller inspects afterward).
    pub paramnos: Rc<core::cell::RefCell<Vec<i32>>>,
    /// `pinfo->collation` analogue — the expr's function input collation, used
    /// when the variable's own collation is invalid (C `make_datum_param` keeps
    /// the datum's collation; this is a fallback for unset collations).
    pub input_collation: Oid,
}

impl PlpgsqlExprParseState {
    /// Build the parse state from the pre-resolved name → param-info map.
    pub fn new(
        names: alloc::collections::BTreeMap<alloc::string::String, PlpgsqlParamInfo>,
        input_collation: Oid,
    ) -> PlpgsqlExprParseState {
        Self::with_labels(names, alloc::collections::BTreeSet::new(), input_collation)
    }

    /// Build the parse state with an explicit set of enclosing block-label
    /// names (see [`PlpgsqlExprParseState::labels`]).
    pub fn with_labels(
        names: alloc::collections::BTreeMap<alloc::string::String, PlpgsqlParamInfo>,
        labels: alloc::collections::BTreeSet<alloc::string::String>,
        input_collation: Oid,
    ) -> PlpgsqlExprParseState {
        PlpgsqlExprParseState {
            names: Rc::new(names),
            labels: Rc::new(labels),
            paramnos: Rc::new(core::cell::RefCell::new(Vec::new())),
            input_collation,
        }
    }

    /// Record that datum `dno` was referenced (C `bms_add_member(expr->paramnos,
    /// dno)`); idempotent.
    pub fn record_paramno(&self, dno: i32) {
        let mut p = self.paramnos.borrow_mut();
        if !p.contains(&dno) {
            p.push(dno);
        }
    }

    /// The recorded referenced datum numbers, sorted ascending.
    pub fn referenced_dnos(&self) -> Vec<i32> {
        let mut v = self.paramnos.borrow().clone();
        v.sort_unstable();
        v
    }
}

/// `void *p_ref_hook_state` (`parser/parse_node.h`) — common passthrough state
/// for the parser hook functions above. Owned by the hook installer.
///
/// The C field is a bare `void *` whose pointee is whatever the hook installer
/// chose; in the core backend the two concrete installers are
/// `setup_parse_variable_parameters` (its [`VarParamState`] is read back by the
/// post-analysis `check_variable_parameters` pass) and
/// `setup_parse_fixed_parameters` (its [`FixedParamState`], read during the
/// walk). Both are modeled here as enum arms — no opaque stand-in is introduced.
/// The active arm is also what selects the installed paramref hook, mirroring
/// the C `pstate->p_paramref_hook = {fixed,variable}_paramref_hook` assignment.
#[derive(Clone)]
pub enum ParseRefHookState {
    /// C `NULL` — no ref-hook state installed.
    None,
    /// `setup_parse_variable_parameters`' shared, growable type array.
    VarParams(VarParamState),
    /// `setup_parse_fixed_parameters`' fixed type array.
    FixedParams(FixedParamState),
    /// `sql_fn_parser_setup`'s SQL-function-body parse info (executor/functions.c):
    /// resolves a body bareword that names a function parameter to its `$n`
    /// `Param`, and a `$n` `ParamRef` against the function's argument types.
    SqlFunction(SqlFnParseInfo),
    /// `plpgsql_parser_setup`'s PL/pgSQL expression parse state (pl_comp.c):
    /// resolves a bareword (or `block.var`) that names a PL/pgSQL variable to a
    /// `PARAM_EXTERN` `Param` (paramid = dno+1) via the pre-resolved namespace
    /// map, recording the referenced datum numbers back through `paramnos`.
    PlpgsqlExpr(PlpgsqlExprParseState),
    /// `domainAddCheckConstraint`'s prepared `CoerceToDomainValue *` (typecmds.c):
    /// the template node `replace_domain_constraint_value` copies when it sees a
    /// reference to `VALUE` in a domain CHECK constraint expression. C stores the
    /// bare `CoerceToDomainValue *` in `p_ref_hook_state`; here we carry the value.
    DomainCheckValue(crate::primnodes::CoerceToDomainValue),
}

impl ParseRefHookState {
    /// True if a ref-hook state is installed (C `p_ref_hook_state != NULL`).
    pub fn is_some(&self) -> bool {
        !matches!(self, ParseRefHookState::None)
    }

    /// The installed [`VarParamState`], if this is the variable-parameter case.
    pub fn as_var_params(&self) -> Option<&VarParamState> {
        match self {
            ParseRefHookState::VarParams(v) => Some(v),
            _ => None,
        }
    }

    /// The installed [`FixedParamState`], if this is the fixed-parameter case.
    pub fn as_fixed_params(&self) -> Option<&FixedParamState> {
        match self {
            ParseRefHookState::FixedParams(f) => Some(f),
            _ => None,
        }
    }
}

/// `ParseState` (`parser/parse_node.h`) — the working state threaded through
/// parse analysis. The full ~36-field struct; this is the single canonical home
/// for the parser cluster (`parse_expr`/`parse_relation`/`parse_clause`/...).
///
/// `make_parsestate` (parser/parse_node.c) is the C `palloc0` image with the
/// two nonzero starts (`p_next_resno = 1`, `p_resolve_unknowns = true`); see
/// [`ParseState::new`].
pub struct ParseState<'mcx> {
    /// `ParseState *parentParseState` — stack link; `None` in a top-level state.
    pub parentParseState: Option<PgBox<'mcx, ParseState<'mcx>>>,
    /// `const char *p_sourcetext` — the source text of the current query (used
    /// only for error cursor positions); `None` if not available.
    pub p_sourcetext: Option<PgString<'mcx>>,
    /// `List *p_rtable` — range table so far.
    pub p_rtable: PgVec<'mcx, RangeTblEntry<'mcx>>,
    /// `List *p_rteperminfos` — `RTEPermissionInfo` for each `RTE_RELATION`.
    pub p_rteperminfos: PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    /// `List *p_joinexprs` — `JoinExpr`s for `RTE_JOIN` p_rtable entries
    /// (one-for-one with p_rtable, `None` for non-join RTEs, may be shorter).
    pub p_joinexprs: PgVec<'mcx, Option<PgBox<'mcx, JoinExpr<'mcx>>>>,
    /// `List *p_nullingrels` — `Bitmapset`s showing nulling outer joins
    /// (one-for-one with p_rtable, may be shorter; missing == empty).
    pub p_nullingrels: PgVec<'mcx, Bitmapset<'mcx>>,
    /// `List *p_joinlist` — join items (`RangeTblRef`/`JoinExpr` nodes) that
    /// will become the top-level `FromExpr`'s fromlist.
    pub p_joinlist: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *p_namespace` — currently-referenceable RTEs
    /// (list of `ParseNamespaceItem`).
    pub p_namespace: PgVec<'mcx, ParseNamespaceItem<'mcx>>,
    /// `bool p_lateral_active` — are `p_lateral_only` items visible?
    pub p_lateral_active: bool,
    /// `List *p_ctenamespace` — current namespace for common table exprs.
    pub p_ctenamespace: PgVec<'mcx, CommonTableExpr<'mcx>>,
    /// `List *p_future_ctes` — common table exprs not yet in namespace.
    pub p_future_ctes: PgVec<'mcx, CommonTableExpr<'mcx>>,
    /// `CommonTableExpr *p_parent_cte` — this query's containing CTE.
    pub p_parent_cte: Option<PgBox<'mcx, CommonTableExpr<'mcx>>>,
    /// `Relation p_target_relation` — INSERT/UPDATE/DELETE/MERGE target rel.
    pub p_target_relation: Option<Relation<'mcx>>,
    /// `ParseNamespaceItem *p_target_nsitem` — target rel's NSItem, or `None`.
    pub p_target_nsitem: Option<PgBox<'mcx, ParseNamespaceItem<'mcx>>>,
    /// `ParseNamespaceItem *p_grouping_nsitem` — NSItem for grouping, or `None`.
    pub p_grouping_nsitem: Option<PgBox<'mcx, ParseNamespaceItem<'mcx>>>,
    /// `bool p_is_insert` — process assignment like INSERT, not UPDATE.
    pub p_is_insert: bool,
    /// `List *p_windowdefs` — raw representations of window clauses.
    pub p_windowdefs: PgVec<'mcx, WindowDef<'mcx>>,
    /// `ParseExprKind p_expr_kind` — what kind of expression we're parsing.
    pub p_expr_kind: ParseExprKind,
    /// `int p_next_resno` — next `TargetEntry.resno` to assign (from 1).
    pub p_next_resno: i32,
    /// `List *p_multiassign_exprs` — junk tlist entries for multiassign.
    pub p_multiassign_exprs: PgVec<'mcx, TargetEntry<'mcx>>,
    /// `List *p_locking_clause` — raw FOR UPDATE/FOR SHARE info.
    pub p_locking_clause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `bool p_locked_from_parent` — parent has marked this subquery FOR
    /// UPDATE/SHARE.
    pub p_locked_from_parent: bool,
    /// `bool p_resolve_unknowns` — resolve unknown-type SELECT outputs as text.
    pub p_resolve_unknowns: bool,
    /// `QueryEnvironment *p_queryEnv` — current env, or `None` for the default.
    pub p_queryEnv: Option<PgBox<'mcx, QueryEnvironment<'mcx>>>,
    /// `bool p_hasAggs` — found aggregates in the query?
    pub p_hasAggs: bool,
    /// `bool p_hasWindowFuncs` — found window functions?
    pub p_hasWindowFuncs: bool,
    /// `bool p_hasTargetSRFs` — found set-returning functions in a target list?
    pub p_hasTargetSRFs: bool,
    /// `bool p_hasSubLinks` — found subquery `SubLink`s?
    pub p_hasSubLinks: bool,
    /// `bool p_hasModifyingCTE` — found a data-modifying CTE?
    pub p_hasModifyingCTE: bool,
    /// `Node *p_last_srf` — most recent set-returning func/op found, or `None`.
    pub p_last_srf: Option<NodePtr<'mcx>>,
    /// `PreParseColumnRefHook p_pre_columnref_hook` — optional, `None` unless set.
    pub p_pre_columnref_hook: Option<PreParseColumnRefHook<'mcx>>,
    /// `PostParseColumnRefHook p_post_columnref_hook` — optional.
    pub p_post_columnref_hook: Option<PostParseColumnRefHook<'mcx>>,
    /// `ParseParamRefHook p_paramref_hook` — optional.
    pub p_paramref_hook: Option<ParseParamRefHook<'mcx>>,
    /// `CoerceParamHook p_coerce_param_hook` — optional.
    pub p_coerce_param_hook: Option<CoerceParamHook<'mcx>>,
    /// `void *p_ref_hook_state` — common passthrough state for the hooks above.
    pub p_ref_hook_state: ParseRefHookState,
}

impl<'mcx> ParseState<'mcx> {
    /// `make_parsestate(NULL)` (parser/parse_node.c) over a fresh top-level
    /// state: the C `palloc0` image with the two nonzero starts
    /// (`p_next_resno = 1`, `p_resolve_unknowns = true`). The `mcx` arena backs
    /// the (initially empty) list fields. Hooks default to `None`.
    pub fn new(mcx: Mcx<'mcx>) -> PgResult<ParseState<'mcx>> {
        Ok(ParseState {
            parentParseState: None,
            p_sourcetext: None,
            p_rtable: PgVec::new_in(mcx),
            p_rteperminfos: PgVec::new_in(mcx),
            p_joinexprs: PgVec::new_in(mcx),
            p_nullingrels: PgVec::new_in(mcx),
            p_joinlist: PgVec::new_in(mcx),
            p_namespace: PgVec::new_in(mcx),
            p_lateral_active: false,
            p_ctenamespace: PgVec::new_in(mcx),
            p_future_ctes: PgVec::new_in(mcx),
            p_parent_cte: None,
            p_target_relation: None,
            p_target_nsitem: None,
            p_grouping_nsitem: None,
            p_is_insert: false,
            p_windowdefs: PgVec::new_in(mcx),
            p_expr_kind: ParseExprKind::EXPR_KIND_NONE,
            p_next_resno: 1,
            p_multiassign_exprs: PgVec::new_in(mcx),
            p_locking_clause: PgVec::new_in(mcx),
            p_locked_from_parent: false,
            p_resolve_unknowns: true,
            p_queryEnv: None,
            p_hasAggs: false,
            p_hasWindowFuncs: false,
            p_hasTargetSRFs: false,
            p_hasSubLinks: false,
            p_hasModifyingCTE: false,
            p_last_srf: None,
            p_pre_columnref_hook: None,
            p_post_columnref_hook: None,
            p_paramref_hook: None,
            p_coerce_param_hook: None,
            p_ref_hook_state: ParseRefHookState::None,
        })
    }

    /// Clone the read-only spine of this `ParseState` (and, recursively, its
    /// ancestors) into `mcx` for use as a child state's `parentParseState`.
    ///
    /// C threads `parentParseState` as a non-owning back-pointer so a child
    /// state reads the live parent. The owned model holds the parent by value,
    /// so a child built by `make_parsestate(parent)` instead carries a deep copy
    /// of the fields the upper-level walks read: the range table, the namespace
    /// and CTE namespace, the containing CTE, the source text, and the hooks.
    /// Mutable-back-write fields (per-RTE select-priv marks, refcount bumps that
    /// must reach `qry->cteList`) resolve at the same query level in practice;
    /// the recursive-CTE self-reference, which only *reads* the parent's
    /// `p_ctenamespace`, is the case this copy enables.
    pub fn clone_read_spine<'b>(&self, mcx: Mcx<'b>) -> PgResult<ParseState<'b>> {
        let mut out = ParseState::new(mcx)?;

        out.parentParseState = match self.parentParseState.as_deref() {
            Some(p) => Some(PgBox::new_in(p.clone_read_spine(mcx)?, mcx)),
            None => None,
        };
        out.p_sourcetext = match self.p_sourcetext.as_ref() {
            Some(s) => Some(s.clone_in(mcx)?),
            None => None,
        };

        out.p_rtable = PgVec::new_in(mcx);
        out.p_rtable
            .try_reserve(self.p_rtable.len())
            .map_err(|_| mcx.oom(self.p_rtable.len()))?;
        for rte in self.p_rtable.iter() {
            out.p_rtable.push(rte.clone_in(mcx)?);
        }

        out.p_namespace = PgVec::new_in(mcx);
        out.p_namespace
            .try_reserve(self.p_namespace.len())
            .map_err(|_| mcx.oom(self.p_namespace.len()))?;
        for ns in self.p_namespace.iter() {
            out.p_namespace.push(ns.clone_in(mcx)?);
        }

        out.p_ctenamespace = PgVec::new_in(mcx);
        out.p_ctenamespace
            .try_reserve(self.p_ctenamespace.len())
            .map_err(|_| mcx.oom(self.p_ctenamespace.len()))?;
        for cte in self.p_ctenamespace.iter() {
            out.p_ctenamespace.push(cte.clone_in(mcx)?);
        }

        // The permission-info list is part of the spine an upper-level reference
        // reads/marks: a LATERAL or correlated subquery resolves an outer-query
        // Var via `parentParseState` and then marks SELECT privilege on the
        // outer RTE's `RTEPermissionInfo` (markVarForSelectPriv walks up
        // `varlevelsup` levels). Without this copy the cloned ancestor's
        // `p_rteperminfos` is empty and getRTEPermissionInfo raises "invalid
        // perminfoindex". `parse_sub_analyze` merges these marks back into the
        // live parent after sub-analysis (the owned model holds the parent by
        // value, so the marks land on the clone here).
        out.p_rteperminfos = PgVec::new_in(mcx);
        out.p_rteperminfos
            .try_reserve(self.p_rteperminfos.len())
            .map_err(|_| mcx.oom(self.p_rteperminfos.len()))?;
        for pi in self.p_rteperminfos.iter() {
            out.p_rteperminfos.push(pi.clone_in(mcx)?);
        }

        out.p_lateral_active = self.p_lateral_active;

        // The nulling-rels list is also part of the spine an upper-level Var
        // reference reads: a correlated subquery resolves an outer-query Var via
        // `parentParseState` and then `markNullableIfNeeded` consults the
        // ancestor's `p_nullingrels[varno-1]` to set the Var's `varnullingrels`
        // (so a reference to the nullable side of an outer join, made inside a
        // SubLink, is correctly marked nulled by that join). Without this copy
        // the cloned ancestor's `p_nullingrels` is empty and the correlated Var
        // gets no nullingrels, diverging from C (which holds the parent by a
        // live back-pointer) and tripping setrefs' `wrong varnullingrels` check.
        // This list is never mutated through the clone (markNullableIfNeeded
        // only reads it), so no merge-back step is needed.
        out.p_nullingrels = PgVec::new_in(mcx);
        out.p_nullingrels
            .try_reserve(self.p_nullingrels.len())
            .map_err(|_| mcx.oom(self.p_nullingrels.len()))?;
        for bms in self.p_nullingrels.iter() {
            out.p_nullingrels.push(bms.clone_in(mcx)?);
        }

        out.p_parent_cte = match self.p_parent_cte.as_deref() {
            Some(c) => Some(PgBox::new_in(c.clone_in(mcx)?, mcx)),
            None => None,
        };

        // p_expr_kind is part of the spine an outer-level aggregate's constraint
        // check reads through `parentParseState`. `check_agglevels_and_constraints`
        // (parse_agg.c) walks `pstate` up `agglevelsup` levels and then reads the
        // walked-up level's `p_expr_kind` to decide whether the aggregate sits in
        // a forbidden place (e.g. EXPR_KIND_FROM_SUBSELECT for an aggregate whose
        // Vars belong to the level recursing into a FROM sub-SELECT). In C the
        // back-pointer reaches the live outer pstate (whose p_expr_kind is set by
        // transformRangeSubselect); the owned model resolves through this clone,
        // so the field must be carried or the check sees EXPR_KIND_NONE and the
        // error is missed. Read-only at the ancestor level, so no merge-back.
        out.p_expr_kind = self.p_expr_kind;

        // Hooks (fn pointers carrying the `'mcx` lifetime) are not re-lifetimed
        // into the cloned ancestor; `make_parsestate` copies them onto the child
        // directly, and hook dispatch happens at the child's own level.

        Ok(out)
    }
}

/// `ParseNamespaceItem` (`parser/parse_node.h`) — an element of a namespace
/// list: the table/column names exposed by an RTE plus the per-column Var-
/// construction data and the visibility flags used during FROM/RETURNING.
pub struct ParseNamespaceItem<'mcx> {
    /// `Alias *p_names` — table and column names exposed by this nsitem.
    pub p_names: Option<PgBox<'mcx, Alias<'mcx>>>,
    /// `RangeTblEntry *p_rte` — the relation's rangetable entry.
    pub p_rte: Option<PgBox<'mcx, RangeTblEntry<'mcx>>>,
    /// `int p_rtindex` — the relation's index in the rangetable.
    pub p_rtindex: i32,
    /// `RTEPermissionInfo *p_perminfo` — the relation's rteperminfos entry.
    pub p_perminfo: Option<PgBox<'mcx, RTEPermissionInfo<'mcx>>>,
    /// `ParseNamespaceColumn *p_nscolumns` — per-column data, same length as
    /// `p_names->colnames`.
    pub p_nscolumns: PgVec<'mcx, ParseNamespaceColumn>,
    /// `bool p_rel_visible` — relation name visible (for qualified refs)?
    pub p_rel_visible: bool,
    /// `bool p_cols_visible` — column names visible as unqualified refs?
    pub p_cols_visible: bool,
    /// `bool p_lateral_only` — visible only to LATERAL expressions?
    pub p_lateral_only: bool,
    /// `bool p_lateral_ok` — if so, does the join type allow use?
    pub p_lateral_ok: bool,
    /// `VarReturningType p_returning_type` — OLD/NEW for use in RETURNING.
    pub p_returning_type: VarReturningType,
}

impl<'mcx> ParseNamespaceItem<'mcx> {
    /// Deep-copy this namespace item into `mcx` (used to clone a parent
    /// `ParseState`'s read-only namespace spine into a child state).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ParseNamespaceItem<'b>> {
        let p_names = match self.p_names.as_deref() {
            Some(a) => Some(PgBox::new_in(a.clone_in(mcx)?, mcx)),
            None => None,
        };
        let p_rte = match self.p_rte.as_deref() {
            Some(r) => Some(PgBox::new_in(r.clone_in(mcx)?, mcx)),
            None => None,
        };
        let p_perminfo = match self.p_perminfo.as_deref() {
            Some(p) => Some(PgBox::new_in(p.clone_in(mcx)?, mcx)),
            None => None,
        };
        let mut p_nscolumns: PgVec<'b, ParseNamespaceColumn> = PgVec::new_in(mcx);
        p_nscolumns
            .try_reserve(self.p_nscolumns.len())
            .map_err(|_| mcx.oom(self.p_nscolumns.len()))?;
        for c in self.p_nscolumns.iter() {
            p_nscolumns.push(*c);
        }
        Ok(ParseNamespaceItem {
            p_names,
            p_rte,
            p_rtindex: self.p_rtindex,
            p_perminfo,
            p_nscolumns,
            p_rel_visible: self.p_rel_visible,
            p_cols_visible: self.p_cols_visible,
            p_lateral_only: self.p_lateral_only,
            p_lateral_ok: self.p_lateral_ok,
            p_returning_type: self.p_returning_type,
        })
    }
}

/// `ParseNamespaceColumn` (`parser/parse_node.h`) — data about one column of a
/// [`ParseNamespaceItem`], the info needed to construct a Var for the column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseNamespaceColumn {
    /// `Index p_varno` — rangetable index of the semantic referent.
    pub p_varno: Index,
    /// `AttrNumber p_varattno` — attribute number of the semantic referent.
    pub p_varattno: AttrNumber,
    /// `Oid p_vartype` — OID of the column's data type.
    pub p_vartype: Oid,
    /// `int32 p_vartypmod` — type modifier value.
    pub p_vartypmod: i32,
    /// `Oid p_varcollid` — OID of the column's collation, if any.
    pub p_varcollid: Oid,
    /// `VarReturningType p_varreturningtype` — OLD/NEW (duplicated per column).
    pub p_varreturningtype: VarReturningType,
    /// `Index p_varnosyn` — rangetable index for ruleutils display.
    pub p_varnosyn: Index,
    /// `AttrNumber p_varattnosyn` — attribute number for ruleutils display.
    pub p_varattnosyn: AttrNumber,
    /// `bool p_dontexpand` — suppress whole-row expansion?
    pub p_dontexpand: bool,
}

impl Default for ParseNamespaceColumn {
    /// The C `palloc0` image (all-zero) of a `ParseNamespaceColumn`.
    fn default() -> Self {
        ParseNamespaceColumn {
            p_varno: 0,
            p_varattno: 0,
            p_vartype: INVALID_OID,
            p_vartypmod: 0,
            p_varcollid: INVALID_OID,
            p_varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            p_varnosyn: 0,
            p_varattnosyn: 0,
            p_dontexpand: false,
        }
    }
}

/// `RawStmt` (`nodes/parsenodes.h`) — the wrapper a raw parse tree is placed
/// in before parse analysis, recording the statement's source-text span.
///
/// ```c
/// typedef struct RawStmt {
///     NodeTag type;
///     Node *stmt;        /* raw parse tree */
///     ParseLoc stmt_location;  /* start location, or -1 if unknown */
///     ParseLoc stmt_len;       /* length in bytes; 0 means "rest of string" */
/// } RawStmt;
/// ```
#[derive(Debug)]
pub struct RawStmt<'mcx> {
    /// `Node *stmt` — the contained raw parse tree.
    pub stmt: PgBox<'mcx, Node<'mcx>>,
    /// `ParseLoc stmt_location` — start location, or -1 if unknown.
    pub stmt_location: i32,
    /// `ParseLoc stmt_len` — length in bytes; 0 means "rest of string".
    pub stmt_len: i32,
}

impl RawStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RawStmt`). The contained raw
    /// parse tree is copied via `Node::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RawStmt<'b>> {
        Ok(RawStmt {
            stmt: mcx::alloc_in(mcx, self.stmt.clone_in(mcx)?)?,
            stmt_location: self.stmt_location,
            stmt_len: self.stmt_len,
        })
    }
}

/// `PrepareStmt` (`nodes/parsenodes.h`) — the parsed `PREPARE` statement.
#[derive(Debug)]
pub struct PrepareStmt<'mcx> {
    /// `char *name` — name of plan, arbitrary (`None` / empty = the protocol
    /// unnamed statement, which PREPARE rejects).
    pub name: Option<mcx::PgString<'mcx>>,
    /// `List *argtypes` — type names for parameters. Each is a concrete
    /// `TypeName` (the real fields the grammar's `makeTypeName` produced:
    /// `names`/`typeOid`/`setof`/`pct_type`/`typemod`/`location`); the PREPARE
    /// driver never inspects them, it hands each straight to `typenameTypeId`.
    pub argtypes: mcx::PgVec<'mcx, TypeName>,
    /// `Node *query` — the query itself (as a raw parse tree).
    pub query: Option<PgBox<'mcx, Node<'mcx>>>,
}

impl PrepareStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `PrepareStmt`). `name` is a
    /// `char *`, `argtypes` a `List *` of `TypeName` (lifetime-free, plain
    /// clone), and `query` the raw parse tree (`Node::clone_in`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PrepareStmt<'b>> {
        let mut argtypes = mcx::vec_with_capacity_in(mcx, self.argtypes.len())?;
        for t in self.argtypes.iter() {
            argtypes.push(t.clone());
        }
        Ok(PrepareStmt {
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            argtypes,
            query: match &self.query {
                Some(q) => Some(mcx::alloc_in(mcx, q.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `ExecuteStmt` (`nodes/parsenodes.h`) — the parsed `EXECUTE` statement.
#[derive(Debug)]
pub struct ExecuteStmt<'mcx> {
    /// `char *name` — the name of the prepared statement.
    pub name: Option<mcx::PgString<'mcx>>,
    /// `List *params` — values to assign to parameters (raw parser output).
    pub params: mcx::PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
}

impl ExecuteStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `ExecuteStmt`). `name` is a
    /// `char *`; `params` is a `List *` of raw parse trees, each copied via
    /// `Node::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ExecuteStmt<'b>> {
        let mut params = mcx::vec_with_capacity_in(mcx, self.params.len())?;
        for p in self.params.iter() {
            params.push(mcx::alloc_in(mcx, p.clone_in(mcx)?)?);
        }
        Ok(ExecuteStmt {
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            params,
        })
    }
}

/// `DeallocateStmt` (`nodes/parsenodes.h`) — the parsed `DEALLOCATE` statement.
#[derive(Debug)]
pub struct DeallocateStmt<'mcx> {
    /// `char *name` — the name of the prepared statement (`None` == DEALLOCATE
    /// ALL).
    pub name: Option<mcx::PgString<'mcx>>,
    /// `bool isall` — true if DEALLOCATE ALL (kept for parity; the driver
    /// branches on `name`).
    pub isall: bool,
}

impl DeallocateStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `DeallocateStmt`). `name` is a
    /// `char *`; `isall` a scalar flag.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DeallocateStmt<'b>> {
        Ok(DeallocateStmt {
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            isall: self.isall,
        })
    }
}

/// `IntoClause` (`nodes/primnodes.h`) — target-relation spec for CREATE TABLE
/// AS / SELECT INTO. The EXECUTE/EXPLAIN drivers thread it through
/// `GetIntoRelEFlags` and read `skipData`; the rest is owned by createas.
#[derive(Debug)]
pub struct IntoClause<'mcx> {
    /// `bool skipData` — true for WITH NO DATA.
    pub skipData: bool,
    /// The remaining IntoClause fields the createas unit owns, threaded as the
    /// opaque parser node payload.
    pub node: PgBox<'mcx, Node<'mcx>>,
}

impl IntoClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `IntoClause`). `skipData` is a
    /// scalar; the remaining createas-owned fields cross as the opaque parser
    /// node payload, copied via `Node::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IntoClause<'b>> {
        Ok(IntoClause {
            skipData: self.skipData,
            node: mcx::alloc_in(mcx, self.node.clone_in(mcx)?)?,
        })
    }
}

// The EXPLAIN output state (`ExplainState`, `commands/explain_state.h`) lives in
// the `types-explain` crate, now with the full node-tree fields the
// `backend-commands-explain` unit fills. The previously-trimmed placeholder that
// lived here has been retired onto that canonical type.

/// A prepared statement's stored data (`commands/prepare.h`
/// `PreparedStatement`). `stmt_name[NAMEDATALEN]` is the per-backend hash key;
/// the rest are a thin veneer over the plancache entry handle.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    /// `stmt_name[NAMEDATALEN]` — the dynahash key (truncated to
    /// `NAMEDATALEN-1`). Owned by the per-backend hash table, so a plain
    /// `String` (a backend-lifetime global per docs/mctx-design.md decision 5).
    pub stmt_name: alloc::string::String,
    /// `CachedPlanSource *plansource` — the actual cached plan.
    pub plansource: CachedPlanSourceHandle,
    /// `bool from_sql` — prepared via SQL, not the FE/BE protocol?
    pub from_sql: bool,
    /// `TimestampTz prepare_time` — the time when the stmt was prepared.
    pub prepare_time: TimestampTz,
}
