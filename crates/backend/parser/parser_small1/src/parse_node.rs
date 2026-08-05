use datum::Datum;
use elog::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use numutils::pg_strtoint64_safe;
use queryenvironment::QueryEnvironment;
use types_core::catalog::{
    BOOLOID, INT2ARRAYOID, INT2VECTOROID, INT4OID, INT8OID, NUMERICOID, OIDARRAYOID, OIDVECTOROID,
    UNKNOWNOID, BITOID,};
use types_core::fmgr::FLOAT8PASSBYVAL;
use types_core::{AttrNumber, Index, InvalidOid, Oid, ParseLoc};
use types_error::{ErrorLocation, PgResult, SoftErrorContext, ERRCODE_TOO_MANY_COLUMNS, ERROR};
use types_nodes::{
    A_Const, Alias, Const, Node, RangeTblEntry, ValUnion, VarReturningType,
};
use types_rel::{NoLock, Relation};
use types_tuple::htup::MaxTupleAttributeNumber;
use wchar::pg_enc;

use crate::parse_param::ParseRefHookState;
use crate::parser_errposition_source;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
#[allow(non_camel_case_types)]
pub enum ParseExprKind {
    #[default]
    EXPR_KIND_NONE = 0,
    EXPR_KIND_OTHER = 1,
    EXPR_KIND_JOIN_ON = 2,
    EXPR_KIND_JOIN_USING = 3,
    EXPR_KIND_FROM_SUBSELECT = 4,
    EXPR_KIND_FROM_FUNCTION = 5,
    EXPR_KIND_WHERE = 6,
    EXPR_KIND_HAVING = 7,
    EXPR_KIND_FILTER = 8,
    EXPR_KIND_WINDOW_PARTITION = 9,
    EXPR_KIND_WINDOW_ORDER = 10,
    EXPR_KIND_WINDOW_FRAME_RANGE = 11,
    EXPR_KIND_WINDOW_FRAME_ROWS = 12,
    EXPR_KIND_WINDOW_FRAME_GROUPS = 13,
    EXPR_KIND_SELECT_TARGET = 14,
    EXPR_KIND_INSERT_TARGET = 15,
    EXPR_KIND_UPDATE_SOURCE = 16,
    EXPR_KIND_UPDATE_TARGET = 17,
    EXPR_KIND_MERGE_WHEN = 18,
    EXPR_KIND_GROUP_BY = 19,
    EXPR_KIND_ORDER_BY = 20,
    EXPR_KIND_DISTINCT_ON = 21,
    EXPR_KIND_LIMIT = 22,
    EXPR_KIND_OFFSET = 23,
    EXPR_KIND_RETURNING = 24,
    EXPR_KIND_MERGE_RETURNING = 25,
    EXPR_KIND_VALUES = 26,
    EXPR_KIND_VALUES_SINGLE = 27,
    EXPR_KIND_CHECK_CONSTRAINT = 28,
    EXPR_KIND_DOMAIN_CHECK = 29,
    EXPR_KIND_COLUMN_DEFAULT = 30,
    EXPR_KIND_FUNCTION_DEFAULT = 31,
    EXPR_KIND_INDEX_EXPRESSION = 32,
    EXPR_KIND_INDEX_PREDICATE = 33,
    EXPR_KIND_STATS_EXPRESSION = 34,
    EXPR_KIND_ALTER_COL_TRANSFORM = 35,
    EXPR_KIND_EXECUTE_PARAMETER = 36,
    EXPR_KIND_TRIGGER_WHEN = 37,
    EXPR_KIND_POLICY = 38,
    EXPR_KIND_PARTITION_BOUND = 39,
    EXPR_KIND_PARTITION_EXPRESSION = 40,
    EXPR_KIND_CALL_ARGUMENT = 41,
    EXPR_KIND_COPY_WHERE = 42,
    EXPR_KIND_GENERATED_COLUMN = 43,
    EXPR_KIND_CYCLE_MARK = 44,
}

#[derive(Clone, Copy, Default)]
#[allow(non_snake_case)]
pub struct ParseNamespaceColumn {
    pub p_varno: Index,
    pub p_varattno: AttrNumber,
    pub p_vartype: Oid,
    pub p_vartypmod: i32,
    pub p_varcollid: Oid,
    pub p_varreturningtype: VarReturningType,
    pub p_varnosyn: Index,
    pub p_varattnosyn: AttrNumber,
    pub p_dontexpand: bool,
}

// p_perminfo and p_rte are Node handles, not borrows: markRTEForSelectPriv
// mutates the perminfo through the rteperminfos list, and RTEs are with_mut'd
// through p_rtable after nsitem creation (C mutates the aliased pointers); a
// standing `&` would alias those writes. Deref p_rte via rte(), which
// re-derives at each use.
#[allow(non_snake_case)]
pub struct ParseNamespaceItem<'mcx> {
    pub p_names: &'mcx Alias<'mcx>,
    pub p_rte: Node<'mcx>,
    pub p_rtindex: i32,
    pub p_perminfo: Option<Node<'mcx>>,
    pub p_nscolumns: &'mcx [ParseNamespaceColumn],
    pub p_rel_visible: bool,
    pub p_cols_visible: bool,
    // Cell: C's setNamespaceLateralState mutates items in place while they
    // are aliased by p_namespace and the per-join namespace lists.
    pub p_lateral_only: core::cell::Cell<bool>,
    pub p_lateral_ok: core::cell::Cell<bool>,
    pub p_returning_type: VarReturningType,
}

impl<'mcx> ParseNamespaceItem<'mcx> {
    #[inline]
    pub fn rte(&self) -> &'mcx RangeTblEntry<'mcx> {
        self.p_rte.as_range_tbl_entry().expect("p_rte is RangeTblEntry")
    }
}

// C divergences: parentParseState/p_queryEnv are borrows of caller-owned state
// (C aliases raw pointers); p_joinexprs/p_nullingrels are dense vectors whose
// NULL cells are None/empty (C Lists with NULL elements); hook fn-pointer
// fields are replaced by the closed p_ref_hook_state arm (rule-4 dispatch);
// the pre/post columnref hooks arrive with their installer units.
#[allow(non_snake_case)]
pub struct ParseState<'p, 'mcx> {
    pub parentParseState: Option<&'p ParseState<'p, 'mcx>>,
    pub p_sourcetext: Option<&'mcx [u8]>,
    pub p_rtable: types_nodes::NodeList<'mcx>,
    pub p_rteperminfos: types_nodes::NodeList<'mcx>,
    pub p_joinexprs: PgVec<'mcx, Option<Node<'mcx>>>,
    pub p_nullingrels: PgVec<'mcx, types_nodes::Bitmapset<'mcx>>,
    pub p_joinlist: types_nodes::NodeList<'mcx>,
    pub p_namespace: PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>>,
    pub p_lateral_active: bool,
    pub p_ctenamespace: types_nodes::NodeList<'mcx>,
    pub p_future_ctes: types_nodes::NodeList<'mcx>,
    pub p_parent_cte: Option<Node<'mcx>>,
    pub p_target_relation: Option<Relation<'mcx>>,
    pub p_target_nsitem: Option<&'mcx ParseNamespaceItem<'mcx>>,
    pub p_grouping_nsitem: Option<&'mcx ParseNamespaceItem<'mcx>>,
    pub p_is_insert: bool,
    pub p_windowdefs: types_nodes::NodeList<'mcx>,
    pub p_expr_kind: ParseExprKind,
    pub p_next_resno: i32,
    pub p_multiassign_exprs: types_nodes::NodeList<'mcx>,
    pub p_locking_clause: types_nodes::NodeList<'mcx>,
    pub p_locked_from_parent: bool,
    pub p_resolve_unknowns: bool,
    pub p_queryEnv: Option<&'p QueryEnvironment<'mcx>>,
    // Cell: check_agglevels_and_constraints marks an OUTER level (reached
    // through the shared parentParseState chain) as having aggregates.
    pub p_hasAggs: core::cell::Cell<bool>,
    pub p_hasWindowFuncs: bool,
    pub p_hasTargetSRFs: bool,
    pub p_hasSubLinks: bool,
    pub p_hasModifyingCTE: bool,
    pub p_last_srf: Option<Node<'mcx>>,
    pub p_ref_hook_state: ParseRefHookState<'p>,
    pub p_pre_columnref_hook: PreColumnRefHook,
    // C's transformOptionalSelectInto (analyze.c) NULLs the leftmost setop
    // member's intoClause through its pointer; larg is a shared borrow here,
    // so the consumed IntoClause is skipped by node identity instead.
    pub p_consumed_into: Option<Node<'mcx>>,
    // FROM-subselect Query nodes, registered at the ROOT ParseState and keyed
    // by rte.subquery pointer identity: markQueryForLocking (analyze.c)
    // mutates nested sub-Queries through shared RTE refs, and with_mut needs
    // the owning Node (RefCell: the parent chain is shared borrows, like
    // p_hasAggs).
    pub p_subquery_nodes: core::cell::RefCell<alloc::vec::Vec<Node<'mcx>>>,
}

// C's p_pre_columnref_hook fn pointer as a closed installer set (rule-4
// dispatch); DomainValue = typecmds.c replace_domain_constraint_value.
#[derive(Clone, Copy, Default)]
pub enum PreColumnRefHook {
    #[default]
    None,
    DomainValue(types_nodes::CoerceToDomainValue),
}

pub fn make_parsestate<'p, 'mcx>(
    mcx: Mcx<'mcx>,
    parent: Option<&'p ParseState<'p, 'mcx>>,
) -> ParseState<'p, 'mcx> {
    let mut pstate = ParseState {
        parentParseState: parent,
        p_sourcetext: None,
        p_rtable: types_nodes::NodeList::nil(),
        p_rteperminfos: types_nodes::NodeList::nil(),
        p_joinexprs: PgVec::new_in(mcx),
        p_nullingrels: PgVec::new_in(mcx),
        p_joinlist: types_nodes::NodeList::nil(),
        p_namespace: PgVec::new_in(mcx),
        p_lateral_active: false,
        p_ctenamespace: types_nodes::NodeList::nil(),
        p_future_ctes: types_nodes::NodeList::nil(),
        p_parent_cte: None,
        p_target_relation: None,
        p_target_nsitem: None,
        p_grouping_nsitem: None,
        p_is_insert: false,
        p_windowdefs: types_nodes::NodeList::nil(),
        p_expr_kind: ParseExprKind::EXPR_KIND_NONE,
        p_subquery_nodes: core::cell::RefCell::new(alloc::vec::Vec::new()),
        p_next_resno: 1,
        p_multiassign_exprs: types_nodes::NodeList::nil(),
        p_locking_clause: types_nodes::NodeList::nil(),
        p_locked_from_parent: false,
        p_resolve_unknowns: true,
        p_queryEnv: None,
        p_hasAggs: core::cell::Cell::new(false),
        p_hasWindowFuncs: false,
        p_hasTargetSRFs: false,
        p_hasSubLinks: false,
        p_hasModifyingCTE: false,
        p_last_srf: None,
        p_ref_hook_state: ParseRefHookState::None,
        p_pre_columnref_hook: PreColumnRefHook::None,
        p_consumed_into: None,
    };
    if let Some(parent) = parent {
        pstate.p_sourcetext = parent.p_sourcetext;
        pstate.p_pre_columnref_hook = parent.p_pre_columnref_hook;
        pstate.p_ref_hook_state = parent.p_ref_hook_state.clone();
        pstate.p_queryEnv = parent.p_queryEnv;
        pstate.p_consumed_into = parent.p_consumed_into;
    }
    pstate
}

pub fn free_parsestate(pstate: ParseState<'_, '_>) -> PgResult<()> {
    if pstate.p_next_resno - 1 > MaxTupleAttributeNumber {
        return Err(alloc::boxed::Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_COLUMNS)
                .errmsg(alloc::format!(
                    "target lists can have at most {MaxTupleAttributeNumber} entries"
                ))
                .into_error()
                .with_error_location(loc("free_parsestate")),
        ));
    }
    if let Some(rel) = pstate.p_target_relation {
        table::table_close(rel, NoLock)?;
    }
    Ok(())
}

pub fn parser_errposition(pstate: &ParseState<'_, '_>, location: i32, encoding: pg_enc) -> i32 {
    parser_errposition_source(pstate.p_sourcetext, location, encoding)
}

// C's error_context_stack callback shim is retired: the parse location is
// attached where the fallible callee (numeric_in/bit_in seam) returns.
pub fn setup_parser_errposition_callback(_pstate: &ParseState<'_, '_>, _location: i32) {}

pub fn cancel_parser_errposition_callback() {}

pub fn transformContainerType(
    container_type: &mut Oid,
    container_typmod: &mut i32,
) -> PgResult<()> {
    *container_type = lsyscache::typ::getBaseTypeAndTypmod(*container_type, container_typmod)?;
    if *container_type == INT2VECTOROID {
        *container_type = INT2ARRAYOID;
    } else if *container_type == OIDVECTOROID {
        *container_type = OIDARRAYOID;
    }
    Ok(())
}

pub fn transformContainerSubscripts() -> ! {
    panic!(
        "transformContainerSubscripts (parse_node.c): SubscriptingRef/A_Indices are not in \
         types_nodes yet and the typsubscript SubscriptRoutines handlers (subscripting.h \
         owners, e.g. arraysubs.c) are unported"
    )
}

pub fn make_const<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, '_>,
    aconst: &A_Const<'_>,
) -> PgResult<Node<'mcx>> {
    let Some(val) = aconst.val else {
        return mk_const_node(mcx, UNKNOWNOID, -2, Datum::null(), true, false, aconst.location);
    };
    let (val, typeid, typelen, typebyval) = match val {
        ValUnion::Integer(i) => (Datum::from_i32(i.ival), INT4OID, 4, true),
        ValUnion::Float(f) => {
            let mut escontext = SoftErrorContext::new(false);
            match pg_strtoint64_safe(f.fval, Some(&mut escontext)) {
                Ok(val64) if !escontext.error_occurred() => {
                    let val32 = val64 as i32;
                    if val64 == i64::from(val32) {
                        (Datum::from_i32(val32), INT4OID, 4, true)
                    } else {
                        (Datum::from_i64(val64), INT8OID, 8, FLOAT8PASSBYVAL != 0)
                    }
                }
                _ => {
                    let img = adt_numeric::io::numeric_in(f.fval, -1, None)?
                        .expect("numeric_in: soft-error escape without an escontext");
                    let bytes = img.as_bytes();
                    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, bytes.len())?;
                    mcx::vec_append_bytes(&mut buf, bytes)?;
                    (Datum::from_usize(buf.leak().as_ptr() as usize), NUMERICOID, -1, false)
                }
            }
        }
        ValUnion::Boolean(b) => (Datum::from_bool(b.boolval), BOOLOID, 1, true),
        ValUnion::String(s) => (cstring_datum_in(mcx, s.sval)?, UNKNOWNOID, -2, false),
        ValUnion::BitString(bs) => {
            // C rides setup_parser_errposition_callback around bit_in.
            let img = adt_varbit::bit_in_cstr(mcx, bs.bsval.as_bytes()).map_err(|mut e| {
                if e.cursor_position().is_none() {
                    let pos = parser_errposition(
                        pstate,
                        aconst.location,
                        mbutils::GetDatabaseEncoding(),
                    );
                    if pos > 0 {
                        e.cursor_position = Some(pos);
                    }
                }
                e
            })?;
            (Datum::from_usize(img.leak().as_ptr() as usize), BITOID, -1, false)
        }
    };
    mk_const_node(mcx, typeid, typelen, val, false, typebyval, aconst.location)
}

fn mk_const_node<'mcx>(
    mcx: Mcx<'mcx>,
    consttype: Oid,
    constlen: i32,
    constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        Const {
            consttype,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen,
            constvalue,
            constisnull,
            constbyval,
            location,
        },
    )
}

// C aliases the A_Const's string (CStringGetDatum); the sval here is not
// NUL-terminated, so the cstring image is materialized once in `mcx`.
fn cstring_datum_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, s.len() + 1)?;
    mcx::vec_append_bytes(&mut buf, s.as_bytes())?;
    buf.push(0);
    Ok(Datum::from_usize(buf.leak().as_ptr() as usize))
}
