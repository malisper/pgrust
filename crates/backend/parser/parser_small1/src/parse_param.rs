use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;

use elog::ereport;
use mcx::{Mcx, MAX_ALLOC_SIZE};
use nodes_core::{expression_tree_walker, query_tree_walker, NodeWalker};
use types_core::catalog::{UNKNOWNOID, VOIDOID};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_PARAMETER, ERRCODE_UNDEFINED_PARAMETER,
    ERROR,
};
use types_nodes::parsenodes::Query;
use types_nodes::{Node, Param, ParamKind, ParamRef};
use wchar::pg_enc;

use crate::parse_node::{parser_errposition, ParseExprKind, ParseState};

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[derive(Clone, Copy)]
pub struct FixedParamState<'p> {
    pub param_types: &'p [Oid],
}

// C SQLFunctionParseInfo, reduced to the parser-visible fields; empty
// string = unnamed parameter.
#[derive(Clone, Copy)]
pub struct SqlFnParamState<'p> {
    pub fname: &'p str,
    pub argtypes: &'p [Oid],
    pub argnames: &'p [&'p str],
    pub input_collation: Oid,
}

/// C `VarParamState` aliases the caller's mutable `Oid **paramTypes` /
/// `int *numParams`; the shared `Rc<RefCell<Vec<Oid>>>` carrier reproduces
/// that back-write (the caller reads resolved types after analysis; the Vec
/// length is C's `*numParams`).
#[derive(Clone)]
pub struct VarParamState {
    pub param_types: Rc<RefCell<Vec<Oid>>>,
}

impl VarParamState {
    pub fn new() -> Self {
        VarParamState { param_types: Rc::new(RefCell::new(Vec::new())) }
    }
}

impl Default for VarParamState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
pub struct PlpgsqlNameEntry<'p> {
    /// Down-cased dotted key: "v", "label.v", "rec.f", "label.rec.f".
    pub key: &'p str,
    pub dno: i32,
    pub typoid: Oid,
    pub typmod: i32,
    pub collation: Oid,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PlpgsqlResolveOption {
    Error,
    Variable,
    Column,
}

/// plpgsql_parser_setup's hooks, data-driven: the plpgsql executor
/// pre-resolves the expr's namespace chain into a flat name table (C walks
/// the live ns via plpgsql_ns_lookup inside the hooks; the tables are a
/// compile-time constant of the expr, so the flattening is lossless).
#[derive(Clone, Copy)]
pub struct PlpgsqlHookState<'p> {
    pub names: &'p [PlpgsqlNameEntry<'p>],
    /// dno → (typoid, typmod, collation); a None slot is a datum a Param
    /// cannot carry.
    pub params_by_dno: &'p [Option<(Oid, i32, Oid)>],
    /// Function argument dnos in signature order: `$n` resolves through
    /// slot n-1 only (C's ns holds just parameter names, pl_comp.c:1062).
    pub arg_dnos: &'p [i32],
    /// Record/row variable names (incl. label-qualified) for the
    /// "record has no field" error arm of resolve_column_ref.
    pub recs: &'p [&'p str],
    /// Valueless RECORD-typed recs: any field reference is 55000 at parse
    /// (C make_datum_param -> exec_get_datum_type_info -> instantiate).
    pub valueless_recs: &'p [&'p str],
    pub resolve_option: PlpgsqlResolveOption,
    /// Out-param: dnos the analysis referenced (C expr->paramnos).
    pub used: &'p RefCell<Vec<i32>>,
}

impl<'p> PlpgsqlHookState<'p> {
    pub fn mark_used(&self, dno: i32) {
        let mut used = self.used.borrow_mut();
        if !used.contains(&dno) {
            used.push(dno);
        }
    }
}

/// C selects param hooks by installing fn pointers alongside a `void *`
/// `p_ref_hook_state`; the closed arm set is the dispatch here (rule 4).
#[derive(Clone, Default)]
pub enum ParseRefHookState<'p> {
    #[default]
    None,
    FixedParams(FixedParamState<'p>),
    VarParams(VarParamState),
    SqlFnParams(SqlFnParamState<'p>),
    PlpgsqlParams(PlpgsqlHookState<'p>),
}

impl<'p> ParseRefHookState<'p> {
    pub fn as_fixed_params(&self) -> Option<&FixedParamState<'p>> {
        match self {
            ParseRefHookState::FixedParams(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_var_params(&self) -> Option<&VarParamState> {
        match self {
            ParseRefHookState::VarParams(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_sql_fn_params(&self) -> Option<&SqlFnParamState<'p>> {
        match self {
            ParseRefHookState::SqlFnParams(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_plpgsql_params(&self) -> Option<&PlpgsqlHookState<'p>> {
        match self {
            ParseRefHookState::PlpgsqlParams(s) => Some(s),
            _ => None,
        }
    }
}

pub fn setup_parse_fixed_parameters<'p>(
    pstate: &mut ParseState<'p, '_>,
    param_types: &'p [Oid],
) {
    pstate.p_ref_hook_state = ParseRefHookState::FixedParams(FixedParamState { param_types });
}

pub fn setup_parse_variable_parameters(pstate: &mut ParseState<'_, '_>, parstate: VarParamState) {
    pstate.p_ref_hook_state = ParseRefHookState::VarParams(parstate);
}

pub fn setup_parse_sql_fn_parameters<'p>(
    pstate: &mut ParseState<'p, '_>,
    parstate: SqlFnParamState<'p>,
) {
    pstate.p_ref_hook_state = ParseRefHookState::SqlFnParams(parstate);
}

#[track_caller]
#[cold]
fn no_parameter_err(paramno: i32, errpos: i32, funcname: &'static str) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(alloc::format!("there is no parameter ${paramno}"))
            .errposition(errpos)
            .into_error()
            .with_error_location(loc(funcname)),
    )
}

pub fn fixed_paramref_hook<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    pref: &ParamRef,
    encoding: pg_enc,
) -> PgResult<Node<'mcx>> {
    let parstate = pstate
        .p_ref_hook_state
        .as_fixed_params()
        .expect("fixed_paramref_hook: p_ref_hook_state is not FixedParams");
    let paramno = pref.number;
    if paramno <= 0
        || paramno as usize > parstate.param_types.len()
        || !OidIsValid(parstate.param_types[(paramno - 1) as usize])
    {
        return Err(no_parameter_err(
            paramno,
            parser_errposition(pstate, pref.location, encoding),
            "fixed_paramref_hook",
        ));
    }
    let paramtype = parstate.param_types[(paramno - 1) as usize];
    mk_param(mcx, paramno, paramtype, pref.location)
}

pub fn sql_fn_paramref_hook<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    pref: &ParamRef,
    encoding: pg_enc,
) -> PgResult<Node<'mcx>> {
    let parstate = pstate
        .p_ref_hook_state
        .as_sql_fn_params()
        .expect("sql_fn_paramref_hook: p_ref_hook_state is not SqlFnParams");
    let paramno = pref.number;
    if paramno <= 0 || paramno as usize > parstate.argtypes.len() {
        return Err(no_parameter_err(
            paramno,
            parser_errposition(pstate, pref.location, encoding),
            "sql_fn_paramref_hook",
        ));
    }
    sql_fn_make_param(
        mcx,
        parstate,
        paramno,
        parstate.argtypes[(paramno - 1) as usize],
        pref.location,
    )
}

pub fn sql_fn_resolve_param_name(state: &SqlFnParamState<'_>, name: &str) -> Option<(i32, Oid)> {
    state
        .argnames
        .iter()
        .position(|n| !n.is_empty() && *n == name)
        .map(|i| (i as i32 + 1, state.argtypes[i]))
}

pub fn sql_fn_make_param<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SqlFnParamState<'_>,
    paramno: i32,
    paramtype: Oid,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let node = mk_param(mcx, paramno, paramtype, location)?;
    // A function input collation overrides the type-derived collation for
    // parameter symbols (functions.c sql_fn_make_param).
    if OidIsValid(state.input_collation) {
        let p = node.as_param().expect("just built");
        if OidIsValid(p.paramcollid) {
            // SAFETY: sole reference to the freshly built Param node.
            unsafe {
                node.with_mut::<Param, _>(|p| p.paramcollid = state.input_collation);
            }
        }
    }
    Ok(node)
}

pub fn variable_paramref_hook<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    pref: &ParamRef,
    encoding: pg_enc,
) -> PgResult<Node<'mcx>> {
    let parstate = pstate
        .p_ref_hook_state
        .as_var_params()
        .expect("variable_paramref_hook: p_ref_hook_state is not VarParams");
    let paramno = pref.number;
    if paramno <= 0 || paramno as usize > MAX_ALLOC_SIZE / core::mem::size_of::<Oid>() {
        return Err(no_parameter_err(
            paramno,
            parser_errposition(pstate, pref.location, encoding),
            "variable_paramref_hook",
        ));
    }

    let mut param_types = parstate.param_types.borrow_mut();
    // Growth zero-fills the new slots (palloc0_array/repalloc0_array;
    // InvalidOid == 0).
    if paramno as usize > param_types.len() {
        param_types.resize(paramno as usize, InvalidOid);
    }
    let idx = (paramno - 1) as usize;
    if param_types[idx] == InvalidOid {
        param_types[idx] = UNKNOWNOID;
    }
    // JDBC hack: a void argument of a CALL is interpreted as unknown (see
    // also ParseFuncOrColumn).
    if param_types[idx] == VOIDOID && pstate.p_expr_kind == ParseExprKind::EXPR_KIND_CALL_ARGUMENT
    {
        param_types[idx] = UNKNOWNOID;
    }
    let paramtype = param_types[idx];
    drop(param_types);

    mk_param(mcx, paramno, paramtype, pref.location)
}

/// plpgsql_param_ref (pl_exec.c): `$n` names the n-th function argument;
/// anything else is undefined_parameter (C looks the name "$n" up in a
/// namespace that holds only parameters).
pub fn plpgsql_paramref_hook<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    pref: &ParamRef,
    encoding: pg_enc,
) -> PgResult<Node<'mcx>> {
    let parstate = pstate
        .p_ref_hook_state
        .as_plpgsql_params()
        .expect("plpgsql_paramref_hook: p_ref_hook_state is not PlpgsqlParams");
    let paramno = pref.number;
    let dno = if paramno >= 1 && (paramno as usize) <= parstate.arg_dnos.len() {
        Some(parstate.arg_dnos[(paramno - 1) as usize])
    } else {
        None
    };
    let slot = dno.and_then(|d| parstate.params_by_dno.get(d as usize).copied().flatten());
    // C's hook returns NULL and the core reports undefined_parameter.
    let (Some(dno), Some((typoid, typmod, collation))) = (dno, slot) else {
        return Err(no_parameter_err(
            paramno,
            parser_errposition(pstate, pref.location, encoding),
            "plpgsql_param_ref",
        ));
    };
    parstate.mark_used(dno);
    Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: dno + 1,
            paramtype: typoid,
            paramtypmod: typmod,
            paramcollid: collation,
            location: pref.location,
        },
    )
}

/// resolve_column_ref (pl_exec.c) over the flattened name table. Returns the
/// Param for a match; `error_if_no_field` raises the record-has-no-field
/// error when the name's rec prefix is known but the field key is not.
pub fn plpgsql_resolve_column_ref<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    parstate: &PlpgsqlHookState<'_>,
    fields: &[&str],
    location: i32,
    error_if_no_field: bool,
    encoding: pg_enc,
) -> PgResult<Option<Node<'mcx>>> {
    if fields.is_empty() || fields.len() > 3 {
        return Ok(None);
    }
    let key = fields.join(".").to_ascii_lowercase();
    if fields.len() >= 2 {
        let prefix = fields[..fields.len() - 1].join(".").to_ascii_lowercase();
        if parstate.valueless_recs.iter().any(|r| *r == prefix) {
            let recname = fields[fields.len() - 2];
            // C's error comes from exec_get_datum_type_info (no cursor);
            // the SPI callback then supplies the statement context line.
            return Err(Box::new(
                ereport(ERROR)
                    .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(alloc::format!("record \"{recname}\" is not assigned yet"))
                    .errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")
                    .into_error()
                    .with_error_location(loc("resolve_column_ref")),
            ));
        }
    }
    if let Some(e) = parstate.names.iter().find(|e| e.key == key) {
        parstate.mark_used(e.dno);
        return Ok(Some(Node::mk(
            mcx,
            Param {
                paramkind: ParamKind::PARAM_EXTERN,
                paramid: e.dno + 1,
                paramtype: e.typoid,
                paramtypmod: e.typmod,
                paramcollid: e.collation,
                location,
            },
        )?));
    }
    if error_if_no_field && fields.len() >= 2 {
        // C reports against the last-1 prefix that named a rec/row.
        let prefix = fields[..fields.len() - 1].join(".").to_ascii_lowercase();
        if parstate.recs.iter().any(|r| *r == prefix) {
            let recname = fields[fields.len() - 2];
            let field = fields[fields.len() - 1];
            return Err(Box::new(
                ereport(ERROR)
                    .errcode(types_error::ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(alloc::format!(
                        "record \"{recname}\" has no field \"{field}\""
                    ))
                    .errposition(parser_errposition(pstate, location, encoding))
                    .into_error()
                    .with_error_location(loc("resolve_column_ref")),
            ));
        }
    }
    Ok(None)
}

fn mk_param<'mcx>(
    mcx: Mcx<'mcx>,
    paramno: i32,
    paramtype: Oid,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: paramno,
            paramtype,
            paramtypmod: -1,
            paramcollid: lsyscache::typ::get_typcollation(paramtype)?,
            location,
        },
    )
}

/// Returns true when the hook consumed the coercion (C returns the mutated
/// `Param *`), false to proceed with normal coercion (C returns NULL).
pub fn variable_coerce_param_hook(
    pstate: &ParseState<'_, '_>,
    param: &mut Param,
    target_type_id: Oid,
    _target_type_mod: i32,
    location: i32,
    encoding: pg_enc,
) -> PgResult<bool> {
    if !(param.paramkind == ParamKind::PARAM_EXTERN && param.paramtype == UNKNOWNOID) {
        return Ok(false);
    }
    let parstate = pstate
        .p_ref_hook_state
        .as_var_params()
        .expect("variable_coerce_param_hook: p_ref_hook_state is not VarParams");
    let paramno = param.paramid;
    let mut param_types = parstate.param_types.borrow_mut();
    if paramno <= 0 || paramno as usize > param_types.len() {
        drop(param_types);
        return Err(no_parameter_err(
            paramno,
            parser_errposition(pstate, param.location, encoding),
            "variable_coerce_param_hook",
        ));
    }
    let idx = (paramno - 1) as usize;
    if param_types[idx] == UNKNOWNOID {
        param_types[idx] = target_type_id;
    } else if param_types[idx] == target_type_id {
        // Previously resolved, and it matches.
    } else {
        let old = format_type::format_type_be(param_types[idx])?;
        let new = format_type::format_type_be(target_type_id)?;
        let errpos = parser_errposition(pstate, param.location, encoding);
        drop(param_types);
        return Err(Box::new(
            ereport(ERROR)
                .errcode(types_error::ERRCODE_AMBIGUOUS_PARAMETER)
                .errmsg(alloc::format!(
                    "inconsistent types deduced for parameter ${paramno}"
                ))
                .errdetail(alloc::format!("{old} versus {new}"))
                .errposition(errpos)
                .into_error()
                .with_error_location(loc("variable_coerce_param_hook")),
        ));
    }
    drop(param_types);

    param.paramtype = target_type_id;
    // paramtypmod stays -1 so a run-time length check/coercion occurs if
    // needed.
    param.paramtypmod = -1;
    param.paramcollid = lsyscache::typ::get_typcollation(param.paramtype)?;
    if location >= 0 && (param.location < 0 || location < param.location) {
        param.location = location;
    }
    Ok(true)
}

struct CheckParamResolution<'a, 'p, 'mcx> {
    pstate: &'a ParseState<'p, 'mcx>,
    encoding: pg_enc,
}

impl<'mcx> NodeWalker<'mcx> for CheckParamResolution<'_, '_, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(param) = node.as_param() {
            if param.paramkind == ParamKind::PARAM_EXTERN {
                let parstate = self
                    .pstate
                    .p_ref_hook_state
                    .as_var_params()
                    .expect("check_variable_parameters: p_ref_hook_state is not VarParams");
                let paramno = param.paramid;
                // Borrow released before returning: the errposition path calls
                // back into pstate helpers.
                let expected = {
                    let param_types = parstate.param_types.borrow();
                    if paramno <= 0 || paramno as usize > param_types.len() {
                        None
                    } else {
                        Some(param_types[(paramno - 1) as usize])
                    }
                };
                let Some(expected) = expected else {
                    return Err(no_parameter_err(
                        paramno,
                        parser_errposition(self.pstate, param.location, self.encoding),
                        "check_parameter_resolution_walker",
                    ));
                };
                if param.paramtype != expected {
                    return Err(Box::new(
                        ereport(ERROR)
                            .errcode(ERRCODE_AMBIGUOUS_PARAMETER)
                            .errmsg(alloc::format!(
                                "could not determine data type of parameter ${paramno}"
                            ))
                            .errposition(parser_errposition(
                                self.pstate,
                                param.location,
                                self.encoding,
                            ))
                            .into_error()
                            .with_error_location(loc("check_parameter_resolution_walker")),
                    ));
                }
            }
            return Ok(false);
        }
        if let Some(q) = node.as_query() {
            return query_tree_walker(q, self, 0);
        }
        expression_tree_walker(node, self)
    }

    // Recurse into RTE subqueries (C's IsA(node, Query) arm).
    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        query_tree_walker(q, self, 0)
    }
}

pub fn check_variable_parameters<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    query: &'mcx Query<'mcx>,
    encoding: pg_enc,
) -> PgResult<()> {
    let parstate = pstate
        .p_ref_hook_state
        .as_var_params()
        .expect("check_variable_parameters: p_ref_hook_state is not VarParams");
    // C: *parstate->numParams == 0 — no Params were generated.
    if parstate.param_types.borrow().is_empty() {
        return Ok(());
    }
    let mut cx = CheckParamResolution { pstate, encoding };
    query_tree_walker(query, &mut cx, 0)?;
    Ok(())
}

struct HasExternParam;

impl<'mcx> NodeWalker<'mcx> for HasExternParam {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(param) = node.as_param() {
            return Ok(param.paramkind == ParamKind::PARAM_EXTERN);
        }
        if let Some(q) = node.as_query() {
            return query_tree_walker(q, self, 0);
        }
        expression_tree_walker(node, self)
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        query_tree_walker(q, self, 0)
    }
}

pub fn query_contains_extern_params<'mcx>(query: &'mcx Query<'mcx>) -> PgResult<bool> {
    query_tree_walker(query, &mut HasExternParam, 0)
}
