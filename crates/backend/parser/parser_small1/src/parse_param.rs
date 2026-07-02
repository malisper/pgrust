use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;

use elog::ereport;
use mcx::{Mcx, MAX_ALLOC_SIZE};
use types_core::catalog::{UNKNOWNOID, VOIDOID};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_UNDEFINED_PARAMETER, ERROR};
use types_nodes::{Node, Param, ParamKind, ParamRef};
use wchar::pg_enc;

use crate::parse_node::{parser_errposition, ParseExprKind, ParseState};

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("parse_param.c", 0, funcname)
}

#[derive(Clone, Copy)]
pub struct FixedParamState<'p> {
    pub param_types: &'p [Oid],
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

/// C selects param hooks by installing fn pointers alongside a `void *`
/// `p_ref_hook_state`; the closed arm set is the dispatch here (rule 4).
#[derive(Clone, Default)]
pub enum ParseRefHookState<'p> {
    #[default]
    None,
    FixedParams(FixedParamState<'p>),
    VarParams(VarParamState),
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
        panic!(
            "variable_coerce_param_hook (parse_param.c): inconsistent types deduced for \
             parameter ${paramno}; the errdetail needs format_type_be (adt-format-type \
             unported; direct dep when it lands)"
        );
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

pub fn check_variable_parameters() -> ! {
    panic!(
        "check_variable_parameters (parse_param.c): needs query_tree_walker/\
         expression_tree_walker (backend-nodes nodeFuncs.c unported)"
    )
}

pub fn query_contains_extern_params() -> ! {
    panic!(
        "query_contains_extern_params (parse_param.c): needs query_tree_walker/\
         expression_tree_walker (backend-nodes nodeFuncs.c unported)"
    )
}
