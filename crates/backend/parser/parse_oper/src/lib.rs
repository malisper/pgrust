#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::mem::ManuallyDrop;

use cache_syscache::cacheinfo::{CASTSOURCETARGET, OPERNAMENSP};
use coerce::COERCION_IMPLICIT;
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap};
use parser_small1::{parser_errposition, ParseState};
use syscache_seams::PgOperatorShape;
use types_core::catalog::UNKNOWNOID;
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::{CoercionForm, Node, NodeList, OpExpr, OptNodeList, ScalarArrayOpExpr};

pub struct Operator {
    pub oid: Oid,
    pub shape: PgOperatorShape,
}

const NAMEDATALEN: usize = 64;
const MAX_CACHED_PATH_LEN: usize = 16;

// Zero-filled unused bytes keep hashing stable (C's MemSet'd OprCacheKey).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct OprCacheKey {
    oprname: [u8; NAMEDATALEN],
    left_arg: Oid,
    right_arg: Oid,
    search_path: [Oid; MAX_CACHED_PATH_LEN],
}

struct OprCache {
    map: PgHashMap<'static, OprCacheKey, Oid>,
}

thread_local! {
    static OPR_CACHE: RefCell<Option<ManuallyDrop<OprCache>>> = const { RefCell::new(None) };
}

fn with_opr_cache<R>(f: impl FnOnce(&mut PgHashMap<'static, OprCacheKey, Oid>) -> R) -> PgResult<R> {
    OPR_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            // The context is leaked (backend-lifetime table, C: hash_create in
            // TopMemoryContext); flush on pg_operator and pg_cast changes.
            let mcx = ::mcx::session_root("Operator lookup cache").mcx();
            inval::invalidate::CacheRegisterSyscacheCallback(
                OPERNAMENSP,
                InvalidateOprCacheCallBack,
                Datum::null(),
            )?;
            inval::invalidate::CacheRegisterSyscacheCallback(
                CASTSOURCETARGET,
                InvalidateOprCacheCallBack,
                Datum::null(),
            )?;
            *slot = Some(ManuallyDrop::new(OprCache {
                map: PgHashMap::with_capacity_in(256, mcx),
            }));
        }
        Ok(f(&mut slot.as_mut().unwrap().map))
    })
}

fn InvalidateOprCacheCallBack(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    OPR_CACHE.with(|cell| {
        if let Some(cache) = cell.borrow_mut().as_mut() {
            cache.map.clear();
        }
    });
}

fn name_parts<'a, 'mcx>(opname: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = opname.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = opname.nth(i).as_string().expect("operator name list holds String nodes").sval;
    }
    &buf[..n]
}

// LookupOperName (parse_oper.c) with pstate=NULL, location=-1: exact-match
// lookup for DDL callers; no implicit-coercion resolution.
pub fn LookupOperName(
    opername: &NodeList<'_>,
    oprleft: Oid,
    oprright: Oid,
    noError: bool,
) -> PgResult<Oid> {
    let mut buf = [""; 4];
    let parts = name_parts(opername, &mut buf);
    let result = catalog_namespace::OpernameGetOprid(parts, oprleft, oprright)?;
    if OidIsValid(result) {
        return Ok(result);
    }
    if !noError {
        if !OidIsValid(oprright) {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("postfix operators are not supported".to_string())
                    .into_error(),
            ));
        }
        let sig = op_signature_string(parts, oprleft, oprright)?;
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!("operator does not exist: {sig}"))
                .into_error(),
        ));
    }
    Ok(InvalidOid)
}

// LookupOperWithArgs (parse_oper.c): exactly two objargs cells; a None cell
// is the oper_argtypes NONE side of a unary operator (InvalidOid side).
pub fn LookupOperWithArgs(
    oper_name: &NodeList<'_>,
    oper_args: &OptNodeList<'_>,
    noError: bool,
) -> PgResult<Oid> {
    assert_eq!(oper_args.len(), 2, "LookupOperWithArgs: objargs must have 2 entries");
    let scratch = mcx::MemoryContext::new("LookupOperWithArgs");
    let mut oids = [InvalidOid; 2];
    for (i, n) in oper_args.iter().enumerate() {
        let Some(n) = n else { continue };
        let t = n
            .as_variant::<types_nodes::rawnodes::TypeName>()
            .expect("oper_argtypes holds TypeName nodes");
        oids[i] = parse_utilcmd::LookupTypeNameOidExtended(scratch.mcx(), t, noError)?;
    }
    LookupOperName(oper_name, oids[0], oids[1], noError)
}

fn make_oper_cache_key(
    key: &mut OprCacheKey,
    parts: &[&str],
    ltypeId: Oid,
    rtypeId: Oid,
) -> PgResult<bool> {
    let (schemaname, opername) = catalog_namespace::DeconstructQualifiedName(parts)?;

    let n = opername.len().min(NAMEDATALEN - 1);
    key.oprname[..n].copy_from_slice(&opername.as_bytes()[..n]);
    key.left_arg = ltypeId;
    key.right_arg = rtypeId;

    if let Some(schemaname) = schemaname {
        key.search_path[0] = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
    } else if catalog_namespace::fetch_search_path_array(&mut key.search_path)?
        > MAX_CACHED_PATH_LEN
    {
        return Ok(false);
    }
    Ok(true)
}

fn binary_oper_exact(parts: &[&str], arg1: Oid, arg2: Oid) -> PgResult<Oid> {
    let mut was_unknown = false;
    let (mut arg1, mut arg2) = (arg1, arg2);
    if arg1 == UNKNOWNOID && arg2 != InvalidOid {
        arg1 = arg2;
        was_unknown = true;
    } else if arg2 == UNKNOWNOID && arg1 != InvalidOid {
        arg2 = arg1;
        was_unknown = true;
    }

    let result = catalog_namespace::OpernameGetOprid(parts, arg1, arg2)?;
    if OidIsValid(result) {
        return Ok(result);
    }

    if was_unknown {
        let basetype = lsyscache::getBaseType(arg1)?;
        if basetype != arg1 {
            let result = catalog_namespace::OpernameGetOprid(parts, basetype, basetype)?;
            if OidIsValid(result) {
                return Ok(result);
            }
        }
    }
    Ok(InvalidOid)
}

pub fn oper(
    pstate: &ParseState<'_, '_>,
    opname: &NodeList<'_>,
    ltypeId: Oid,
    rtypeId: Oid,
    noError: bool,
    location: ParseLoc,
) -> PgResult<Option<Operator>> {
    let mut buf = [""; 4];
    let parts = name_parts(opname, &mut buf);
    let mut ltypeId = ltypeId;
    let mut rtypeId = rtypeId;

    let mut key = OprCacheKey {
        oprname: [0; NAMEDATALEN],
        left_arg: InvalidOid,
        right_arg: InvalidOid,
        search_path: [InvalidOid; MAX_CACHED_PATH_LEN],
    };
    let key_ok = make_oper_cache_key(&mut key, parts, ltypeId, rtypeId)?;

    if key_ok {
        let cached = with_opr_cache(|map| map.get(&key).copied().unwrap_or(InvalidOid))?;
        if OidIsValid(cached) {
            if let Some(shape) = syscache_seams::lookup_pg_operator_shape::call(cached)? {
                return Ok(Some(Operator { oid: cached, shape }));
            }
        }
    }

    let mut fd_multiple = false;
    let mut operOid = binary_oper_exact(parts, ltypeId, rtypeId)?;
    if !OidIsValid(operOid) {
        // Per-call scratch: cold path behind the OprCache memo (C pallocs the
        // candidate space per call too).
        let scratch = MemoryContext::new("oper candidates");
        let clist =
            catalog_namespace::OpernameGetCandidates(scratch.mcx(), parts, b'b' as i8, false)?;
        if !clist.is_empty() {
            if rtypeId == InvalidOid {
                rtypeId = ltypeId;
            } else if ltypeId == InvalidOid {
                ltypeId = rtypeId;
            }
            let input_typeids = [ltypeId, rtypeId];
            // oper_select_candidate (parse_oper.c).
            let matched =
                parse_func::func_match_argtypes(scratch.mcx(), &input_typeids, clist.as_slice())?;
            operOid = match matched.len() {
                0 => InvalidOid,
                1 => matched[0].oid,
                _ => match parse_func::func_select_candidate(&input_typeids, matched)? {
                    Some(c) => c.oid,
                    None => {
                        fd_multiple = true;
                        InvalidOid
                    }
                },
            };
        }
    }

    let shape =
        if OidIsValid(operOid) { syscache_seams::lookup_pg_operator_shape::call(operOid)? } else { None };
    match shape {
        Some(shape) => {
            if key_ok {
                with_opr_cache(|map| map.insert(key, operOid))?;
            }
            Ok(Some(Operator { oid: operOid, shape }))
        }
        None if noError => Ok(None),
        None => Err(op_error(pstate, parts, ltypeId, rtypeId, fd_multiple, location)),
    }
}

pub fn left_oper(
    pstate: &ParseState<'_, '_>,
    opname: &NodeList<'_>,
    arg: Oid,
    noError: bool,
    location: ParseLoc,
) -> PgResult<Option<Operator>> {
    let mut buf = [""; 4];
    let parts = name_parts(opname, &mut buf);

    let mut key = OprCacheKey {
        oprname: [0; NAMEDATALEN],
        left_arg: InvalidOid,
        right_arg: InvalidOid,
        search_path: [InvalidOid; MAX_CACHED_PATH_LEN],
    };
    let key_ok = make_oper_cache_key(&mut key, parts, InvalidOid, arg)?;

    if key_ok {
        let cached = with_opr_cache(|map| map.get(&key).copied().unwrap_or(InvalidOid))?;
        if OidIsValid(cached) {
            if let Some(shape) = syscache_seams::lookup_pg_operator_shape::call(cached)? {
                return Ok(Some(Operator { oid: cached, shape }));
            }
        }
    }

    let mut fd_multiple = false;
    let mut operOid = catalog_namespace::OpernameGetOprid(parts, InvalidOid, arg)?;
    if !OidIsValid(operOid) {
        let scratch = MemoryContext::new("oper candidates");
        let mut clist =
            catalog_namespace::OpernameGetCandidates(scratch.mcx(), parts, b'l' as i8, false)?;
        if !clist.is_empty() {
            // candidates carry (0, oprright); move the data into args[0] so
            // the 1-arg selection walk stays simple (C does the same shift)
            for c in clist.iter_mut() {
                c.args[0] = c.args[1];
            }
            let input_typeids = [arg];
            let matched =
                parse_func::func_match_argtypes(scratch.mcx(), &input_typeids, clist.as_slice())?;
            operOid = match matched.len() {
                0 => InvalidOid,
                1 => matched[0].oid,
                _ => match parse_func::func_select_candidate(&input_typeids, matched)? {
                    Some(c) => c.oid,
                    None => {
                        fd_multiple = true;
                        InvalidOid
                    }
                },
            };
        }
    }

    let shape = if OidIsValid(operOid) {
        syscache_seams::lookup_pg_operator_shape::call(operOid)?
    } else {
        None
    };
    match shape {
        Some(shape) => {
            if key_ok {
                with_opr_cache(|map| map.insert(key, operOid))?;
            }
            Ok(Some(Operator { oid: operOid, shape }))
        }
        None if noError => Ok(None),
        None => Err(op_error(pstate, parts, InvalidOid, arg, fd_multiple, location)),
    }
}

fn compatible_oper(
    pstate: &ParseState<'_, '_>,
    opname: &NodeList<'_>,
    arg1: Oid,
    arg2: Oid,
    noError: bool,
    location: ParseLoc,
) -> PgResult<Option<Operator>> {
    let Some(optup) = oper(pstate, opname, arg1, arg2, noError, location)? else {
        return Ok(None);
    };
    if coerce::IsBinaryCoercible(arg1, optup.shape.oprleft)?
        && coerce::IsBinaryCoercible(arg2, optup.shape.oprright)?
    {
        return Ok(Some(optup));
    }
    if !noError {
        let mut buf = [""; 4];
        let parts = name_parts(opname, &mut buf);
        return Err(coercion_error(pstate, parts, arg1, arg2, location));
    }
    Ok(None)
}

/// C `compatible_oper_opid`; C passes a NULL pstate and location -1, so
/// errors carry no position (callers attach one).
pub fn compatible_oper_opid(
    pstate: &ParseState<'_, '_>,
    opname: &NodeList<'_>,
    arg1: Oid,
    arg2: Oid,
    noError: bool,
) -> PgResult<Oid> {
    Ok(match compatible_oper(pstate, opname, arg1, arg2, noError, -1)? {
        Some(op) => op.oid,
        None => InvalidOid,
    })
}

#[track_caller]
#[cold]
#[inline(never)]
fn coercion_error(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    arg1: Oid,
    arg2: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    let sig = match op_signature_string(parts, arg1, arg2) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("operator requires run-time type coercion: {sig}"))
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "compatible_oper")),
    )
}

/// C `make_op`, with the operand types precomputed by the caller (exprType's
/// closed-set slice lives in parse_expr pending backend-nodes-core).
#[allow(clippy::too_many_arguments)]
pub fn make_op<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    opname: &NodeList<'mcx>,
    ltree: Option<Node<'mcx>>,
    rtree: Option<Node<'mcx>>,
    ltypeId: Oid,
    rtypeId: Oid,
    last_srf: Option<Node<'mcx>>,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    let Some(rtree) = rtree else {
        return Err(postfix_error());
    };

    let op = match ltree {
        Some(_) => oper(pstate, opname, ltypeId, rtypeId, false, location)?,
        None => left_oper(pstate, opname, rtypeId, false, location)?,
    }
    .expect("oper(noError=false) always returns an operator");

    if !OidIsValid(op.shape.oprcode) {
        let mut buf = [""; 4];
        let parts = name_parts(opname, &mut buf);
        return Err(shell_error(pstate, parts, &op, location));
    }

    let nargs = if ltree.is_some() { 2 } else { 1 };
    let actual_arg_types =
        if ltree.is_some() { [ltypeId, rtypeId] } else { [rtypeId, InvalidOid] };
    let mut declared_arg_types = if ltree.is_some() {
        [op.shape.oprleft, op.shape.oprright]
    } else {
        [op.shape.oprright, InvalidOid]
    };

    let rettype = coerce::enforce_generic_type_consistency(
        &actual_arg_types[..nargs],
        &mut declared_arg_types[..nargs],
        op.shape.oprresult,
        false,
    )?;

    // make_fn_arguments (parse_func.c) hosted here until backend-parser-func.
    let ltree = match ltree {
        Some(l) => Some(coerce_arg(mcx, pstate, l, actual_arg_types[0], declared_arg_types[0])?),
        None => None,
    };
    let (r_actual, r_declared) = if ltree.is_some() {
        (actual_arg_types[1], declared_arg_types[1])
    } else {
        (actual_arg_types[0], declared_arg_types[0])
    };
    let rtree = coerce_arg(mcx, pstate, rtree, r_actual, r_declared)?;

    let opretset = lsyscache::get_func_retset(op.shape.oprcode)?;
    if opretset {
        parse_func::check_srf_call_placement(pstate, last_srf, location)?;
    }

    let args = match ltree {
        Some(l) => NodeList::make2(mcx, l, rtree)?,
        None => NodeList::make1(mcx, rtree)?,
    };
    let result = Node::mk(
        mcx,
        OpExpr {
            opno: op.oid,
            opfuncid: op.shape.oprcode,
            opresulttype: rettype,
            opretset,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args,
            location,
        },
    )?;
    if opretset {
        pstate.p_last_srf = Some(result);
    }
    Ok(result)
}

fn coerce_arg<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    actual: Oid,
    declared: Oid,
) -> PgResult<Node<'mcx>> {
    if actual == declared {
        return Ok(node);
    }
    coerce::coerce_type(
        mcx,
        pstate,
        node,
        actual,
        declared,
        -1,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )
}

/// C `make_scalar_array_op`, operand types precomputed by the caller as in
/// [`make_op`].
#[allow(clippy::too_many_arguments)]
pub fn make_scalar_array_op<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    opname: &NodeList<'mcx>,
    useOr: bool,
    ltree: Node<'mcx>,
    rtree: Node<'mcx>,
    ltypeId: Oid,
    atypeId: Oid,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    let rtypeId = if atypeId == UNKNOWNOID {
        UNKNOWNOID
    } else {
        let elem = lsyscache::get_base_element_type(atypeId)?;
        if !OidIsValid(elem) {
            return Err(saop_error(
                pstate,
                "op ANY/ALL (array) requires array on right side",
                location,
            ));
        }
        elem
    };

    let op = oper(pstate, opname, ltypeId, rtypeId, false, location)?
        .expect("oper(noError=false) always returns an operator");

    if !OidIsValid(op.shape.oprcode) {
        let mut buf = [""; 4];
        let parts = name_parts(opname, &mut buf);
        return Err(shell_error(pstate, parts, &op, location));
    }

    let actual_arg_types = [ltypeId, rtypeId];
    let mut declared_arg_types = [op.shape.oprleft, op.shape.oprright];

    let rettype = coerce::enforce_generic_type_consistency(
        &actual_arg_types,
        &mut declared_arg_types,
        op.shape.oprresult,
        false,
    )?;

    if rettype != types_core::catalog::BOOLOID {
        return Err(saop_error(
            pstate,
            "op ANY/ALL (array) requires operator to yield boolean",
            location,
        ));
    }
    if lsyscache::get_func_retset(op.shape.oprcode)? {
        return Err(saop_error(
            pstate,
            "op ANY/ALL (array) requires operator not to return a set",
            location,
        ));
    }

    let res_atypeId = if coerce::IsPolymorphicType(declared_arg_types[1]) {
        atypeId
    } else {
        let arr = lsyscache::get_array_type(declared_arg_types[1])?;
        if !OidIsValid(arr) {
            return Err(no_array_type_error(pstate, declared_arg_types[1], location));
        }
        arr
    };

    let ltree = coerce_arg(mcx, pstate, ltree, ltypeId, declared_arg_types[0])?;
    let rtree = coerce_arg(mcx, pstate, rtree, atypeId, res_atypeId)?;

    Node::mk(
        mcx,
        ScalarArrayOpExpr {
            opno: op.oid,
            opfuncid: op.shape.oprcode,
            hashfuncid: InvalidOid,
            negfuncid: InvalidOid,
            useOr,
            inputcollid: InvalidOid,
            args: NodeList::make2(mcx, ltree, rtree)?,
            location,
        },
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn saop_error(pstate: &ParseState<'_, '_>, msg: &str, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(msg.to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_scalar_array_op")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_array_type_error(
    pstate: &ParseState<'_, '_>,
    elemtype: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    let tname = match format_type::format_type_be(elemtype) {
        Ok(t) => t,
        Err(e) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("could not find array type for data type {tname}"))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_scalar_array_op")),
    )
}

#[derive(Debug)]
pub struct SortGroupOperators {
    pub lt_opr: Oid,
    pub eq_opr: Oid,
    pub gt_opr: Oid,
    pub hashable: bool,
}

/// C `get_sort_group_operators`; missing-operator errors carry no position —
/// callers attach the parser errposition (C's callback pattern).
pub fn get_sort_group_operators(
    argtype: Oid,
    need_lt: bool,
    need_eq: bool,
    need_gt: bool,
    want_hashable: bool,
) -> PgResult<SortGroupOperators> {
    let mut cache_flags =
        typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_EQ_OPR | typcache::TYPECACHE_GT_OPR;
    if want_hashable {
        cache_flags |= typcache::TYPECACHE_HASH_PROC;
    }

    let typentry = typcache::lookup_type_cache(argtype, cache_flags)?;
    let ops = SortGroupOperators {
        lt_opr: typentry.lt_opr(),
        eq_opr: typentry.eq_opr(),
        gt_opr: typentry.gt_opr(),
        hashable: OidIsValid(typentry.hash_proc()),
    };

    if (need_lt && !OidIsValid(ops.lt_opr)) || (need_gt && !OidIsValid(ops.gt_opr)) {
        return Err(missing_op_error(argtype, true));
    }
    if need_eq && !OidIsValid(ops.eq_opr) {
        return Err(missing_op_error(argtype, false));
    }
    Ok(ops)
}

#[track_caller]
#[cold]
#[inline(never)]
fn missing_op_error(argtype: Oid, ordering: bool) -> Box<PgError> {
    let tname = match format_type::format_type_be(argtype) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let mut b = elog::ereport(ERROR).errcode(ERRCODE_UNDEFINED_FUNCTION);
    if ordering {
        b = b
            .errmsg(format!("could not identify an ordering operator for type {tname}"))
            .errhint("Use an explicit ordering operator or modify the query.".to_string());
    } else {
        b = b.errmsg(format!("could not identify an equality operator for type {tname}"));
    }
    Box::new(
        b.into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "get_sort_group_operators")),
    )
}

pub fn op_signature_string(parts: &[&str], arg1: Oid, arg2: Oid) -> PgResult<String> {
    let opname = parts.join(".");
    Ok(if OidIsValid(arg1) {
        format!(
            "{} {opname} {}",
            format_type::format_type_be(arg1)?,
            format_type::format_type_be(arg2)?
        )
    } else {
        format!("{opname} {}", format_type::format_type_be(arg2)?)
    })
}

#[track_caller]
#[cold]
#[inline(never)]
fn op_error(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    arg1: Oid,
    arg2: Oid,
    fd_multiple: bool,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    let sig = match op_signature_string(parts, arg1, arg2) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    if fd_multiple {
        return Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                .errmsg(format!("operator is not unique: {sig}"))
                .errhint(
                    "Could not choose a best candidate operator. You might need to add \
                     explicit type casts."
                        .to_string(),
                )
                .errposition(parser_errposition(pstate, location, encoding))
                .into_error()
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "op_error")),
        );
    }
    let hint = if !OidIsValid(arg1) || !OidIsValid(arg2) {
        "No operator matches the given name and argument type. \
         You might need to add an explicit type cast."
    } else {
        "No operator matches the given name and argument types. \
         You might need to add explicit type casts."
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("operator does not exist: {sig}"))
            .errhint(hint.to_string())
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "op_error")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn shell_error(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    op: &Operator,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    let sig = match op_signature_string(parts, op.shape.oprleft, op.shape.oprright) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("operator is only a shell: {sig}"))
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_op")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn postfix_error() -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("postfix operators are not supported".to_string())
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_op")),
    )
}
