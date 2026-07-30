// enum.c (+ hashenum/hashenumextended, C home hashfunc.c — no hash-AM adt
// crate yet, the int/int8 precedent).
#![allow(non_snake_case)]

pub mod builtins;

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::FnExprErased;
use types_core::{InvalidOid, Oid, NAMEDATALEN};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE, ERROR,
};
use types_fmgr::FmgrInfo;
use types_nodes::{Node, NodeTag};
use pg_enum_seams::EnumSortedRow;
use syscache_seams::PgEnumShape;

const TYPALIGN_INT: u8 = b'i';

// check_safe_enum_use (enum.c:62): uncommitted pg_enum rows must not reach
// SQL, or a rollback strands them in indexes.
fn check_safe_enum_use(
    oid: Oid,
    enumtypid: Oid,
    label: &[u8],
    xmin: types_core::TransactionId,
    xmin_committed: bool,
) -> PgResult<()> {
    if xmin_committed {
        return Ok(());
    }
    if !procarray_seams::transaction_id_is_in_progress::call(xmin)?
        && transam_seams::transaction_id_did_commit::call(xmin)?
    {
        return Ok(());
    }
    if !pg_enum_seams::enum_uncommitted::call(oid) {
        return Ok(());
    }
    Err(unsafe_new_value(label, enumtypid)?)
}

fn shape_safe(en: &PgEnumShape) -> PgResult<()> {
    check_safe_enum_use(en.oid, en.enumtypid, en.enumlabel.name_str(), en.xmin, en.xmin_committed)
}

fn row_safe(row: &EnumSortedRow) -> PgResult<()> {
    check_safe_enum_use(
        row.oid,
        row.enumtypid,
        row.enumlabel.name_str(),
        row.xmin,
        row.xmin_committed,
    )
}

pub fn enum_in(
    name: &str,
    enumtypoid: Oid,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    // Length gate mirrors C's SearchSysCache assert guard.
    if name.len() >= NAMEDATALEN as usize {
        return ereturn(escontext, None, *invalid_input(enumtypoid, name)?);
    }
    let Some(en) = syscache_seams::lookup_pg_enum_by_typid_label::call(enumtypoid, name)? else {
        return ereturn(escontext, None, *invalid_input(enumtypoid, name)?);
    };
    shape_safe(&en)?;
    Ok(Some(en.oid))
}

pub fn enum_out(enumval: Oid) -> PgResult<PgEnumShape> {
    match syscache_seams::lookup_pg_enum_by_oid::call(enumval)? {
        Some(en) => Ok(en),
        None => Err(invalid_internal(enumval)),
    }
}

// enum_cmp_internal (enum.c:251). fn_extra memoizes the resolved enum *type*
// OID (C caches the typcache pointer; same lookup shape, seam-safe carrier).
// C divergence: the tuplesort comparison shim calls flinfo-less (C's shim
// builds one), so a None flinfo skips memoization — the odd-OID fallback then
// pays an ENUMOID probe per comparison (cold; C Assert(flinfo) covered by the
// fmgr surface always passing one).
fn enum_cmp_internal(
    arg1: Oid,
    arg2: Oid,
    flinfo: Option<&mut FmgrInfo>,
) -> PgResult<i32> {
    if arg1 == arg2 {
        return Ok(0);
    }
    if (arg1 & 1) == 0 && (arg2 & 1) == 0 {
        return Ok(if arg1 < arg2 { -1 } else { 1 });
    }

    let typeoid = match flinfo.as_ref().and_then(|f| f.fn_extra_ref::<Oid>().copied()) {
        Some(t) => t,
        None => {
            let Some(en) = syscache_seams::lookup_pg_enum_by_oid::call(arg1)? else {
                return Err(invalid_internal(arg1));
            };
            if let Some(f) = flinfo {
                f.set_fn_extra(en.enumtypid);
            }
            en.enumtypid
        }
    };
    typcache_seams::compare_values_of_enum::call(typeoid, arg1, arg2)
}

// btree_gist's CallerFInfoFunctionCall2(enum_cmp, ...) surface: same
// engine, caller-owned flinfo carries the fn_extra type-OID memo.
pub fn enum_cmp_with_flinfo(
    arg1: Oid,
    arg2: Oid,
    flinfo: Option<&mut FmgrInfo>,
) -> PgResult<i32> {
    enum_cmp_internal(arg1, arg2, flinfo)
}

pub(crate) fn cmp_via(
    fcinfo: &types_fmgr::FunctionCallInfoBaseData,
    flinfo: Option<&mut FmgrInfo>,
) -> PgResult<i32> {
    let [a, b] = fcinfo.args_n::<2>();
    enum_cmp_internal(a.value.as_oid(), b.value.as_oid(), flinfo)
}

// get_fn_expr_argtype/get_call_expr_argtype (fmgr.c, unported unit) over the
// call families flinfo.fn_expr carries; InvalidOid mirrors the C NULL cases.
fn get_fn_expr_argtype(flinfo: Option<&FmgrInfo>, argnum: usize) -> Oid {
    let Some(expr) = flinfo.and_then(|f| f.fn_expr.as_ref()) else {
        return InvalidOid;
    };
    let node = fn_expr_node(expr);
    let args = match node.node_tag() {
        NodeTag::T_FuncExpr => &node.as_func_expr().expect("FuncExpr").args,
        NodeTag::T_OpExpr => &node.as_op_expr().expect("OpExpr").args,
        tag => panic!("get_fn_expr_argtype: call family {tag:?} not ported"),
    };
    match args.iter().nth(argnum) {
        Some(arg) => expr_type(arg),
        None => InvalidOid,
    }
}

fn fn_expr_node(expr: &FnExprErased) -> Node<'static> {
    *expr.downcast_ref::<Node<'static>>().expect("fn_expr does not carry a Node")
}

fn expr_type(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().expect("Var").vartype,
        NodeTag::T_Const => node.as_const().expect("Const").consttype,
        NodeTag::T_Param => node.as_param().expect("Param").paramtype,
        NodeTag::T_FuncExpr => node.as_func_expr().expect("FuncExpr").funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().expect("OpExpr").opresulttype,
        NodeTag::T_RelabelType => node.as_relabel_type().expect("RelabelType").resulttype,
        tag => panic!("adt_enum exprType: node family {tag:?} not ported"),
    }
}

fn enum_endpoint<'mcx>(mcx: Mcx<'mcx>, enumtypoid: Oid, backward: bool) -> PgResult<Oid> {
    let rows = pg_enum_seams::scan_enum_typid_sorted::call(mcx, enumtypoid, backward, true)?;
    match rows.first() {
        Some(row) => {
            row_safe(row)?;
            Ok(row.oid)
        }
        None => Ok(InvalidOid),
    }
}

pub fn enum_first_last<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: Option<&FmgrInfo>,
    backward: bool,
) -> PgResult<Oid> {
    let enumtypoid = get_fn_expr_argtype(flinfo, 0);
    if enumtypoid == InvalidOid {
        return Err(no_actual_enum_type());
    }
    let endpoint = enum_endpoint(mcx, enumtypoid, backward)?;
    if endpoint == InvalidOid {
        return Err(enum_contains_no_values(enumtypoid)?);
    }
    Ok(endpoint)
}

pub fn enum_range_internal<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    lower: Oid,
    upper: Oid,
) -> PgResult<Datum> {
    let rows = pg_enum_seams::scan_enum_typid_sorted::call(mcx, enumtypoid, false, false)?;
    let mut elems: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
    let mut left_found = lower == InvalidOid;
    for row in rows.iter() {
        if !left_found && lower == row.oid {
            left_found = true;
        }
        if left_found {
            row_safe(row)?;
            elems.push(Datum::from_oid(row.oid));
        }
        if upper != InvalidOid && upper == row.oid {
            break;
        }
    }
    let arr = arrayfuncs::construct::construct_array(
        mcx,
        &elems,
        enumtypoid,
        core::mem::size_of::<Oid>() as i32,
        true,
        TYPALIGN_INT,
    )?;
    let d = Datum::from_usize(arr.as_ptr() as usize);
    core::mem::forget(arr);
    Ok(d)
}

pub fn enum_range_typoid(flinfo: Option<&FmgrInfo>) -> PgResult<Oid> {
    let enumtypoid = get_fn_expr_argtype(flinfo, 0);
    if enumtypoid == InvalidOid {
        return Err(no_actual_enum_type());
    }
    Ok(enumtypoid)
}

pub fn init_seams() {}

#[cold]
#[inline(never)]
fn invalid_input(enumtypoid: Oid, name: &str) -> PgResult<Box<PgError>> {
    let ty = format_type::format_type_be(enumtypoid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("invalid input value for enum {ty}: \"{name}\""))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_internal(enumval: Oid) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("invalid internal value for enum: {enumval}"))
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
    )
}

#[cold]
#[inline(never)]
fn unsafe_new_value(label: &[u8], enumtypid: Oid) -> PgResult<Box<PgError>> {
    let ty = format_type::format_type_be(enumtypid)?;
    let label = core::str::from_utf8(label).unwrap_or("");
    Ok(Box::new(
        PgError::new(ERROR, format!("unsafe use of new value \"{label}\" of enum type {ty}"))
            .with_sqlstate(ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE)
            .with_hint("New enum values must be committed before they can be used."),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_actual_enum_type() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "could not determine actual enum type".to_string())
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[cold]
#[inline(never)]
fn enum_contains_no_values(enumtypoid: Oid) -> PgResult<Box<PgError>> {
    let ty = format_type::format_type_be(enumtypoid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("enum {ty} contains no values"))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    ))
}
