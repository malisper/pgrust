use ::adt_tsvector_core::builtins::arg_tsquery;
use ::adt_tsvector_core::layout::MAXENTRYPOS;
use ::adt_tsvector_core::query::*;
use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::io::*;
use crate::util::{qt2qtn, qtn2qt, QtNode};

fn image_result(img: PgVec<'_, u8>) -> Datum {
    varlena_result(::datum::Varlena::from_image(img))
}

fn copy_image<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut img = vec_with_capacity_in(mcx, q.payload.len() + 4)?;
    img.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut img, q.payload)?;
    Ok(img)
}

pub fn fc_tsqueryin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: input-function arg 0 is a live NUL-terminated cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: context, if set, is a live ErrorSaveNode for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    match tsquery_in_core(mcx, s, esc)? {
        Some(img) => Ok(image_result(img)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_tsqueryout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let q = arg_tsquery(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(cstring_result(tsquery_out_core(mcx, q)?))
}

pub fn fc_tsquerysend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let q = arg_tsquery(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(varlena_result(tsquery_send_core(mcx, q)?))
}

pub fn fc_tsqueryrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(image_result(tsquery_recv_core(mcx, buf)?))
}

pub fn fc_tsquerytree(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let q = arg_tsquery(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let txt = tsquerytree_core(mcx, q)?;
    Ok(varlena_result(::varlena::cstring_to_text(mcx, &txt)?))
}

pub fn fc_tsquery_numnode(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let q = arg_tsquery(fcinfo, 0)?;
    Ok(Datum::from_i32(q.size() as i32))
}

fn join_tsqueries<'mcx>(
    mcx: Mcx<'mcx>,
    a: TsQueryRef<'_>,
    b: TsQueryRef<'_>,
    oper: i8,
    distance: u16,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut children: PgVec<QtNode> = PgVec::new_in(mcx);
    children.try_reserve_exact(2).map_err(|_| mcx.oom(2))?;
    children.push(qt2qtn(mcx, b, 0)?);
    children.push(qt2qtn(mcx, a, 0)?);
    let res = QtNode {
        item: Item::Opr(Operator {
            oper,
            distance: if oper == OP_PHRASE { distance as i16 } else { 0 },
            left: 0,
        }),
        word: PgVec::new_in(mcx),
        sign: children[0].sign | children[1].sign,
        flags: 0,
        children,
    };
    qtn2qt(mcx, &res)
}

fn binop(fcinfo: &mut Fcinfo, oper: i8, distance: u16) -> PgResult<Datum> {
    let a = arg_tsquery(fcinfo, 0)?;
    let b = arg_tsquery(fcinfo, 1)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    if a.size() == 0 {
        return Ok(image_result(copy_image(mcx, b)?));
    } else if b.size() == 0 {
        return Ok(image_result(copy_image(mcx, a)?));
    }
    Ok(image_result(join_tsqueries(mcx, a, b, oper, distance)?))
}

pub fn fc_tsquery_and(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    binop(fcinfo, OP_AND, 0)
}

pub fn fc_tsquery_or(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    binop(fcinfo, OP_OR, 0)
}

pub fn fc_tsquery_phrase(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    binop(fcinfo, OP_PHRASE, 1)
}

pub fn fc_tsquery_phrase_distance(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let distance = fcinfo.arg_i32(2);
    if distance < 0 || distance > MAXENTRYPOS as i32 {
        return Err(PgError::error(format!(
            "distance in phrase operator must be an integer value between zero and {MAXENTRYPOS} inclusive"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    binop(fcinfo, OP_PHRASE, distance as u16)
}

pub fn fc_tsquery_not(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_tsquery(fcinfo, 0)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    if a.size() == 0 {
        return Ok(image_result(copy_image(mcx, a)?));
    }
    let mut children: PgVec<QtNode> = PgVec::new_in(mcx);
    children.try_reserve_exact(1).map_err(|_| mcx.oom(1))?;
    children.push(qt2qtn(mcx, a, 0)?);
    let res = QtNode {
        item: Item::Opr(Operator { oper: OP_NOT, distance: 0, left: 0 }),
        word: PgVec::new_in(mcx),
        sign: children[0].sign,
        flags: 0,
        children,
    };
    Ok(image_result(qtn2qt(mcx, &res)?))
}

macro_rules! fc_tsquery_cmp {
    ($($fc:ident: $conv:expr;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_tsquery(fcinfo, 0)?;
            let b = arg_tsquery(fcinfo, 1)?;
            // SAFETY: the armed result mcx outlives this call.
            let mcx = unsafe { fcinfo.result_mcx_detached() };
            let res = compare_tsq(a, b, mcx)?;
            #[allow(clippy::redundant_closure_call)]
            Ok(($conv)(res))
        }
    )*};
}

fc_tsquery_cmp! {
    fc_tsquery_lt: |r: i32| Datum::from_bool(r < 0);
    fc_tsquery_le: |r: i32| Datum::from_bool(r <= 0);
    fc_tsquery_eq: |r: i32| Datum::from_bool(r == 0);
    fc_tsquery_ne: |r: i32| Datum::from_bool(r != 0);
    fc_tsquery_ge: |r: i32| Datum::from_bool(r >= 0);
    fc_tsquery_gt: |r: i32| Datum::from_bool(r > 0);
    fc_tsquery_cmp: |r: i32| Datum::from_i32(r);
}

pub fn fc_tsq_mcontains(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let query = arg_tsquery(fcinfo, 0)?;
    let ex = arg_tsquery(fcinfo, 1)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(Datum::from_bool(tsq_mcontains_core(mcx, query, ex)?))
}

pub fn fc_tsq_mcontained(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ex = arg_tsquery(fcinfo, 0)?;
    let query = arg_tsquery(fcinfo, 1)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(Datum::from_bool(tsq_mcontains_core(mcx, query, ex)?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TSQUERY_BUILTINS: &[FmgrBuiltin] = &[
    b(3612, "tsqueryin", 1, fc_tsqueryin),
    b(3613, "tsqueryout", 1, fc_tsqueryout),
    b(3640, "tsquerysend", 1, fc_tsquerysend),
    b(3641, "tsqueryrecv", 1, fc_tsqueryrecv),
    b(3662, "tsquery_lt", 2, fc_tsquery_lt),
    b(3663, "tsquery_le", 2, fc_tsquery_le),
    b(3664, "tsquery_eq", 2, fc_tsquery_eq),
    b(3665, "tsquery_ne", 2, fc_tsquery_ne),
    b(3666, "tsquery_ge", 2, fc_tsquery_ge),
    b(3667, "tsquery_gt", 2, fc_tsquery_gt),
    b(3668, "tsquery_cmp", 2, fc_tsquery_cmp),
    b(3669, "tsquery_and", 2, fc_tsquery_and),
    b(3670, "tsquery_or", 2, fc_tsquery_or),
    b(3671, "tsquery_not", 1, fc_tsquery_not),
    b(3672, "tsquery_numnode", 1, fc_tsquery_numnode),
    b(3673, "tsquerytree", 1, fc_tsquerytree),
    b(3691, "tsq_mcontains", 2, fc_tsq_mcontains),
    b(3692, "tsq_mcontained", 2, fc_tsq_mcontained),
    b(5003, "tsquery_phrase", 2, fc_tsquery_phrase),
    b(5004, "tsquery_phrase_distance", 3, fc_tsquery_phrase_distance),
];
