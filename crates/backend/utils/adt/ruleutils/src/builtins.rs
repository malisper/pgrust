use datum::Datum;
use mcx::MemoryContext;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::{get_pretty_flags, PRETTYFLAG_INDENT};

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: result needs a resolved FmgrInfo's scratch")
}

// C pallocs each result per call; the resolved FmgrInfo owns retained scratch
// (varlena builtins precedent). The Datum aliases it until the next call
// through the same FmgrInfo.
struct OutBuf(Vec<u8>);

fn out_scratch<'a>(flinfo: Option<&'a mut FmgrInfo>, name: &'static str) -> &'a mut Vec<u8> {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0
}

fn text_result(flinfo: Option<&mut FmgrInfo>, name: &'static str, s: &str) -> Datum {
    let buf = out_scratch(flinfo, name);
    buf.clear();
    buf.reserve(datum::varlena::VARHDRSZ + s.len());
    buf.extend_from_slice(&datum::varlena::set_varsize_4b(datum::varlena::VARHDRSZ + s.len()));
    buf.extend_from_slice(s.as_bytes());
    Datum::from_usize(buf.as_ptr() as usize)
}

pub fn fc_pg_get_userbyid(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = crate::pg_get_userbyid_core(fcinfo.arg_oid(0))?;
    let buf = out_scratch(flinfo, "pg_get_userbyid");
    buf.clear();
    buf.extend_from_slice(&name.data);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

fn indexdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    colno: i32,
    pretty_flags: i32,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_indexdef");
    let res = crate::pg_get_indexdef_worker(
        ctx.mcx(),
        fcinfo.arg_oid(0),
        colno,
        None,
        colno != 0,
        false,
        false,
        false,
        pretty_flags,
        true,
    )?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_indexdef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_indexdef(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    indexdef(flinfo, fcinfo, 0, PRETTYFLAG_INDENT)
}

pub fn fc_pg_get_indexdef_ext(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let colno = fcinfo.arg_i32(1);
    let pretty = fcinfo.arg_bool(2);
    indexdef(flinfo, fcinfo, colno, get_pretty_flags(pretty))
}

fn constraintdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    pretty_flags: i32,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_constraintdef");
    let res =
        crate::pg_get_constraintdef_worker(ctx.mcx(), fcinfo.arg_oid(0), pretty_flags, true)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_constraintdef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_constraintdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    constraintdef(flinfo, fcinfo, PRETTYFLAG_INDENT)
}

pub fn fc_pg_get_constraintdef_ext(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let pretty = fcinfo.arg_bool(1);
    constraintdef(flinfo, fcinfo, get_pretty_flags(pretty))
}

fn expr(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo, pretty_flags: i32) -> PgResult<Datum> {
    // SAFETY: arg 0 of strict pg_get_expr is a non-null pg_node_tree (text).
    let raw = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let text = core::str::from_utf8(raw.data()).expect("non-UTF-8 pg_node_tree").to_owned();
    let relid = fcinfo.arg_oid(1);
    let ctx = MemoryContext::new("pg_get_expr");
    let res = crate::pg_get_expr_worker(ctx.mcx(), &text, relid, pretty_flags)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_expr", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_expr(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    expr(flinfo, fcinfo, PRETTYFLAG_INDENT)
}

pub fn fc_pg_get_expr_ext(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let pretty = fcinfo.arg_bool(2);
    expr(flinfo, fcinfo, get_pretty_flags(pretty))
}

pub fn fc_pg_get_partkeydef(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_partkeydef");
    let res = crate::pg_get_partkeydef_worker(
        ctx.mcx(),
        fcinfo.arg_oid(0),
        PRETTYFLAG_INDENT,
        false,
        true,
    )?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_partkeydef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_function_arg_default(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let funcid = fcinfo.arg_oid(0);
    let nth_arg = fcinfo.arg_i32(1);
    let ctx = MemoryContext::new("pg_get_function_arg_default");
    let res = crate::pg_get_function_arg_default_worker(ctx.mcx(), funcid, nth_arg)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_function_arg_default", &s),
        None => fcinfo.return_null(),
    })
}

fn statisticsobjdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    columns_only: bool,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_statisticsobjdef");
    let res =
        crate::pg_get_statisticsobj_worker(ctx.mcx(), fcinfo.arg_oid(0), columns_only, true)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_statisticsobjdef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_statisticsobjdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    statisticsobjdef(flinfo, fcinfo, false)
}

pub fn fc_pg_get_statisticsobjdef_columns(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    statisticsobjdef(flinfo, fcinfo, true)
}

fn viewdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    viewoid: Oid,
    pretty_flags: i32,
    wrap_column: i32,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_viewdef");
    let res = crate::pg_get_viewdef_worker(ctx.mcx(), viewoid, pretty_flags, wrap_column)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_viewdef", &s),
        None => fcinfo.return_null(),
    })
}

fn viewdef_name_arg(fcinfo: &mut Fcinfo) -> PgResult<Oid> {
    // SAFETY: arg 0 of the strict by-name pg_get_viewdef forms is text.
    let raw = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let name = core::str::from_utf8(raw.data()).expect("non-UTF-8 view name").to_owned();
    let ctx = MemoryContext::new("pg_get_viewdef_name");
    crate::viewdef::view_name_to_oid(ctx.mcx(), &name)
}

pub fn fc_pg_get_viewdef(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let viewoid = fcinfo.arg_oid(0);
    viewdef(flinfo, fcinfo, viewoid, PRETTYFLAG_INDENT, crate::viewdef::WRAP_COLUMN_DEFAULT)
}

pub fn fc_pg_get_viewdef_ext(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let viewoid = fcinfo.arg_oid(0);
    let pretty = fcinfo.arg_bool(1);
    viewdef(flinfo, fcinfo, viewoid, get_pretty_flags(pretty), crate::viewdef::WRAP_COLUMN_DEFAULT)
}

pub fn fc_pg_get_viewdef_wrap(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let viewoid = fcinfo.arg_oid(0);
    let wrap = fcinfo.arg_i32(1);
    viewdef(flinfo, fcinfo, viewoid, get_pretty_flags(true), wrap)
}

pub fn fc_pg_get_viewdef_name(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let viewoid = viewdef_name_arg(fcinfo)?;
    viewdef(flinfo, fcinfo, viewoid, PRETTYFLAG_INDENT, crate::viewdef::WRAP_COLUMN_DEFAULT)
}

pub fn fc_pg_get_viewdef_name_ext(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let viewoid = viewdef_name_arg(fcinfo)?;
    let pretty = fcinfo.arg_bool(1);
    viewdef(flinfo, fcinfo, viewoid, get_pretty_flags(pretty), crate::viewdef::WRAP_COLUMN_DEFAULT)
}

fn ruledef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    pretty_flags: i32,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_ruledef");
    let res = crate::pg_get_ruledef_worker(ctx.mcx(), fcinfo.arg_oid(0), pretty_flags)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_ruledef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_ruledef(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ruledef(flinfo, fcinfo, PRETTYFLAG_INDENT)
}

pub fn fc_pg_get_ruledef_ext(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let pretty = fcinfo.arg_bool(1);
    ruledef(flinfo, fcinfo, get_pretty_flags(pretty))
}

fn triggerdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    pretty: bool,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_triggerdef");
    let res = crate::pg_get_triggerdef_worker(ctx.mcx(), fcinfo.arg_oid(0), pretty)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_triggerdef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_triggerdef(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    triggerdef(flinfo, fcinfo, false)
}

pub fn fc_pg_get_triggerdef_ext(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let pretty = fcinfo.arg_bool(1);
    triggerdef(flinfo, fcinfo, pretty)
}

pub fn fc_pg_get_functiondef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_functiondef");
    let res = crate::pg_get_functiondef_worker(ctx.mcx(), fcinfo.arg_oid(0))?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_functiondef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_function_arguments(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_function_arguments");
    let res = crate::pg_get_function_arguments_worker(ctx.mcx(), fcinfo.arg_oid(0))?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_function_arguments", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_function_identity_arguments(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_function_identity_arguments");
    let res = crate::pg_get_function_identity_arguments_worker(ctx.mcx(), fcinfo.arg_oid(0))?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_function_identity_arguments", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_function_result(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_function_result");
    let res = crate::pg_get_function_result_worker(ctx.mcx(), fcinfo.arg_oid(0))?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_function_result", &s),
        None => fcinfo.return_null(),
    })
}

fn text_arg(fcinfo: &mut Fcinfo, argno: usize, what: &str) -> PgResult<String> {
    // SAFETY: strict builtin, text argument.
    let raw = unsafe { fcinfo.arg_varlena_packed(argno) }?;
    Ok(core::str::from_utf8(raw.data())
        .unwrap_or_else(|_| panic!("non-UTF-8 {what}"))
        .to_owned())
}

pub fn fc_pg_get_serial_sequence(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let tablename = text_arg(fcinfo, 0, "table name")?;
    let columnname = text_arg(fcinfo, 1, "column name")?;
    let ctx = MemoryContext::new("pg_get_serial_sequence");
    let res = crate::pg_get_serial_sequence_worker(ctx.mcx(), &tablename, &columnname)?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_serial_sequence", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_partition_constraintdef(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_partition_constraintdef");
    let res = crate::pg_get_partition_constraintdef_worker(ctx.mcx(), fcinfo.arg_oid(0))?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_partition_constraintdef", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_function_sqlbody(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_get_function_sqlbody");
    let res = crate::pg_get_function_sqlbody_worker(ctx.mcx(), fcinfo.arg_oid(0))?;
    Ok(match res {
        Some(s) => text_result(flinfo, "pg_get_function_sqlbody", &s),
        None => fcinfo.return_null(),
    })
}

pub fn fc_pg_get_statisticsobjdef_expressions(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    const TEXTOID: Oid = 25;
    let ctx = MemoryContext::new("pg_get_statisticsobjdef_expressions");
    let mcx = ctx.mcx();
    let Some(exprs) = crate::pg_get_statisticsobjdef_expressions_worker(mcx, fcinfo.arg_oid(0))?
    else {
        return Ok(fcinfo.return_null());
    };
    let mut texts = Vec::with_capacity(exprs.len());
    for e in &exprs {
        texts.push(varlena::cstring_to_text(mcx, e.as_bytes())?);
    }
    let elems: Vec<Datum> =
        texts.iter().map(|t| Datum::from_usize(t.as_bytes().as_ptr() as usize)).collect();
    let img = arrayfuncs::construct_array(mcx, &elems, TEXTOID, -1, false, b'i')?;
    let buf = out_scratch(flinfo, "pg_get_statisticsobjdef_expressions");
    buf.clear();
    buf.extend_from_slice(&img);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const RULEUTILS_BUILTINS: &[FmgrBuiltin] = &[
    b(1387, "pg_get_constraintdef", 1, fc_pg_get_constraintdef),
    b(1573, "pg_get_ruledef", 1, fc_pg_get_ruledef),
    b(1640, "pg_get_viewdef_name", 1, fc_pg_get_viewdef_name),
    b(1641, "pg_get_viewdef", 1, fc_pg_get_viewdef),
    b(1642, "pg_get_userbyid", 1, fc_pg_get_userbyid),
    b(1643, "pg_get_indexdef", 1, fc_pg_get_indexdef),
    b(1662, "pg_get_triggerdef", 1, fc_pg_get_triggerdef),
    b(1665, "pg_get_serial_sequence", 2, fc_pg_get_serial_sequence),
    b(1716, "pg_get_expr", 2, fc_pg_get_expr),
    b(2098, "pg_get_functiondef", 1, fc_pg_get_functiondef),
    b(2162, "pg_get_function_arguments", 1, fc_pg_get_function_arguments),
    b(2165, "pg_get_function_result", 1, fc_pg_get_function_result),
    b(2232, "pg_get_function_identity_arguments", 1, fc_pg_get_function_identity_arguments),
    b(2504, "pg_get_ruledef_ext", 2, fc_pg_get_ruledef_ext),
    b(2505, "pg_get_viewdef_name_ext", 2, fc_pg_get_viewdef_name_ext),
    b(2506, "pg_get_viewdef_ext", 2, fc_pg_get_viewdef_ext),
    b(2507, "pg_get_indexdef_ext", 3, fc_pg_get_indexdef_ext),
    b(2508, "pg_get_constraintdef_ext", 2, fc_pg_get_constraintdef_ext),
    b(2509, "pg_get_expr_ext", 3, fc_pg_get_expr_ext),
    b(2730, "pg_get_triggerdef_ext", 2, fc_pg_get_triggerdef_ext),
    b(3159, "pg_get_viewdef_wrap", 2, fc_pg_get_viewdef_wrap),
    b(3352, "pg_get_partkeydef", 1, fc_pg_get_partkeydef),
    b(3408, "pg_get_partition_constraintdef", 1, fc_pg_get_partition_constraintdef),
    b(3415, "pg_get_statisticsobjdef", 1, fc_pg_get_statisticsobjdef),
    b(3808, "pg_get_function_arg_default", 2, fc_pg_get_function_arg_default),
    b(6173, "pg_get_statisticsobjdef_expressions", 1, fc_pg_get_statisticsobjdef_expressions),
    b(6174, "pg_get_statisticsobjdef_columns", 1, fc_pg_get_statisticsobjdef_columns),
    b(6197, "pg_get_function_sqlbody", 1, fc_pg_get_function_sqlbody),
];
