//! Type I/O for pg_ndistinct/pg_dependencies (mvdistinct.c/dependencies.c).

use core::fmt::Write;
use datum::Datum;
use mcx::MemoryContext;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: result needs a resolved FmgrInfo's scratch")
}

// C pallocs each result per call; the resolved FmgrInfo owns retained scratch
// (ruleutils builtins precedent). The Datum aliases it until the next call
// through the same FmgrInfo.
struct OutBuf(Vec<u8>);

fn cstring_result(flinfo: Option<&mut FmgrInfo>, name: &'static str, s: &str) -> Datum {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    buf.reserve(s.len() + 1);
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
    Datum::from_usize(buf.as_ptr() as usize)
}

fn cannot_accept(typname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("cannot accept a value of type {typname}"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn fc_pg_ndistinct_in(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept("pg_ndistinct"))
}

pub fn fc_pg_ndistinct_recv(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_ndistinct"))
}

pub fn fc_pg_ndistinct_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_ndistinct_out");
    // SAFETY: arg 0 is a live bytea datum.
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let nd = crate::mvdistinct::statext_ndistinct_deserialize(ctx.mcx(), v.data())?;
    let mut s = String::from("{");
    for (i, item) in nd.items.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        for (j, a) in item.attributes.iter().enumerate() {
            s.push_str(if j == 0 { "\"" } else { ", " });
            let _ = write!(s, "{a}");
        }
        let _ = write!(s, "\": {}", item.ndistinct as i32);
    }
    s.push('}');
    Ok(cstring_result(flinfo, "pg_ndistinct_out", &s))
}

pub fn fc_pg_dependencies_in(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_dependencies"))
}

pub fn fc_pg_dependencies_recv(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_dependencies"))
}

pub fn fc_pg_dependencies_out(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_dependencies_out");
    // SAFETY: arg 0 is a live bytea datum.
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let deps = crate::dependencies::statext_dependencies_deserialize(ctx.mcx(), v.data())?;
    let mut s = String::from("{");
    for (i, dep) in deps.deps.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push('"');
        let n = dep.attributes.len();
        for (j, a) in dep.attributes.iter().enumerate() {
            if j == n - 1 {
                s.push_str(" => ");
            } else if j > 0 {
                s.push_str(", ");
            }
            let _ = write!(s, "{a}");
        }
        let _ = write!(s, "\": {:.6}", dep.degree);
    }
    s.push('}');
    Ok(cstring_result(flinfo, "pg_dependencies_out", &s))
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const STATISTICS_BUILTINS: &[FmgrBuiltin] = &[
    b(3355, "pg_ndistinct_in", 1, fc_pg_ndistinct_in),
    b(3356, "pg_ndistinct_out", 1, fc_pg_ndistinct_out),
    b(3357, "pg_ndistinct_recv", 1, fc_pg_ndistinct_recv),
    b(3404, "pg_dependencies_in", 1, fc_pg_dependencies_in),
    b(3405, "pg_dependencies_out", 1, fc_pg_dependencies_out),
    b(3406, "pg_dependencies_recv", 1, fc_pg_dependencies_recv),
];
