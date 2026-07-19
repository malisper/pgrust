use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::TEXTOID;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

// Per-call wide-char conversion scratch (C pallocs and pfrees in
// CurrentMemoryContext); reset on entry, so nothing escapes a call.
std::thread_local! {
    static EXEC_SCRATCH: core::cell::RefCell<Option<&'static mut ::mcx::MemoryContext>> =
        const { core::cell::RefCell::new(None) };
}

fn with_exec_scratch<R>(f: impl FnOnce(::mcx::Mcx<'_>) -> R) -> R {
    EXEC_SCRATCH.with(|cell| {
        let mut slot = cell.borrow_mut();
        let ctx = slot.get_or_insert_with(|| {
            ::mcx::session_root_mut(::mcx::MemoryContext::new_bump("RegexpExecScratch"))
        });
        ctx.reset();
        f(ctx.mcx())
    })
}

// C: s = NameStr(*str); slen = strlen(s).
#[inline]
fn name_str(name: &[u8]) -> &[u8] {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    &name[..end]
}

macro_rules! fc_textre {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null text varlenas (strict fn).
            let (s, p) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            with_exec_scratch(|mcx| {
                Ok(Datum::from_bool(crate::$core(
                    mcx,
                    s.data(),
                    p.data(),
                    fcinfo.get_collation(),
                )?))
            })
        }
    )*};
}

fc_textre! {
    fc_textregexeq: textregexeq;
    fc_textregexne: textregexne;
    fc_texticregexeq: texticregexeq;
    fc_texticregexne: texticregexne;
}

macro_rules! fc_namere {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are a non-null name block and text varlena (strict fn).
            let (n, p) = unsafe { (fcinfo.arg_name(0), fcinfo.arg_varlena_packed(1)?) };
            with_exec_scratch(|mcx| {
                Ok(Datum::from_bool(crate::$core(
                    mcx,
                    name_str(n),
                    p.data(),
                    fcinfo.get_collation(),
                )?))
            })
        }
    )*};
}

fc_namere! {
    fc_nameregexeq: nameregexeq;
    fc_nameregexne: nameregexne;
    fc_nameicregexeq: nameicregexeq;
    fc_nameicregexne: nameicregexne;
}

fn text_datum(mcx: Mcx<'_>, payload: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, payload)?))
}

fn image_datum(img: PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    d
}

fn text_array_datum(mcx: Mcx<'_>, datums: &[Datum], nulls: &[bool]) -> PgResult<Datum> {
    let img = arrayfuncs::construct_md_array(
        mcx,
        datums,
        Some(nulls),
        1,
        &[datums.len() as i32],
        &[1],
        TEXTOID,
        -1,
        false,
        b'i',
    )?;
    Ok(image_datum(img))
}

macro_rules! arg_text {
    ($fcinfo:ident, $i:expr) => {
        // SAFETY: catalog arg is a non-null text varlena.
        unsafe { $fcinfo.arg_varlena_packed($i)? }
    };
}

pub fn fc_textregexsubstr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = {
        let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
        let mcx = fcinfo.result_mcx();
        match crate::textregexsubstr(mcx, s.data(), p.data(), fcinfo.get_collation())? {
            Some(v) => Some(text_datum(mcx, &v)?),
            None => None,
        }
    };
    match out {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_textregexreplace_noopt(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (s, p, r) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1), arg_text!(fcinfo, 2));
    let mcx = fcinfo.result_mcx();
    let out =
        crate::textregexreplace_noopt(mcx, s.data(), p.data(), r.data(), fcinfo.get_collation())?;
    text_datum(mcx, &out)
}

pub fn fc_textregexreplace(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (s, p, r, opt) =
        (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1), arg_text!(fcinfo, 2), arg_text!(fcinfo, 3));
    let mcx = fcinfo.result_mcx();
    let out = crate::textregexreplace(
        mcx,
        s.data(),
        p.data(),
        r.data(),
        opt.data(),
        fcinfo.get_collation(),
    )?;
    text_datum(mcx, &out)
}

fn fc_replace_extended(fcinfo: &mut Fcinfo, with_n: bool, with_flags: bool) -> PgResult<Datum> {
    let (s, p, r) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1), arg_text!(fcinfo, 2));
    let start = fcinfo.arg_i32(3);
    let n = if with_n { Some(fcinfo.arg_i32(4)) } else { None };
    let flags = if with_flags { Some(arg_text!(fcinfo, 5)) } else { None };
    let mcx = fcinfo.result_mcx();
    let out = crate::textregexreplace_extended(
        mcx,
        s.data(),
        p.data(),
        r.data(),
        Some(start),
        n,
        flags.as_ref().map(|f| f.data()),
        fcinfo.get_collation(),
    )?;
    text_datum(mcx, &out)
}

pub fn fc_textregexreplace_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_replace_extended(fcinfo, true, true)
}

pub fn fc_textregexreplace_extended_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_replace_extended(fcinfo, true, false)
}

pub fn fc_textregexreplace_extended_no_n(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_replace_extended(fcinfo, false, false)
}

pub fn fc_similar_to_escape_1(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let pat = arg_text!(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    let out = crate::similar_to_escape_1(mcx, pat.data())?;
    text_datum(mcx, &out)
}

pub fn fc_similar_to_escape_2(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (pat, esc) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
    let mcx = fcinfo.result_mcx();
    let out = crate::similar_to_escape_2(mcx, pat.data(), esc.data())?;
    text_datum(mcx, &out)
}

pub fn fc_similar_escape(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let pat = arg_text!(fcinfo, 0);
    let esc = if fcinfo.argisnull(1) { None } else { Some(arg_text!(fcinfo, 1)) };
    let mcx = fcinfo.result_mcx();
    let out =
        crate::similar_escape_internal(mcx, pat.data(), esc.as_ref().map(|e| e.data()))?;
    text_datum(mcx, &out)
}

fn fc_count(fcinfo: &mut Fcinfo, with_start: bool, with_flags: bool) -> PgResult<Datum> {
    let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
    let start = if with_start { Some(fcinfo.arg_i32(2)) } else { None };
    let flags = if with_flags { Some(arg_text!(fcinfo, 3)) } else { None };
    with_exec_scratch(|mcx| {
        Ok(Datum::from_i32(crate::matches::regexp_count(
            mcx,
            s.data(),
            p.data(),
            start,
            flags.as_ref().map(|f| f.data()),
            fcinfo.get_collation(),
        )?))
    })
}

pub fn fc_regexp_count(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_count(fcinfo, true, true)
}

pub fn fc_regexp_count_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_count(fcinfo, true, false)
}

pub fn fc_regexp_count_no_start(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_count(fcinfo, false, false)
}

fn fc_instr(fcinfo: &mut Fcinfo, nopt: usize) -> PgResult<Datum> {
    let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
    let start = if nopt >= 1 { Some(fcinfo.arg_i32(2)) } else { None };
    let n = if nopt >= 2 { Some(fcinfo.arg_i32(3)) } else { None };
    let endoption = if nopt >= 3 { Some(fcinfo.arg_i32(4)) } else { None };
    let flags = if nopt >= 4 { Some(arg_text!(fcinfo, 5)) } else { None };
    let subexpr = if nopt >= 5 { Some(fcinfo.arg_i32(6)) } else { None };
    with_exec_scratch(|mcx| {
        Ok(Datum::from_i32(crate::matches::regexp_instr(
            mcx,
            s.data(),
            p.data(),
            start,
            n,
            endoption,
            flags.as_ref().map(|f| f.data()),
            subexpr,
            fcinfo.get_collation(),
        )?))
    })
}

pub fn fc_regexp_instr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_instr(fcinfo, 5)
}

pub fn fc_regexp_instr_no_subexpr(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_instr(fcinfo, 4)
}

pub fn fc_regexp_instr_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_instr(fcinfo, 3)
}

pub fn fc_regexp_instr_no_endoption(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_instr(fcinfo, 2)
}

pub fn fc_regexp_instr_no_n(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_instr(fcinfo, 1)
}

pub fn fc_regexp_instr_no_start(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_instr(fcinfo, 0)
}

fn fc_like(fcinfo: &mut Fcinfo, with_flags: bool) -> PgResult<Datum> {
    let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
    let flags = if with_flags { Some(arg_text!(fcinfo, 2)) } else { None };
    with_exec_scratch(|mcx| {
        Ok(Datum::from_bool(crate::matches::regexp_like(
            mcx,
            s.data(),
            p.data(),
            flags.as_ref().map(|f| f.data()),
            fcinfo.get_collation(),
        )?))
    })
}

pub fn fc_regexp_like(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_like(fcinfo, true)
}

pub fn fc_regexp_like_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_like(fcinfo, false)
}

fn fc_substr(fcinfo: &mut Fcinfo, nopt: usize) -> PgResult<Datum> {
    let out = {
        let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
        let start = if nopt >= 1 { Some(fcinfo.arg_i32(2)) } else { None };
        let n = if nopt >= 2 { Some(fcinfo.arg_i32(3)) } else { None };
        let flags = if nopt >= 3 { Some(arg_text!(fcinfo, 4)) } else { None };
        let subexpr = if nopt >= 4 { Some(fcinfo.arg_i32(5)) } else { None };
        let mcx = fcinfo.result_mcx();
        match crate::matches::regexp_substr(
            mcx,
            s.data(),
            p.data(),
            start,
            n,
            flags.as_ref().map(|f| f.data()),
            subexpr,
            fcinfo.get_collation(),
        )? {
            Some(v) => Some(text_datum(mcx, &v)?),
            None => None,
        }
    };
    match out {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_regexp_substr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_substr(fcinfo, 4)
}

pub fn fc_regexp_substr_no_subexpr(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_substr(fcinfo, 3)
}

pub fn fc_regexp_substr_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_substr(fcinfo, 2)
}

pub fn fc_regexp_substr_no_n(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_substr(fcinfo, 1)
}

pub fn fc_regexp_substr_no_start(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_substr(fcinfo, 0)
}

fn fc_match(fcinfo: &mut Fcinfo, with_flags: bool) -> PgResult<Datum> {
    let out = {
        let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
        let flags = if with_flags { Some(arg_text!(fcinfo, 2)) } else { None };
        let mcx = fcinfo.result_mcx();
        match crate::matches::regexp_match(
            mcx,
            s.data(),
            p.data(),
            flags.as_ref().map(|f| f.data()),
            fcinfo.get_collation(),
        )? {
            Some(ctx) => {
                let n = ctx.npatterns as usize;
                let mut datums: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
                let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
                crate::matches::build_regexp_match_result(&ctx, |e| {
                    match e {
                        Some(payload) => {
                            datums.push(text_datum(mcx, &payload)?);
                            nulls.push(false);
                        }
                        None => {
                            datums.push(Datum::from_usize(0));
                            nulls.push(true);
                        }
                    }
                    Ok(())
                })?;
                Some(text_array_datum(mcx, &datums, &nulls)?)
            }
            None => None,
        }
    };
    match out {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_regexp_match(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_match(fcinfo, true)
}

pub fn fc_regexp_match_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_match(fcinfo, false)
}

fn fc_split_to_array(fcinfo: &mut Fcinfo, with_flags: bool) -> PgResult<Datum> {
    let (s, p) = (arg_text!(fcinfo, 0), arg_text!(fcinfo, 1));
    let flags = if with_flags { Some(arg_text!(fcinfo, 2)) } else { None };
    let mcx = fcinfo.result_mcx();
    let mut ctx = crate::matches::regexp_split_setup(
        mcx,
        s.data(),
        p.data(),
        flags.as_ref().map(|f| f.data()),
        fcinfo.get_collation(),
        "regexp_split_to_array()",
    )?;
    let n = (ctx.nmatches + 1) as usize;
    let mut datums: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    while ctx.next_match <= ctx.nmatches {
        let payload = crate::matches::build_regexp_split_result(&ctx)?;
        datums.push(text_datum(mcx, &payload)?);
        ctx.next_match += 1;
    }
    let img = arrayfuncs::construct_array(mcx, &datums, TEXTOID, -1, false, b'i')?;
    Ok(image_datum(img))
}

pub fn fc_regexp_split_to_array(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_split_to_array(fcinfo, true)
}

pub fn fc_regexp_split_to_array_no_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fc_split_to_array(fcinfo, false)
}

// Cross-call SRF rows are owned (std) allocations: per-call memory resets
// between SRF calls, and the fn_extra carrier is heap-boxed.
enum SrfRows {
    Matches(Vec<Vec<Option<Vec<u8>>>>),
    Texts(Vec<Vec<u8>>),
}

fn srf_drive(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
    collect: impl FnOnce(&Fcinfo) -> PgResult<SrfRows>,
) -> PgResult<Datum> {
    let flinfo = flinfo.unwrap_or_else(|| panic!("{name}: NULL flinfo"));
    if !flinfo.has_fn_extra() {
        let rows = collect(fcinfo)?;
        let fctx = funcapi_srf::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi_srf::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("SRF rows set at first call")
        .downcast_ref::<SrfRows>()
        .expect("user_fctx is SrfRows");
    let mcx = fcinfo.result_mcx();
    let out: Option<Datum> = match rows {
        SrfRows::Matches(v) => match v.get(idx) {
            None => None,
            Some(row) => {
                let mut datums: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, row.len())?;
                let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, row.len())?;
                for e in row {
                    match e {
                        Some(b) => {
                            datums.push(text_datum(mcx, b)?);
                            nulls.push(false);
                        }
                        None => {
                            datums.push(Datum::from_usize(0));
                            nulls.push(true);
                        }
                    }
                }
                Some(text_array_datum(mcx, &datums, &nulls)?)
            }
        },
        SrfRows::Texts(v) => match v.get(idx) {
            None => None,
            Some(payload) => Some(text_datum(mcx, payload)?),
        },
    };
    match out {
        Some(d) => Ok(funcapi_srf::srf_return_next(flinfo, fcinfo, d)),
        None => Ok(funcapi_srf::srf_return_done(flinfo, fcinfo)),
    }
}

fn collect_matches(fcinfo: &Fcinfo, with_flags: bool) -> PgResult<SrfRows> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (s, p) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let flags = if with_flags {
        // SAFETY: as above.
        Some(unsafe { fcinfo.arg_varlena_packed(2)? })
    } else {
        None
    };
    let mcx = fcinfo.result_mcx();
    let mut ctx = crate::matches::regexp_matches_setup(
        mcx,
        s.data(),
        p.data(),
        flags.as_ref().map(|f| f.data()),
        fcinfo.get_collation(),
    )?;
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(ctx.nmatches.max(0) as usize);
    while ctx.next_match < ctx.nmatches {
        let mut row: Vec<Option<Vec<u8>>> = Vec::with_capacity(ctx.npatterns as usize);
        crate::matches::build_regexp_match_result(&ctx, |e| {
            row.push(e.map(|v| v.as_slice().to_vec()));
            Ok(())
        })?;
        rows.push(row);
        ctx.next_match += 1;
    }
    Ok(SrfRows::Matches(rows))
}

fn collect_split(fcinfo: &Fcinfo, with_flags: bool) -> PgResult<SrfRows> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (s, p) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let flags = if with_flags {
        // SAFETY: as above.
        Some(unsafe { fcinfo.arg_varlena_packed(2)? })
    } else {
        None
    };
    let mcx = fcinfo.result_mcx();
    let mut ctx = crate::matches::regexp_split_setup(
        mcx,
        s.data(),
        p.data(),
        flags.as_ref().map(|f| f.data()),
        fcinfo.get_collation(),
        "regexp_split_to_table()",
    )?;
    let mut rows = Vec::with_capacity((ctx.nmatches + 1).max(1) as usize);
    while ctx.next_match <= ctx.nmatches {
        rows.push(crate::matches::build_regexp_split_result(&ctx)?.as_slice().to_vec());
        ctx.next_match += 1;
    }
    Ok(SrfRows::Texts(rows))
}

pub fn fc_regexp_matches(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "regexp_matches", |fcinfo| collect_matches(fcinfo, true))
}

pub fn fc_regexp_matches_no_flags(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "regexp_matches", |fcinfo| collect_matches(fcinfo, false))
}

pub fn fc_regexp_split_to_table(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "regexp_split_to_table", |fcinfo| collect_split(fcinfo, true))
}

pub fn fc_regexp_split_to_table_no_flags(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "regexp_split_to_table", |fcinfo| collect_split(fcinfo, false))
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 2, strict: true, retset: false, func }
}

const fn bn(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

// pg_proc.dat rows; 1656-1659 are the bpchar rows sharing the text prosrc,
// 1623 similar_escape is the lone non-strict row.
pub const REGEXP_BUILTINS: &[FmgrBuiltin] = &[
    b(79, "nameregexeq", fc_nameregexeq),
    b(1238, "texticregexeq", fc_texticregexeq),
    b(1239, "texticregexne", fc_texticregexne),
    b(1240, "nameicregexeq", fc_nameicregexeq),
    b(1241, "nameicregexne", fc_nameicregexne),
    b(1252, "nameregexne", fc_nameregexne),
    b(1254, "textregexeq", fc_textregexeq),
    b(1256, "textregexne", fc_textregexne),
    FmgrBuiltin {
        foid: 1623,
        name: "similar_escape",
        nargs: 2,
        strict: false,
        retset: false,
        func: fc_similar_escape,
    },
    b(1656, "bpcharicregexeq", fc_texticregexeq),
    b(1657, "bpcharicregexne", fc_texticregexne),
    b(1658, "bpcharregexeq", fc_textregexeq),
    b(1659, "bpcharregexne", fc_textregexne),
    bn(1986, "similar_to_escape_2", 2, fc_similar_to_escape_2),
    bn(1987, "similar_to_escape_1", 1, fc_similar_to_escape_1),
    bn(2073, "textregexsubstr", 2, fc_textregexsubstr),
    bn(2284, "textregexreplace_noopt", 3, fc_textregexreplace_noopt),
    bn(2285, "textregexreplace", 4, fc_textregexreplace),
    srf(2763, "regexp_matches_no_flags", 2, fc_regexp_matches_no_flags),
    srf(2764, "regexp_matches", 3, fc_regexp_matches),
    srf(2765, "regexp_split_to_table_no_flags", 2, fc_regexp_split_to_table_no_flags),
    srf(2766, "regexp_split_to_table", 3, fc_regexp_split_to_table),
    bn(2767, "regexp_split_to_array_no_flags", 2, fc_regexp_split_to_array_no_flags),
    bn(2768, "regexp_split_to_array", 3, fc_regexp_split_to_array),
    bn(3396, "regexp_match_no_flags", 2, fc_regexp_match_no_flags),
    bn(3397, "regexp_match", 3, fc_regexp_match),
    bn(6251, "textregexreplace_extended", 6, fc_textregexreplace_extended),
    bn(6252, "textregexreplace_extended_no_flags", 5, fc_textregexreplace_extended_no_flags),
    bn(6253, "textregexreplace_extended_no_n", 4, fc_textregexreplace_extended_no_n),
    bn(6254, "regexp_count_no_start", 2, fc_regexp_count_no_start),
    bn(6255, "regexp_count_no_flags", 3, fc_regexp_count_no_flags),
    bn(6256, "regexp_count", 4, fc_regexp_count),
    bn(6257, "regexp_instr_no_start", 2, fc_regexp_instr_no_start),
    bn(6258, "regexp_instr_no_n", 3, fc_regexp_instr_no_n),
    bn(6259, "regexp_instr_no_endoption", 4, fc_regexp_instr_no_endoption),
    bn(6260, "regexp_instr_no_flags", 5, fc_regexp_instr_no_flags),
    bn(6261, "regexp_instr_no_subexpr", 6, fc_regexp_instr_no_subexpr),
    bn(6262, "regexp_instr", 7, fc_regexp_instr),
    bn(6263, "regexp_like_no_flags", 2, fc_regexp_like_no_flags),
    bn(6264, "regexp_like", 3, fc_regexp_like),
    bn(6265, "regexp_substr_no_start", 2, fc_regexp_substr_no_start),
    bn(6266, "regexp_substr_no_n", 3, fc_regexp_substr_no_n),
    bn(6267, "regexp_substr_no_flags", 4, fc_regexp_substr_no_flags),
    bn(6268, "regexp_substr_no_subexpr", 5, fc_regexp_substr_no_subexpr),
    bn(6269, "regexp_substr", 6, fc_regexp_substr),
];
