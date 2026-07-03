use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

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
            Box::leak(Box::new(::mcx::MemoryContext::new_bump("RegexpExecScratch")))
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

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 2, strict: true, retset: false, func }
}

// pg_proc.dat rows (all proisstrict, none retset); 1656-1659 are the bpchar
// rows sharing the text prosrc.
pub const REGEXP_BUILTINS: &[FmgrBuiltin] = &[
    b(79, "nameregexeq", fc_nameregexeq),
    b(1238, "texticregexeq", fc_texticregexeq),
    b(1239, "texticregexne", fc_texticregexne),
    b(1240, "nameicregexeq", fc_nameicregexeq),
    b(1241, "nameicregexne", fc_nameicregexne),
    b(1252, "nameregexne", fc_nameregexne),
    b(1254, "textregexeq", fc_textregexeq),
    b(1256, "textregexne", fc_textregexne),
    b(1656, "bpcharicregexeq", fc_texticregexeq),
    b(1657, "bpcharicregexne", fc_texticregexne),
    b(1658, "bpcharregexeq", fc_textregexeq),
    b(1659, "bpcharregexne", fc_textregexne),
];
