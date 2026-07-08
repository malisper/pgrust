//! fmgr wrappers (`fc_*`) + `NAME_BUILTINS` for fmgr-core. Still value-core-only:
//! nameconcatoid.

use datum::Datum;
use types_core::catalog::NAMEOID;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use types_tuple::NameData;

use crate::NAMELEN;

fn arg_name(fcinfo: &Fcinfo, i: usize) -> NameData {
    // SAFETY: catalog args of these strict fns are non-null name blocks.
    NameData {
        data: *unsafe { fcinfo.arg_name(i) },
    }
}

macro_rules! fc_namecmp {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_name(fcinfo, 0), arg_name(fcinfo, 1));
            Ok(Datum::$conv(crate::$core(&a, &b, fcinfo.get_collation())?))
        }
    )*};
}

macro_rules! fc_nametext {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_name(fcinfo, 0);
            // SAFETY: catalog arg of these strict fns is a non-null text varlena.
            let b = unsafe { fcinfo.arg_varlena_packed(1)? };
            Ok(Datum::$conv(crate::$core(&a, b.data(), fcinfo.get_collation())?))
        }
    )*};
}

fc_nametext! {
    fc_nameeqtext: nameeqtext -> from_bool;
    fc_namelttext: namelttext -> from_bool;
    fc_nameletext: nameletext -> from_bool;
    fc_namegetext: namegetext -> from_bool;
    fc_namegttext: namegttext -> from_bool;
    fc_namenetext: namenetext -> from_bool;
    fc_btnametextcmp: btnametextcmp -> from_i32;
}

macro_rules! fc_textname {
    ($($fname:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg of these strict fns is a non-null text varlena.
            let a = unsafe { fcinfo.arg_varlena_packed(0)? };
            let b = arg_name(fcinfo, 1);
            Ok(Datum::$conv(crate::$core(a.data(), &b, fcinfo.get_collation())?))
        }
    )*};
}

fc_textname! {
    fc_texteqname: texteqname -> from_bool;
    fc_textltname: textltname -> from_bool;
    fc_textlename: textlename -> from_bool;
    fc_textgename: textgename -> from_bool;
    fc_textgtname: textgtname -> from_bool;
    fc_textnename: textnename -> from_bool;
    fc_bttextnamecmp: bttextnamecmp -> from_i32;
}

fc_namecmp! {
    fc_nameeq: nameeq -> from_bool;
    fc_namene: namene -> from_bool;
    fc_namelt: namelt -> from_bool;
    fc_namele: namele -> from_bool;
    fc_namegt: namegt -> from_bool;
    fc_namege: namege -> from_bool;
    fc_btnamecmp: btnamecmp -> from_i32;
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the int.c out-function precedent). The Datum aliases it until the next
// out call on this thread.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; NAMELEN + 1]> =
        const { core::cell::UnsafeCell::new([0; NAMELEN + 1]) };
}

// C pallocs a NAMEDATALEN block per call; the resolved FmgrInfo owns one
// retained block instead (the varlena textin precedent). The Datum aliases it
// until the next call through the same FmgrInfo.
struct InScratch(NameData);

pub fn fc_namein(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 of strict namein is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let Some(flinfo) = flinfo else {
        panic!("namein: name result needs a resolved FmgrInfo's scratch");
    };
    let name = crate::namein(s);
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(InScratch(name));
    } else {
        flinfo.fn_extra_mut::<InScratch>().unwrap().0 = name;
    }
    let nd = &flinfo.fn_extra_mut::<InScratch>().unwrap().0;
    Ok(Datum::from_usize(nd.data.as_ptr() as usize))
}

pub fn fc_nameout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = arg_name(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let name = s.name_str();
        buf[..name.len()].copy_from_slice(name);
        buf[name.len()] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

// C text_name (name.c): namein over the text body (mbcliplen truncation).
pub fn fc_text_name(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn: arg 0 is a non-null live text varlena.
    let v = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let nd = crate::namein(v.data());
    byref_result(fcinfo.result_mcx(), &nd.data)
}

// C name_text (name.c): cstring_to_text(NameStr(*s)) — bytes up to NUL.
pub fn fc_name_text(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_name(fcinfo, 0);
    let t = varlena::cstring_to_text(fcinfo.result_mcx(), a.name_str())?;
    Ok(types_fmgr::varlena_result(t))
}

pub fn fc_nameconcatoid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let nam = arg_name(fcinfo, 0);
    let oid = fcinfo.arg(1).as_oid();
    let n = crate::nameconcatoid(&nam, oid);
    byref_result(fcinfo.result_mcx(), &n.data)
}

pub fn fc_current_user(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let n = crate::current_user(fcinfo.result_mcx())?;
    byref_result(fcinfo.result_mcx(), &n.data)
}

pub fn fc_session_user(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let n = crate::session_user(fcinfo.result_mcx())?;
    byref_result(fcinfo.result_mcx(), &n.data)
}

pub fn fc_current_schema(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match crate::current_schema(fcinfo.result_mcx())? {
        Some(n) => byref_result(fcinfo.result_mcx(), &n.data),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_current_schemas(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let include_implicit = fcinfo.arg_bool(0);
    let mcx = fcinfo.result_mcx();
    let names = crate::current_schemas(mcx, include_implicit)?;
    let mut datums: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, names.len())?;
    for n in names.iter() {
        datums.push(Datum::from_usize(core::ptr::from_ref(n) as usize));
    }
    let image = arrayfuncs::construct_array(
        mcx,
        &datums,
        NAMEOID,
        NAMELEN as i32,
        false,
        arrayfuncs::foundation::TYPALIGN_CHAR,
    )?;
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    Ok(d)
}

pub fn fc_hashname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_name(fcinfo, 0);
    Ok(Datum::from_u32(::hashfn::hash_bytes(a.name_str())))
}

pub fn fc_hashnameextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_name(fcinfo, 0);
    let seed = fcinfo.arg_i64(1) as u64;
    Ok(Datum::from_u64(::hashfn::hash_bytes_extended(a.name_str(), seed)))
}

pub fn fc_namerecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    let nd = crate::namerecv(mcx, buf)?;
    byref_result(mcx, &nd.data)
}

pub fn fc_namesend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = arg_name(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::namesend(mcx, &s)?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const NAME_BUILTINS: &[FmgrBuiltin] = &[
    b(34, "namein", 1, fc_namein),
    b(35, "nameout", 1, fc_nameout),
    b(62, "nameeq", 2, fc_nameeq),
    b(240, "nameeqtext", 2, fc_nameeqtext),
    b(241, "namelttext", 2, fc_namelttext),
    b(242, "nameletext", 2, fc_nameletext),
    b(243, "namegetext", 2, fc_namegetext),
    b(244, "namegttext", 2, fc_namegttext),
    b(245, "namenetext", 2, fc_namenetext),
    b(246, "btnametextcmp", 2, fc_btnametextcmp),
    b(247, "texteqname", 2, fc_texteqname),
    b(248, "textltname", 2, fc_textltname),
    b(249, "textlename", 2, fc_textlename),
    b(250, "textgename", 2, fc_textgename),
    b(251, "textgtname", 2, fc_textgtname),
    b(252, "textnename", 2, fc_textnename),
    b(253, "bttextnamecmp", 2, fc_bttextnamecmp),
    b(266, "nameconcatoid", 2, fc_nameconcatoid),
    b(359, "btnamecmp", 2, fc_btnamecmp),
    b(406, "name_text", 1, fc_name_text),
    b(407, "text_name", 1, fc_text_name),
    b(447, "hashnameextended", 2, fc_hashnameextended),
    b(455, "hashname", 1, fc_hashname),
    b(655, "namelt", 2, fc_namelt),
    b(656, "namele", 2, fc_namele),
    b(657, "namegt", 2, fc_namegt),
    b(658, "namege", 2, fc_namege),
    b(659, "namene", 2, fc_namene),
    b(710, "current_user", 0, fc_current_user),
    b(745, "current_user", 0, fc_current_user),
    b(746, "session_user", 0, fc_session_user),
    // varchar(text) rides the same varlena bytes as text_name; prorettype
    // differs only at the catalog.
    b(1400, "text_name", 1, fc_text_name),
    // varchar(name) rides the same varlena bytes as name_text (text and
    // varchar share representation); prorettype differs only at the catalog.
    b(1401, "name_text", 1, fc_name_text),
    b(1402, "current_schema", 0, fc_current_schema),
    b(1403, "current_schemas", 1, fc_current_schemas),
    b(2422, "namerecv", 1, fc_namerecv),
    b(2423, "namesend", 1, fc_namesend),
];
