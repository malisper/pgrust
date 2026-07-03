//! fmgr wrappers (`fc_*`) + `NAME_BUILTINS` for fmgr-core. Registrable rows
//! are the by-val comparisons and the scratch-backed in/out functions;
//! recv / send / current_* / nameconcatoid stay value-core-only.

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
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
    b(359, "btnamecmp", 2, fc_btnamecmp),
    b(655, "namelt", 2, fc_namelt),
    b(656, "namele", 2, fc_namele),
    b(657, "namegt", 2, fc_namegt),
    b(658, "namege", 2, fc_namege),
    b(659, "namene", 2, fc_namene),
];
